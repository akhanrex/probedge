from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any, Optional
import math

from probedge.infra.settings import SETTINGS
from probedge.decision.plan_core import build_parity_plan

router = APIRouter()


def _effective_daily_risk_rs() -> int:
    if getattr(SETTINGS, "mode", "paper") == "test":
        return 1000
    return int(getattr(SETTINGS, "risk_budget_rs", 10000))


def _is_active_plan(plan: Dict[str, Any]) -> bool:
    if not isinstance(plan, dict):
        return False
    if plan.get("pick") not in ("BULL", "BEAR"):
        return False
    if plan.get("skip"):
        return False
    rps = plan.get("risk_per_share")
    try:
        rps = float(rps)
    except Exception:
        return False
    return rps > 0.0


def _apply_portfolio_split(
    plans: List[Dict[str, Any]],
    daily_risk_rs: int,
) -> Dict[str, Any]:
    """
    Take per-symbol parity plans and apply equal risk split:
      - Find all active plans (BULL/BEAR, no skip, risk_per_share > 0).
      - risk_per_trade = floor(daily_risk / active_count)
      - qty = floor(risk_per_trade / risk_per_share)
      - If qty == 0 -> mark skip='qty=0_after_split' and do NOT redistribute.
    Returns dict with updated plans + meta.
    """
    active_indices = [i for i, p in enumerate(plans) if _is_active_plan(p)]
    active_count = len(active_indices)

    if active_count <= 0 or daily_risk_rs <= 0:
        # Nothing to allocate; just return baseline plans with zeroed portfolio info
        return {
            "daily_risk_rs": daily_risk_rs,
            "risk_per_trade_rs": 0,
            "active_count": 0,
            "plans": plans,
        }

    risk_per_trade = int(math.floor(daily_risk_rs / active_count))

    for idx in active_indices:
        p = plans[idx]
        rps = float(p.get("risk_per_share", 0.0))
        if rps <= 0:
            p["skip"] = "bad_risk_per_share"
            continue

        qty = int(math.floor(risk_per_trade / rps))
        if qty <= 0:
            # do not redistribute; simply mark and move on
            p["skip"] = "qty=0_after_split"
            continue

        p["qty"] = qty
        p["per_trade_risk_rs_used"] = risk_per_trade
        p["portfolio_mode"] = True  # flag that portfolio split has been applied

    return {
        "daily_risk_rs": daily_risk_rs,
        "risk_per_trade_rs": risk_per_trade,
        "active_count": active_count,
        "plans": plans,
    }


@router.get("/api/plan/all")
def api_plan_all(
    day: str = Query(..., description="YYYY-MM-DD (trading day to evaluate)")
):
    """
    Build parity plans for ALL configured symbols for a given day.
    Does NOT change qty yet; this is raw parity output per-symbol.
    """
    symbols = list(getattr(SETTINGS, "symbols", []))
    if not symbols:
        raise HTTPException(status_code=500, detail="No symbols configured in SETTINGS")

    plans: List[Dict[str, Any]] = []
    for sym in symbols:
        plan = build_parity_plan(sym, day)
        plans.append(plan)

    return {
        "date": day,
        "symbols": symbols,
        "plans": plans,
        "parity_mode": True,
    }


@router.get("/api/state")
def api_state(
    day: str = Query(..., description="YYYY-MM-DD (trading day to evaluate)")
):
    """
    Portfolio state for a given day:
      - daily_risk_rs (effective)
      - risk_per_trade_rs after equal split
      - active_count (symbols with live pick)
      - per-symbol plans (with qty adjusted per split)
    """
    symbols = list(getattr(SETTINGS, "symbols", []))
    if not symbols:
        raise HTTPException(status_code=500, detail="No symbols configured in SETTINGS")

    # 1) Build per-symbol parity plans
    raw_plans: List[Dict[str, Any]] = []
    for sym in symbols:
        p = build_parity_plan(sym, day)
        raw_plans.append(p)

    # 2) Apply portfolio split
    daily_risk_rs = _effective_daily_risk_rs()
    res = _apply_portfolio_split(raw_plans, daily_risk_rs)

    return {
        "date": day,
        "mode": getattr(SETTINGS, "mode", "paper"),
        "symbols": symbols,
        "risk_budget_rs": daily_risk_rs,
        "risk_per_trade_rs": res.get("risk_per_trade_rs", 0),
        "active_count": res.get("active_count", 0),
        "plans": res.get("plans", []),
    }
