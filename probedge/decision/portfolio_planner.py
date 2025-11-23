# probedge/decision/portfolio_planner.py

from typing import Dict, Any, List, Optional
import math
from datetime import date, datetime

from probedge.infra.settings import SETTINGS
from probedge.decision.plan_core import build_parity_plan
from probedge.infra.logger import get_logger

log = get_logger(__name__)


def _today_str() -> str:
    return datetime.now().date().isoformat()


def _effective_daily_risk_rs() -> int:
    """
    Decide which daily risk number to use.

    - MODE = 'test' → use risk_rs_test (default 1,000)
    - Else → use risk_budget_rs (default 10,000)
    """
    mode = (SETTINGS.mode or "").lower()
    if mode == "test":
        return int(getattr(SETTINGS, "risk_rs_test", 1000))

    if hasattr(SETTINGS, "risk_budget_rs") and SETTINGS.risk_budget_rs:
        return int(SETTINGS.risk_budget_rs)

    return int(getattr(SETTINGS, "risk_rs_default", 10000))


def _is_active_plan(p: Dict[str, Any]) -> bool:
    """
    Decide if a plan is tradable for portfolio purposes.
    """
    if not isinstance(p, dict):
        return False

    pick = p.get("pick")
    if pick not in ("BULL", "BEAR"):
        return False

    entry = p.get("entry")
    stop = p.get("stop")

    if entry is None or stop is None:
        return False

    try:
        e = float(entry)
        s = float(stop)
    except (TypeError, ValueError):
        return False

    if not (math.isfinite(e) and math.isfinite(s)):
        return False

    if abs(e - s) <= 0:
        return False

    return True


def build_raw_plans_for_day(day: Optional[str]) -> List[Dict[str, Any]]:
    """
    Build single-symbol parity plans for all portfolio symbols for a given day.

    day may be:
    - 'YYYY-MM-DD' → explicit day
    - None         → builder uses latest available day per symbol
    """
    symbols = SETTINGS.symbols
    raw: List[Dict[str, Any]] = []

    for sym in symbols:
        try:
            plan = build_parity_plan(sym, day)
        except Exception as exc:
            # If tm5 / master missing, mark as ABSTAIN and continue
            log.warning("build_parity_plan failed for %s: %s", sym, exc)
            raw.append(
                {
                    "symbol": sym,
                    "pick": "ABSTAIN",
                    "reason": f"PLAN_ERROR: {exc}",
                }
            )
            continue

        raw.append(plan)

    return raw


def apply_portfolio_split(
    raw_plans: List[Dict[str, Any]],
    daily_risk_rs: int,
) -> Dict[str, Any]:
    """
    Take raw single-symbol plans and apply equal risk-splitting across active trades.

    - Only BULL/BEAR with valid entry/stop are considered active.
    - Risk per active trade = floor(daily_risk_rs / active_count).
    - Qty = floor(risk_per_trade / |entry - stop|).
    """
    # Decide which date to use for the state payload
    day: Optional[str] = None
    for p in raw_plans:
        val = p.get("date")
        if val:
            day = str(val).split("T")[0]
            break

    active_indices = [i for i, p in enumerate(raw_plans) if _is_active_plan(p)]
    active_count = len(active_indices)

    if active_count == 0 or daily_risk_rs <= 0:
        return {
            "date": day,
            "mode": SETTINGS.mode,
            "daily_risk_rs": daily_risk_rs,
            "active_trades": 0,
            "risk_per_trade_rs": 0,
            "plans": raw_plans,
            "total_planned_risk_rs": 0.0,
        }

    risk_per_trade_rs = int(daily_risk_rs // active_count)

    adjusted: List[Dict[str, Any]] = []
    total_planned = 0.0

    for idx, p in enumerate(raw_plans):
        q = dict(p)  # shallow copy

        if idx not in active_indices:
            q["qty"] = int(q.get("qty") or 0)
            q["per_trade_risk_rs_used"] = 0
            q["parity_mode"] = False
            adjusted.append(q)
            continue

        entry = float(p.get("entry"))
        stop = float(p.get("stop"))
        risk_per_share = abs(entry - stop)

        if not math.isfinite(risk_per_share) or risk_per_share <= 0:
            q["qty"] = 0
            q["per_trade_risk_rs_used"] = 0
            q["parity_mode"] = False
            adjusted.append(q)
            continue

        qty = int(risk_per_trade_rs // risk_per_share)

        if qty <= 0:
            q["qty"] = 0
            q["per_trade_risk_rs_used"] = 0
            q["parity_mode"] = False
            adjusted.append(q)
            continue

        per_trade_risk = qty * risk_per_share

        q["risk_per_share"] = risk_per_share
        q["qty"] = qty
        q["per_trade_risk_rs_used"] = per_trade_risk
        q["parity_mode"] = True

        adjusted.append(q)
        total_planned += per_trade_risk

    return {
        "date": day,
        "mode": SETTINGS.mode,
        "daily_risk_rs": daily_risk_rs,
        "active_trades": active_count,
        "risk_per_trade_rs": risk_per_trade_rs,
        "plans": adjusted,
        "total_planned_risk_rs": total_planned,
    }


def build_portfolio_state_for_day(
    day: Optional[date],
    explicit_risk_rs: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Master function used by BOTH:
      - GET /api/state
      - runtime auto-trader

    day: Python date or None
    explicit_risk_rs: if given, overrides default daily risk
    """
    if day is None:
        day_str = _today_str()
    else:
        day_str = day.isoformat()

    raw_plans = build_raw_plans_for_day(day_str)

    if explicit_risk_rs is not None:
        daily_risk = int(explicit_risk_rs)
    else:
        daily_risk = _effective_daily_risk_rs()

    portfolio_state = apply_portfolio_split(raw_plans, daily_risk)

    # Force portfolio date to the requested day for clarity
    portfolio_state["date"] = day_str
    return portfolio_state
