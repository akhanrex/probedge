import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from typing import Dict, Any, List, Optional
import math
from datetime import datetime
from datetime import date
from math import floor
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from probedge.infra.settings import SETTINGS
from probedge.decision.plan_core import build_parity_plan
from probedge.infra.logger import get_logger
from probedge.storage.atomic_json import AtomicJSON

log = get_logger(__name__)
router = APIRouter()

# live_state.json path + helper
STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)


# -------------------------------
# 0) Small helpers
# -------------------------------

def _today_str() -> str:
    """Return today's date as YYYY-MM-DD (system local time; on your Mac this is IST)."""
    return datetime.now().date().isoformat()


def _write_portfolio_plan_to_state(portfolio_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Persist a portfolio-level plan into live_state.json under key 'portfolio_plan'.
    Returns the same object back to the caller.
    """
    state = aj.read(default={})
    state["portfolio_plan"] = portfolio_state
    aj.write(state)
    return portfolio_state


# -------------------------------
# 1) Daily risk & parity helpers
# -------------------------------

def _effective_daily_risk_rs() -> int:
    """
    Decide which daily risk number to use.

    - MODE = 'test' → use RISK_RS_TEST (usually 1,000).
    - Otherwise → use risk_budget_rs (derived from RISK_RS_DEFAULT, usually 10,000).
    """
    mode = (SETTINGS.mode or "").lower()
    if mode == "test":
        return int(getattr(SETTINGS, "risk_rs_test", 1000))

    if hasattr(SETTINGS, "risk_budget_rs") and SETTINGS.risk_budget_rs:
        return int(SETTINGS.risk_budget_rs)

    # Fallback
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


def _build_raw_plans_for_day(day_str: Optional[str]) -> List[Dict[str, Any]]:
    """
    Build single-symbol parity plans for all portfolio symbols for a given day.

    day_str may be:
    - 'YYYY-MM-DD' → explicit day
    - None         → builder uses latest available day per symbol
    """
    symbols = SETTINGS.symbols
    raw: List[Dict[str, Any]] = []

    for sym in symbols:
        try:
            plan = build_parity_plan(sym, day_str)
        except HTTPException as exc:
            # If tm5 / master missing, mark as ABSTAIN and continue
            log.warning("build_parity_plan failed for %s: %s", sym, exc)
            raw.append(
                {
                    "symbol": sym,
                    "pick": "ABSTAIN",
                    "reason": f"PLAN_ERROR: {exc.detail}",
                }
            )
            continue

        raw.append(plan)

    return raw


def _apply_portfolio_split(
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
        # No trades: pass-through
        return {
            "date": day,
            "mode": SETTINGS.mode,
            "daily_risk_rs": daily_risk_rs,
            "active_trades": 0,
            "risk_per_trade_rs": 0,
            "plans": raw_plans,
        }

    risk_per_trade_rs = int(daily_risk_rs // active_count)

    adjusted: List[Dict[str, Any]] = []
    for idx, p in enumerate(raw_plans):
        q = dict(p)  # shallow copy

        if idx not in active_indices:
            # Non-active: make sure qty and per-trade risk are zeroed for clarity
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

        q["risk_per_share"] = risk_per_share
        q["qty"] = qty
        q["per_trade_risk_rs_used"] = risk_per_trade_rs
        q["parity_mode"] = True

        adjusted.append(q)

    return {
        "date": day,
        "mode": SETTINGS.mode,
        "daily_risk_rs": daily_risk_rs,
        "active_trades": active_count,
        "risk_per_trade_rs": risk_per_trade_rs,
        "plans": adjusted,
    }


# -------------------------------
# 2) Existing parity endpoint
# -------------------------------


@router.get("/api/state")
def api_state(
    day: Optional[date] = Query(None),
    risk: Optional[int] = Query(None, description="Override daily risk budget in rupees"),
) -> Dict[str, Any]:
    """
    State for parity plan for a given day.

    If `risk` is provided, we override the daily risk budget and
    recompute qty + per_trade_risk purely from entry/stop.
    """
    settings = SETTINGS
    day = day or date.today()

    # Build raw plans (tags, pick, entry/stop/targets etc) for each symbol
    raw_plans = _build_raw_plans_for_day(day)

    # 1) Choose daily risk: override if query param is given
    daily_risk = int(risk) if risk is not None else int(settings.risk_budget_rs)

    # 2) Identify active picks that can actually take risk
    active_plans = [
        p
        for p in raw_plans
        if p.get("pick") in ("BULL", "BEAR")
        and p.get("entry") is not None
        and p.get("stop") is not None
    ]
    active_trades = len(active_plans)

    # 3) Equal-split risk across active trades
    if active_trades > 0 and daily_risk > 0:
        risk_per_trade = daily_risk // active_trades
    else:
        risk_per_trade = 0

    # 4) Recompute qty + per_trade_risk from entry/stop and risk_per_trade
    total_planned = 0.0
    for p in raw_plans:
        is_active = (
            p.get("pick") in ("BULL", "BEAR")
            and p.get("entry") is not None
            and p.get("stop") is not None
            and risk_per_trade > 0
        )

        if is_active:
            entry = float(p["entry"])
            stop = float(p["stop"])
            risk_per_share = abs(entry - stop)

            if risk_per_share <= 0:
                qty = 0
            else:
                qty = int(floor(risk_per_trade / risk_per_share))

            per_trade_risk = qty * risk_per_share

            p["risk_per_share"] = risk_per_share
            p["qty"] = qty
            p["per_trade_risk_rs_used"] = per_trade_risk
            p["parity_mode"] = True

            total_planned += per_trade_risk
        else:
            # No risk allocation for this symbol
            p["qty"] = 0
            p["per_trade_risk_rs_used"] = 0
            p["parity_mode"] = False

    return {
        "date": day.isoformat(),
        "mode": settings.mode,
        "daily_risk_rs": daily_risk,
        "active_trades": active_trades,
        "risk_per_trade_rs": risk_per_trade,
        "total_planned_risk_rs": total_planned,
        "plans": raw_plans,
    }


# -------------------------------
# 3) Live-state + ARM control
# -------------------------------

class ArmRequest(BaseModel):
    symbol: str
    strategy: str = "batch_v1"


@router.get("/api/state_raw")
def api_state_raw():
    """
    Raw live_state.json dump (whatever batch_agent + other components wrote).
    Used for debugging and UI introspection.
    """
    state = aj.read(default={})
    return state or {}


@router.post("/api/control/arm")
def api_control_arm(req: ArmRequest):
    """
    Ask batch_agent to compute a plan for TODAY for a symbol.

    This just writes control fields into live_state.json:
      - control.action  = 'arm'
      - control.symbol  = SYMBOL (upper-case)
      - control.strategy = strategy (lower-case, default 'batch_v1')

    The batch_agent loop will pick this up and write a 'plan' block back
    into live_state.json.
    """
    state = aj.read(default={})
    ctrl = state.get("control") or {}

    ctrl["action"] = "arm"
    ctrl["symbol"] = req.symbol.strip().upper()
    ctrl["strategy"] = req.strategy.strip().lower()

    state["control"] = ctrl
    aj.write(state)

    return {
        "status": "ok",
        "control": ctrl,
    }


# -------------------------------
# 4) Portfolio ARM for a day
# -------------------------------

@router.post("/api/plan/arm_day")
def api_plan_arm_day(
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses today's date (system local)",
    )
):
    """
    Build the full 10-symbol parity plan for a specific trading day and
    persist it into live_state.json under 'portfolio_plan'.

    - Uses the same logic as GET /api/state.
    - If 'day' is omitted, uses today's date.
    """
    if day is None:
        day = _today_str()

    raw_plans = _build_raw_plans_for_day(day)
    daily_risk_rs = _effective_daily_risk_rs()
    portfolio_state = _apply_portfolio_split(raw_plans, daily_risk_rs)

    # Force portfolio date to the requested day (even if some symbols had older/latest data)
    portfolio_state["date"] = day

    return _write_portfolio_plan_to_state(portfolio_state)
