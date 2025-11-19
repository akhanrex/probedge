from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import math

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
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses latest available day from tm5/master",
    )
):
    """
    Portfolio-level execution state.

    - Builds raw single-symbol parity plans (Colab-equivalent).
    - Applies equal risk splitting across all active (BULL/BEAR) picks.
    - Uses RISK_RS_DEFAULT or RISK_RS_TEST depending on MODE.
    """
    raw_plans = _build_raw_plans_for_day(day)
    daily_risk_rs = _effective_daily_risk_rs()
    state = _apply_portfolio_split(raw_plans, daily_risk_rs)
    return state


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
