from datetime import date
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query

from probedge.decision.plan_core import build_parity_plan
from probedge.infra.settings import SETTINGS
from probedge.storage.atomic_json import AtomicJSON
from probedge.storage.resolver import state_path

router = APIRouter()

_STATE_PATH = state_path()
_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
_state_aj = AtomicJSON(str(_STATE_PATH))


def _resolve_day_for_plan(day: Optional[str]) -> str:
    """
    Shared resolver for 'day' across /api/plan and /api/plan/all.

    Priority:
      1) Explicit ?day=...
      2) sim_day from live_state.json if sim is active
      3) Today's date.
    """
    if day:
        return day

    try:
        st = _state_aj.read()
    except Exception:
        st = None

    if isinstance(st, dict):
        if st.get("sim") and st.get("sim_day"):
            return str(st["sim_day"])

    return date.today().isoformat()


@router.get("/api/plan")
def api_plan(
    symbol: str,
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses sim_day (if SIM) or latest day available for that symbol",
    ),
):
    """
    Single-symbol parity plan (raw Colab-style logic).
    """
    day_resolved = _resolve_day_for_plan(day)
    plan = build_parity_plan(symbol, day_resolved)
    return plan


@router.get("/api/plan/all")
def api_plan_all(
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses sim_day (if SIM) or latest common day across symbols",
    ),
):
    """
    All symbols' single-symbol parity plans in one shot.
    Does NOT do portfolio risk-splitting; just mirrors /api/plan over SETTINGS.symbols.
    """
    day_resolved = _resolve_day_for_plan(day)
    symbols = SETTINGS.symbols
    plans: List[Dict[str, Any]] = []
    for sym in symbols:
        plan = build_parity_plan(sym, day_resolved)
        plans.append(plan)

    return {
        "date": day_resolved,
        "symbols": symbols,
        "plans": plans,
    }
