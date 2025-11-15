from fastapi import APIRouter, HTTPException, Query
from datetime import date
from typing import Optional, List, Dict, Any

from probedge.decision.plan_core import build_parity_plan
from probedge.infra.settings import SETTINGS

router = APIRouter()


@router.get("/api/plan")
def api_plan(
    symbol: str,
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses latest day available for that symbol",
    ),
):
    """
    Single-symbol parity plan (raw Colab-style logic).
    """
    plan = build_parity_plan(symbol, day)
    return plan


@router.get("/api/plan/all")
def api_plan_all(
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses latest common day across symbols",
    ),
):
    """
    All symbols' single-symbol parity plans in one shot.
    Does NOT do portfolio risk-splitting; just mirrors /api/plan over SETTINGS.symbols.
    """
    symbols = SETTINGS.symbols
    plans: List[Dict[str, Any]] = []
    for sym in symbols:
        plan = build_parity_plan(sym, day)
        plans.append(plan)

    # Infer a day if not provided
    out_day = day
    if not out_day:
        for p in plans:
            val = p.get("date")
            if val:
                out_day = str(val).split("T")[0]
                break

    return {
        "date": out_day,
        "symbols": symbols,
        "plans": plans,
    }
