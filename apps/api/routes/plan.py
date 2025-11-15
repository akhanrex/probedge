from fastapi import APIRouter, Query
from typing import Optional

from probedge.decision.plan_core import build_parity_plan

router = APIRouter()


@router.get("/api/plan")
def api_plan(
    symbol: str = Query(...),
    day: Optional[str] = Query(
        None, description="YYYY-MM-DD; if omitted, uses latest day in tm5 for that symbol"
    ),
):
    """
    Single-symbol Colab-parity plan.
    Now delegated to probedge.decision.plan_core.build_parity_plan.
    """
    plan = build_parity_plan(symbol, day)
    return plan
