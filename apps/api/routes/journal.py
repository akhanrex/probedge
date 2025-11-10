from fastapi import APIRouter
router = APIRouter(prefix="/api/journal", tags=["journal"])
@router.get("/daily")
def daily():
    return {"days": 0, "avg_daily_r": 0.0, "sharpe": 0.0, "sortino": 0.0, "calmar": 0.0, "mdd": 0.0, "series": []}
