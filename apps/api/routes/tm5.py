from fastapi import APIRouter, Query
from probedge.storage import tm5 as tm5store
router = APIRouter(prefix="/api", tags=["tm5"])
@router.get("/tm5")
def get_tm5(symbol: str = Query(..., alias="symbol")):
    df = tm5store.read(symbol)
    if df.empty: return {"symbol": symbol, "rows": 0, "data": []}
    data = df.tail(500).to_dict(orient="records")
    return {"symbol": symbol, "rows": len(data), "data": data}
