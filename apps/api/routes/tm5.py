from fastapi import APIRouter, Query
import math
import numpy as np
from probedge.storage import tm5 as tm5_store

router = APIRouter()

def _py_sanitize(v):
    # Null checks first
    if v is None:
        return None
    # numpy -> Python
    if isinstance(v, (np.floating, np.integer, np.bool_)):
        v = v.item()
    # handle python floats NaN/Inf
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    # strings/ints/bools are fine
    return v

def _json_records(df):
    # Collapse numpy inf to NaN first, then map per-value
    df = df.replace([np.inf, -np.inf], np.nan)
    out = []
    for rec in df.to_dict(orient="records"):
        out.append({k: _py_sanitize(v) for k, v in rec.items()})
    return out

@router.get("/api/tm5")
def get_tm5(
    symbol: str = Query(..., description="symbol (TMPV etc.)"),
    limit: int = Query(None, ge=1, le=250000, description="optional row cap (from top)")
):
    df = tm5_store.read(symbol)
    if df is None or len(df) == 0:
        print(f"/api/tm5 {symbol}: empty")
        return {"symbol": symbol, "rows": 0, "data": []}
    if limit:
        df = df.head(limit)
    data = _json_records(df)
    print(f"/api/tm5 {symbol}: {len(data)} rows (limit={limit})")
    return {"symbol": symbol, "rows": len(data), "data": data}
