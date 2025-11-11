from fastapi import APIRouter, Query
import numpy as np
from probedge.storage import tm5 as tm5_store

router = APIRouter()

def _py_sanitize(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, np.integer, np.bool_)):
        return v.item()
    try:
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
    except Exception:
        pass
    return v

def _json_records(df):
    df = df.replace([np.inf, -np.inf], np.nan)
    out = []
    for rec in df.to_dict(orient="records"):
        out.append({k: _py_sanitize(v) for k, v in rec.items()})
    return out

@router.get("/api/tm5")
def get_tm5(
    symbol: str = Query(..., description="symbol (TMPV etc.)"),
    limit: int = Query(None, ge=1, le=250_000, description="optional cap on rows from top")
):
    df = tm5_store.read(symbol)
    if df is None or len(df) == 0:
        return {{"symbol": symbol, "rows": 0, "data": []}}
    if limit:
        df = df.head(limit)
    data = _json_records(df)
    return {{"symbol": symbol, "rows": len(data), "data": data}}
