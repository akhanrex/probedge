from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import numpy as np

from apps.storage.tm5 import read_tm5

router = APIRouter(prefix="/api", tags=["tm5"])

def _py_sanitize(v):
    if v is None:
        return None
    # Convert numpy types to Python scalars for JSON
    if isinstance(v, (np.floating, np.integer, np.bool_)):
        return v.item()
    return v

@router.get("/tm5")
def get_tm5(symbol: str = Query(..., min_length=1), limit: Optional[int] = Query(300, ge=1)):
    sym = symbol.strip().upper()
    try:
        df = read_tm5(sym)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    rows = df.tail(limit).to_dict(orient="records")
    rows = [{k: _py_sanitize(v) for k, v in r.items()} for r in rows]
    return {"symbol": sym, "rows": rows, "count": len(rows)}
