from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import numpy as np
from apps.storage.tm5 import read_master

router = APIRouter(prefix="/api", tags=["superpath"])

@router.get("/superpath")
def superpath(
    symbol: str = Query(..., min_length=1),
    ot: Optional[str] = Query(None),
    ol: Optional[str] = Query(None),
    pdc: Optional[str] = Query(None),
):
    sym = symbol.strip().upper()
    try:
        df = read_master(sym)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    cols = {c.lower(): c for c in df.columns}
    def col(name: str):
        for c in [name, name.upper(), name.lower()]:
            if c in df.columns: return c
        for k in cols:
            if k.startswith(name.lower()):
                return cols[k]
        return None

    c_ot = col("OpeningTrend")
    c_ol = col("OpenLocation")
    c_pdc = col("PrevDayContext")
    m = df.copy()
    if ot: m = m[m[c_ot].astype(str).str.upper() == ot.upper()]
    if ol: m = m[m[c_ol].astype(str).str.upper() == ol.upper()]
    if pdc: m = m[m[c_pdc].astype(str).str.upper() == pdc.upper()]

    res_col = None
    for c in ["Result","Direction","Side","Pick"]:
        if c in m.columns: res_col = c; break
    b = r = 0
    if res_col:
        b = int((m[res_col].astype(str).str.upper() == "BULL").sum())
        r = int((m[res_col].astype(str).str.upper() == "BEAR").sum())
    n = int(len(m))
    edge_pp = (b - r) / max(1, n) * 100.0

    meta = {
        "filters": {"ot": ot, "ol": ol, "pdc": pdc},
        "counts": {"bull": b, "bear": r, "n": n, "edge_pp": edge_pp},
    }
    return {"symbol": sym, "meta": meta}
