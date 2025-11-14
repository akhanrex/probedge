from fastapi import APIRouter, HTTPException, Query
import pandas as pd, json
from probedge.storage.resolver import locate_for_read
from ._jsonsafe import json_safe_df

router = APIRouter()

def _norm(x): return str(x).strip().upper()

@router.get("/api/matches")
def get_matches(
    symbol: str = Query(...),
    ot: str = Query(..., description="OpeningTrend: BULL|BEAR|TR"),
    ol: str = Query("", description="OpenLocation: OAR|OOH|OOL|OIM|OBR (optional)"),
    pdc: str = Query("", description="PrevDayContext: BULL|BEAR|TR (optional)")
):
    path = locate_for_read("masters", symbol)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"MASTER not found for {symbol}")

    try:
        m = pd.read_csv(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read MASTER CSV: {e}")

    m["OpeningTrend"] = m["OpeningTrend"].astype(str).str.upper().str.strip()
    if ol:
        m = m[m["OpenLocation"].astype(str).str.upper().str.strip() == _norm(ol)]
    if pdc:
        m = m[m["PrevDayContext"].astype(str).str.upper().str.strip() == _norm(pdc)]
    m = m[m["OpeningTrend"] == _norm(ot)]
    lab = m["Result"].astype(str).str.upper().str.strip()
    m = m[lab.isin(["BULL", "BEAR"])]

    # Sanitize + force JSON-safe roundtrip
    m = json_safe_df(m)
    try:
        rows = json.loads(m.to_json(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Serialization error: {e}")

    # Dates list (safe strings)
    dates = sorted({r.get("Date") for r in rows if r.get("Date")})

    return {
        "symbol": symbol.upper(),
        "ot": _norm(ot),
        "ol": _norm(ol) if ol else "",
        "pdc": _norm(pdc) if pdc else "",
        "dates": dates,
        "rows": rows,
    }
