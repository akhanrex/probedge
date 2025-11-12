from fastapi import APIRouter, HTTPException, Query
import pandas as pd
from probedge.storage.resolver import locate_for_read

router = APIRouter()

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
    m = pd.read_csv(path)
    def norm(x): return str(x).strip().upper()
    m["OpeningTrend"] = m["OpeningTrend"].astype(str).str.upper().str.strip()
    if ol:
        m = m[m["OpenLocation"].astype(str).str.upper().str.strip() == norm(ol)]
    if pdc:
        m = m[m["PrevDayContext"].astype(str).str.upper().str.strip() == norm(pdc)]
    m = m[m["OpeningTrend"] == norm(ot)]
    # Result filter: only BULL/BEAR for frequency stats
    lab = m["Result"].astype(str).str.upper().str.strip()
    m = m[lab.isin(["BULL", "BEAR"])]
    dates = list(pd.to_datetime(m["Date"], errors="coerce").dropna().astype(str).unique())
    return {
        "symbol": symbol.upper(),
        "ot": norm(ot),
        "ol": norm(ol) if ol else "",
        "pdc": norm(pdc) if pdc else "",
        "dates": dates,
        "rows": m.to_dict(orient="records"),
    }
