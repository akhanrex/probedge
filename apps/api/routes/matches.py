from fastapi import APIRouter, Query
from probedge.storage import masters as mstore
router = APIRouter(prefix="/api", tags=["matches"])
@router.get("/api/matches")
def get_matches(
    symbol: str = Query(...),
    ot: Optional[str] = Query(None),
    ol: Optional[str] = Query(None),
    pdc: Optional[str] = Query(None),
):
    df = mstore.read(symbol)
    if df.empty: return {"symbol": symbol, "dates": [], "rows": 0}
    # case-insensitive column map
    cols = {c.lower(): c for c in df.columns}
    if ot and "openingtrend" in cols: df = df[df[cols["openingtrend"]].astype(str).str.upper()==ot.upper()]
    if ol and "openlocation" in cols: df = df[df[cols["openlocation"]].astype(str).str.upper()==ol.upper()]
    if pdc and "prevdaycontext" in cols: df = df[df[cols["prevdaycontext"]].astype(str).str.upper()==pdc.upper()]
    # assume first column is date
    dates = df.iloc[:1000][df.columns[0]].astype(str).tolist() if not df.empty else []
    return {"symbol": symbol, "dates": dates, "rows": len(df)}
