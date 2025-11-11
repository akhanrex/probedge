from fastapi import APIRouter, Query
from typing import Optional
from probedge.storage import masters

router = APIRouter()

@router.get("/api/matches")
def get_matches(
    symbol: str = Query(...),
    ot: Optional[str] = Query(default=None),
    ol: Optional[str] = Query(default=None),
    pdc: Optional[str] = Query(default=None)
):
    df = masters.read(symbol)
    if df is None or len(df) == 0:
        return {"dates": [], "rows": []}

    filt = [True] * len(df)
    if "OpeningTrend" in df.columns and ot:
        filt = (df["OpeningTrend"].astype(str).str.upper() == ot.upper())
    if "OpenLocation" in df.columns and ol:
        filt = filt & (df["OpenLocation"].astype(str).str.upper() == ol.upper())
    if "PrevDayContext" in df.columns and pdc:
        filt = filt & (df["PrevDayContext"].astype(str).str.upper() == pdc.upper())

    sub = df[filt]
    dates = sub["date"].astype(str).tolist() if "date" in sub.columns else []
    return {"dates": dates, "rows": sub.to_dict(orient="records")}
