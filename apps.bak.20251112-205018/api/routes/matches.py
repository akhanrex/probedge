from fastapi import APIRouter, HTTPException, Query
from probedge.infra.settings import SETTINGS
from pathlib import Path
import pandas as pd

router = APIRouter()

@router.get("/api/matches")
def api_matches(
    symbol: str,
    ot: str | None = Query(None, alias="ot"),
    ol: str | None = Query(None, alias="ol"),
    pdc: str | None = Query(None, alias="pdc"),
):
    sym = symbol.upper().strip()
    path = Path(SETTINGS.paths.masters.format(sym=sym))
    if not path.exists():
        return {"symbol": sym, "rows": 0, "dates": [], "cols": [], "msg": f"master not found: {path}"}

    df = pd.read_csv(path)
    # normalize likely column names
    colmap = {}
    for c in df.columns:
        k = str(c).strip().lower()
        if "openingtrend" in k:   colmap[c] = "OpeningTrend"
        elif "openlocation" in k: colmap[c] = "OpenLocation"
        elif "prevdaycontext" in k or k == "pdc": colmap[c] = "PrevDayContext"
        elif k in ("date", "datetime", "day"): colmap[c] = "Date"
    df = df.rename(columns=colmap)

    # required columns fallback
    for need in ["OpeningTrend","OpenLocation","PrevDayContext"]:
        if need not in df.columns:
            df[need] = None
    if "Date" not in df.columns:
        raise HTTPException(400, "Master file has no Date/DateTime column")

    m = df
    if ot:  m = m[m["OpeningTrend"].astype(str).str.upper() == ot.upper()]
    if ol:  m = m[m["OpenLocation"].astype(str).str.upper() == ol.upper()]
    if pdc: m = m[m["PrevDayContext"].astype(str).str.upper() == pdc.upper()]

    # dates as YYYY-MM-DD strings
    dates = (
        pd.to_datetime(m["Date"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .dropna()
        .unique()
        .tolist()
    )
    dates.sort()
    return {
        "symbol": sym,
        "rows": int(len(m)),
        "dates": dates,
        "cols": list(m.columns),
    }
