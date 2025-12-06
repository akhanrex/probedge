from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
import pandas as pd
from probedge.storage.resolver import locate_for_read

router = APIRouter()


@router.get("/api/tm5")
def get_tm5(symbol: str = Query(..., alias="symbol")):
    """
    Manual terminal contract:

    - Input:  /api/tm5?symbol=ETERNAL
    - Output: text/csv with columns: DateTime,Open,High,Low,Close
    """
    path = locate_for_read("intraday", symbol)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"TM5 not found for {symbol} at {path}")

    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"tm5 read error for {symbol} at {path}: {e}")

    # Normalize column names
    norm = {str(c).strip().lower(): c for c in df.columns}
    def has(k: str) -> bool:
        return k in norm
    def src(k: str) -> str:
        return norm[k]

    ren = {}

    # Price columns
    if has("open"):  ren[src("open")]  = "Open"
    if has("high"):  ren[src("high")]  = "High"
    if has("low"):   ren[src("low")]   = "Low"
    if has("close"): ren[src("close")] = "Close"

    # DateTime: either existing datetime-like column or synth from date+time
    dt_src = None
    for cand in ("datetime", "date_time", "timestamp"):
        if has(cand):
            dt_src = src(cand)
            break
    if dt_src:
        ren[dt_src] = "DateTime"

    df = df.rename(columns=ren)

    # Synthesize DateTime if still missing
    if "DateTime" not in df.columns:
        lower = {str(c).strip().lower(): c for c in df.columns}
        date_col = lower.get("date")
        time_col = lower.get("time") or lower.get("timestr")
        if date_col and time_col:
            df["DateTime"] = pd.to_datetime(
                df[date_col].astype(str) + " " + df[time_col].astype(str),
                errors="coerce",
            )
        elif date_col:
            df["DateTime"] = pd.to_datetime(df[date_col], errors="coerce")
        else:
            raise HTTPException(
                status_code=422,
                detail=f"tm5 missing Date/DateTime columns in {path}",
            )

    # Sanity: required columns
    need = {"DateTime", "Open", "High", "Low", "Close"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"tm5 missing columns {missing} in {path}",
        )

    # Clean + sort
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    df = df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"]).sort_values("DateTime")

    # Browser-friendly ISO without timezone
    df["DateTime"] = df["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    csv = df[["DateTime", "Open", "High", "Low", "Close"]].to_csv(index=False)
    return Response(content=csv, media_type="text/csv")
