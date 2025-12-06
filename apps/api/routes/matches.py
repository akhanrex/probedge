from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
import pandas as pd

from apps.storage.tm5 import read_master
from ._jsonsafe import json_safe_df

router = APIRouter()


def _norm(x: str) -> str:
    return str(x or "").strip().upper()


@router.get("/api/matches")
def get_matches(
    symbol: str = Query(...),
    ot: str = Query(..., description="OpeningTrend: BULL|BEAR|TR"),
    ol: str = Query("", description="OpenLocation: OAR|OOH|OOL|OIM|OBR (optional)"),
    pdc: str = Query("", description="PrevDayContext: BULL|BEAR|TR (optional)"),
):
    sym = _norm(symbol)
    otN, olN, pdcN = _norm(ot), _norm(ol), _norm(pdc)

    try:
        df = read_master(sym)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if df is None or df.empty:
        return {
            "symbol": sym,
            "ot": otN,
            "ol": olN,
            "pdc": pdcN,
            "dates": [],
            "rows": [],
        }

    df = df.copy()

    # Map robust tag columns to short names expected by the manual terminal.
    if "OpeningTrend" in df.columns:
        df["OT"] = df["OpeningTrend"]
    if "OpenLocation" in df.columns:
        df["OL"] = df["OpenLocation"]
    if "PrevDayContext" in df.columns:
        df["PDC"] = df["PrevDayContext"]
    if "FirstCandleType" in df.columns:
        df["FCT"] = df["FirstCandleType"]
    if "RangeStatus" in df.columns:
        df["RS"] = df["RangeStatus"]

    # Normalize Date → "YYYY-MM-DD" strings
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Standardize tags
    for col in ("OT", "OL", "PDC", "FCT", "RS", "Result"):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"NAN": ""})
            )

    # Apply filters
    m = df.copy()
    if otN:
        m = m[m["OT"] == otN]
    if olN:
        m = m[m["OL"] == olN]
    if pdcN:
        m = m[m["PDC"] == pdcN]

    if m.empty:
        return {
            "symbol": sym,
            "ot": otN,
            "ol": olN,
            "pdc": pdcN,
            "dates": [],
            "rows": [],
        }

    # JSON-safe and limit to reasonable columns
    m = json_safe_df(m)

    wanted = ["Date", "PDC", "OL", "OT", "FCT", "RS", "Result"]
    present = [c for c in wanted if c in m.columns]
    rows = m[present].to_dict(orient="records")

    dates = sorted({r.get("Date") for r in rows if r.get("Date")})

    return {
        "symbol": sym,
        "ot": otN,
        "ol": olN,
        "pdc": pdcN,
        "dates": dates,
        "rows": rows,
    }
