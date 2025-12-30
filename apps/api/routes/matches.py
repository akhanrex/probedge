
from fastapi import APIRouter, Query
import pandas as pd, json

from apps.storage.tm5 import read_master
from ._jsonsafe import json_safe_df
from ._freq_select import apply_lookback, select_hist_batch_parity

router = APIRouter()

def _norm(x):
    return str(x or "").strip().upper()

@router.get("/api/matches")
def get_matches(
    symbol: str = Query(...),
    ot: str = Query(...),
    ol: str = Query(""),
    pdc: str = Query(""),
    asof: str | None = Query(None),
):
    sym = _norm(symbol)
    m = read_master(sym)
    if m is None or m.empty:
        return {"symbol": sym, "ot": _norm(ot), "ol": _norm(ol), "pdc": _norm(pdc), "dates": [], "rows": []}

    m, _day = apply_lookback(m, asof)
    hist_bb, meta = select_hist_batch_parity(m, ot, ol, pdc)

    # Short aliases for UI
    if not hist_bb.empty:
        if "OpeningTrend" in hist_bb.columns:    hist_bb["OT"]  = hist_bb["OpeningTrend"]
        if "OpenLocation" in hist_bb.columns:    hist_bb["OL"]  = hist_bb["OpenLocation"]
        if "PrevDayContext" in hist_bb.columns:  hist_bb["PDC"] = hist_bb["PrevDayContext"]
        if "FirstCandleType" in hist_bb.columns: hist_bb["FCT"] = hist_bb["FirstCandleType"]
        if "RangeStatus" in hist_bb.columns:     hist_bb["RS"]  = hist_bb["RangeStatus"]

    hist_bb = json_safe_df(hist_bb)
    rows = json.loads(hist_bb.to_json(orient="records"))

    for r in rows:
        d = r.get("Date")
        if isinstance(d, str) and len(d) >= 10:
            r["Date"] = d[:10]

    dates = sorted({r.get("Date") for r in rows if r.get("Date")})

    return {
        "symbol": sym,
        "ot": _norm(ot), "ol": _norm(ol), "pdc": _norm(pdc),
        "level": meta.get("level"),
        "total": int(meta.get("total") or 0),
        "total_all": int(meta.get("total_all") or 0),
        "tr_n": int(meta.get("tr_n") or 0),
        "counts": meta.get("counts") or {"BULL": int(meta.get("bull_n") or 0), "BEAR": int(meta.get("bear_n") or 0), "TR": int(meta.get("tr_n") or 0), "TOTAL": int((meta.get("bull_n") or 0) + (meta.get("bear_n") or 0) + (meta.get("tr_n") or 0))},

        "dates": dates,
        "rows": rows,
    }
