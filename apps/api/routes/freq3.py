from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Optional

from apps.storage.tm5 import read_master
from ._freq_select import apply_lookback, select_hist_batch_parity

router = APIRouter()

def _norm(x) -> str:
    return str(x or "").strip().upper()

@router.get("/api/freq3")
def freq3(
    symbol: str = Query(...),
    ot: str = Query(...),
    ol: str = Query(...),
    pdc: str = Query(...),
    asof: Optional[str] = Query(None, description="YYYY-MM-DD (default: today IST)"),
):
    sym = _norm(symbol)

    m = read_master(sym)
    if m is None or m.empty:
        return {
            "symbol": sym,
            "tags": {"ot": _norm(ot), "ol": _norm(ol), "pdc": _norm(pdc)},
            "level": "L3",
            "bull_n": 0, "bear_n": 0, "total": 0, "gap_pp": 0.0,
            "pick": "ABSTAIN", "conf_pct": 0,
            "reason": "no master rows",
        }

    m, _day = apply_lookback(m, asof)
    _hist_bb, meta = select_hist_batch_parity(m, ot, ol, pdc)

    # meta already contains batch-parity fields
    return {
        "symbol": sym,
        "tags": {"ot": _norm(ot), "ol": _norm(ol), "pdc": _norm(pdc)},
        "level": meta.get("level") or "L3",
        "bull_n": int(meta.get("bull_n") or 0),
        "bear_n": int(meta.get("bear_n") or 0),
        "total": int(meta.get("total") or 0),
        "gap_pp": float(round(meta.get("gap_pp") or 0.0, 1)),
        "pick": meta.get("pick") or "ABSTAIN",
        "conf_pct": int(meta.get("conf_pct") or 0),
        "reason": meta.get("reason") or "",
    }
