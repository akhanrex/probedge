# decision/tags_engine.py
from __future__ import annotations
from typing import Dict, Optional
import pandas as pd

from probedge.core.classifiers import (
    prev_trading_day_ohlc,
    compute_prevdaycontext_robust,
    compute_openingtrend_robust,
    compute_openlocation_from_df,
    compute_first_candletype,
    compute_rangestatus,
)

def compute_tags_5(df_intraday: pd.DataFrame) -> Dict[str, str]:
    """
    Computes the 5 master tags for the MOST RECENT trading day present in df_intraday.
    Expects columns: DateTime, Open, High, Low, Close (IST-naive).
    Uses prev-day OHLC derived from df_intraday itself (so PDC/OL/etc can be computed).
    Returns keys aligned with master CSV columns.
    """
    out = {"PDC_R":"", "OL":"", "OT_R":"TR", "FIRST_CANDLE":"", "RANGE_STATUS":""}
    if df_intraday is None or df_intraday.empty:
        return out

    d = df_intraday.copy()
    d["DateTime"] = pd.to_datetime(d["DateTime"], errors="coerce")
    d = d.dropna(subset=["DateTime"]).sort_values("DateTime")
    if d.empty:
        return out

    day_norm = d["DateTime"].dt.normalize().iloc[-1]
    day_df   = d[d["DateTime"].dt.normalize().eq(day_norm)].copy()
    if day_df.empty:
        return out

    # prev-day OHLC from the same df
    prev = prev_trading_day_ohlc(d, day_norm)

    # --- PDC (robust)
    if prev:
        out["PDC_R"] = compute_prevdaycontext_robust(prev["open"], prev["high"], prev["low"], prev["close"])

    # --- OL (use wrapper that pulls today's open from df + prev range bands)
    ol = compute_openlocation_from_df(day_df, prev)
    out["OL"] = ol or ""

    # --- OT (robust opening trend 09:15–09:40)
    out["OT_R"] = compute_openingtrend_robust(day_df)

    # --- First candle type (uses prev range too)
    out["FIRST_CANDLE"] = compute_first_candletype(day_df, prev) or ""

    # --- Range status (depends on OL + prev range)
    out["RANGE_STATUS"] = compute_rangestatus(day_df, ol, prev) or ""

    return out
