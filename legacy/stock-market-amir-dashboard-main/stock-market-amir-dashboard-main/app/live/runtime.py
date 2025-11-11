from __future__ import annotations
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict
import pandas as pd
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

try:
    import app.intraday_utils as iu
except Exception:
    iu = None

@dataclass
class LiveState:
    symbol: str
    last_bar_ts: Optional[pd.Timestamp] = None
    last_ohlc: Optional[Dict[str, float]] = None
    prev_day_hi: Optional[float] = None
    prev_day_lo: Optional[float] = None
    tags: Dict[str, str] = None
    signal: Dict[str, float] = None
    meta: Dict = None

def _load_5m(inst_key: str) -> pd.DataFrame:
    if iu is None or not hasattr(iu, "load_intraday_all"):
        return pd.DataFrame()
    return iu.load_intraday_all(inst_key, force_reload=True)

def _latest_bar(df: pd.DataFrame):
    if df is None or df.empty: return None, None
    df = df.sort_values("datetime")
    row = df.iloc[-1]
    ts = pd.to_datetime(row["datetime"]).tz_convert(IST) if getattr(row["datetime"], "tzinfo", None) else pd.to_datetime(row["datetime"]).tz_localize(IST)
    ohlc = dict(open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]))
    return ts, ohlc

def _prev_day_hilo(df: pd.DataFrame):
    if df is None or df.empty: return None, None
    df["date_ts"] = pd.to_datetime(df["date"]).dt.normalize()
    last_day = df["date_ts"].max()
    prev_day = df["date_ts"][df["date_ts"] < last_day].max() if last_day is not None else None
    if prev_day is None: return None, None
    d = df[df["date_ts"].eq(prev_day)]
    return float(d["high"].max()), float(d["low"].min())

def compute_minimal_tags_for_day(df_day: pd.DataFrame, df_all: pd.DataFrame) -> Dict[str, str]:
    """Fast tag trio from 5m: PrevDayContext, OpenLocation, OpeningTrend"""
    try:
        from probedge.core.classifiers import (
            compute_prevdaycontext_robust, compute_openlocation_from_df,
            compute_openingtrend_robust, prev_trading_day_ohlc
        )
        df_day2 = df_day.rename(columns={
            "datetime":"DateTime","open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"
        }).copy()
        df_day2["DateTime"] = pd.to_datetime(df_day2["DateTime"]).dt.tz_convert(IST).dt.tz_localize(None)

        prev = prev_trading_day_ohlc(df_all.rename(columns={
            "datetime":"DateTime","open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"
        }), pd.to_datetime(df_day2["DateTime"].iloc[0]).normalize())
        prev_ctx = "TR"
        if prev and all(k in prev for k in ("open","high","low","close")):
            prev_ctx = compute_prevdaycontext_robust(prev["open"], prev["high"], prev["low"], prev["close"])
        open_loc = compute_openlocation_from_df(df_day2, prev)
        opening_trend = compute_openingtrend_robust(df_day2)
        return {"PrevDayContext": prev_ctx, "OpenLocation": open_loc, "OpeningTrend": opening_trend}
    except Exception:
        return {}

def poll_once(inst_key: str, symbol_label: str) -> LiveState:
    g = _load_5m(inst_key)
    if g.empty:
        return LiveState(symbol=symbol_label, tags={}, signal={}, meta={"error":"no_5m"})
    ts, ohlc = _latest_bar(g)
    g["date_ts"] = pd.to_datetime(g["date"]).dt.normalize()
    day = g[g["date_ts"].eq(g["date_ts"].max())].copy()
    tags = compute_minimal_tags_for_day(day, g) if not day.empty else {}
    hi, lo = _prev_day_hilo(g)
    return LiveState(
        symbol=symbol_label, last_bar_ts=ts, last_ohlc=ohlc,
        prev_day_hi=hi, prev_day_lo=lo, tags=tags, signal={}, meta={}
    )
