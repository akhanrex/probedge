import numpy as np
import pandas as pd
from datetime import time as dtime
from typing import Optional, Dict, Tuple

# ---------- time windows (IST) ----------
T0 = dtime(9, 40)           # 09:40
T1 = dtime(15, 5)           # 15:05
S_M  = 9*60 + 15            # 09:15
E_M  = 9*60 + 35            # 09:35
T0_M = 9*60 + 40            # 09:40
T1_M = 15*60 + 5            # 15:05

# post-window label threshold (% vs 09:40 open)
T_POST = 0.60

# ---------- helpers ----------
def _safe_num(x, default=np.nan):
    try:
        v = float(x)
        return v if np.isfinite(v) else default
    except Exception:
        return default

def _ensure_date_col(i5: pd.DataFrame) -> pd.DataFrame:
    if "Date" not in i5.columns:
        i5 = i5.copy()
        i5["Date"] = pd.to_datetime(i5["DateTime"], errors="coerce").dt.normalize()
    return i5

def _ensure_mins_col(df_day: pd.DataFrame) -> pd.DataFrame:
    if "_mins" not in df_day.columns:
        df_day = df_day.copy()
        df_day["_mins"] = df_day["DateTime"].dt.hour * 60 + df_day["DateTime"].dt.minute
    return df_day

def _slice_minutes(df_day: pd.DataFrame, m0: int, m1: int) -> pd.DataFrame:
    if df_day is None or df_day.empty:
        return pd.DataFrame()
    df_day = _ensure_mins_col(df_day)
    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    return df_day.loc[m, ["DateTime","Open","High","Low","Close","Date"]]

# ---------- prev trading day OHLC ----------
def prev_trading_day_ohlc(i5: pd.DataFrame, day: pd.Timestamp) -> Optional[Dict[str, float]]:
    if i5 is None or i5.empty:
        return None
    i5 = _ensure_date_col(i5).sort_values("DateTime")
    days = list(sorted(i5["Date"].dropna().unique()))
    d = pd.to_datetime(day).normalize()
    # find immediate prior session to 'day'
    prior = [x for x in days if x < d]
    if not prior:
        return None
    dprev = pd.to_datetime(prior[-1])
    dfp = i5[i5["Date"] == dprev]
    if dfp.empty:
        return None
    return {
        "high": float(dfp["High"].max()),
        "low":  float(dfp["Low"].min()),
    }

# ---------- tags ----------
def compute_prevdaycontext_robust(prev_ohlc: Optional[Dict[str, float]]) -> str:
    """PDC ∈ {BULL, BEAR, TR} based on previous day's structure; robust fallback to TR."""
    if not prev_ohlc:
        return "TR"
    ph = _safe_num(prev_ohlc.get("high"))
    pl = _safe_num(prev_ohlc.get("low"))
    if not (np.isfinite(ph) and np.isfinite(pl)):
        return "TR"
    span = ph - pl
    if span <= 0:
        return "TR"
    q = span * 0.25
    # very simple directional lean using quartile buffer
    if (ph - q) - (pl + q) > q:
        return "BULL"
    if (pl + q) - (ph - q) > q:
        return "BEAR"
    return "TR"

def compute_openlocation_from_df(i5: pd.DataFrame, day: pd.Timestamp, prev_ohlc: Optional[Dict[str, float]]) -> str:
    """OL ∈ {OOH, OOL, OAR}: open vs previous day's high/low."""
    if i5 is None or i5.empty or (not prev_ohlc):
        return "OAR"
    i5 = _ensure_date_col(i5)
    d = pd.to_datetime(day).normalize()
    df = i5[i5["Date"] == d].sort_values("DateTime")
    if df.empty:
        return "OAR"
    o  = _safe_num(df["Open"].iloc[0])
    ph = _safe_num(prev_ohlc.get("high"))
    pl = _safe_num(prev_ohlc.get("low"))
    if not (np.isfinite(o) and np.isfinite(ph) and np.isfinite(pl)):
        return "OAR"
    if o > ph: return "OOH"
    if o < pl: return "OOL"
    return "OAR"

def compute_openingtrend_robust(i5: pd.DataFrame, day: pd.Timestamp) -> str:
    """OT ∈ {BULL, BEAR, TR} via 09:15–09:35 ORB midpoint read."""
    if i5 is None or i5.empty:
        return "TR"
    i5 = _ensure_date_col(i5)
    d = pd.to_datetime(day).normalize()
    df = i5[i5["Date"] == d]
    if df.empty:
        return "TR"
    win = _slice_minutes(df, S_M, E_M)
    if win.empty:
        return "TR"
    h = float(win["High"].max()); l = float(win["Low"].min())
    if not (np.isfinite(h) and np.isfinite(l)):
        return "TR"
    rng = h - l
    if rng <= 0:
        return "TR"
    mid = (h + l) / 2.0
    c_last = float(win["Close"].iloc[-1])
    if c_last >= mid + 0.1 * rng:
        return "BULL"
    if c_last <= mid - 0.1 * rng:
        return "BEAR"
    return "TR"

# ---------- session result 09:40→15:05 ----------
def compute_result_0940_1505(df_day: pd.DataFrame) -> Tuple[str, float]:
    """Return (label, %ret) using 09:40 open → 15:05 close."""
    if df_day is None or df_day.empty:
        return "TR", 0.0
    win = _slice_minutes(df_day, T0_M, T1_M)
    if win.empty:
        return "TR", 0.0
    o = float(win["Open"].iloc[0]); c = float(win["Close"].iloc[-1])
    if not (np.isfinite(o) and o > 0 and np.isfinite(c)):
        return "TR", 0.0
    ret = 100.0 * (c - o) / o
    if ret >= T_POST:  return "BULL", round(ret, 3)
    if ret <= -T_POST: return "BEAR", round(ret, 3)
    return "TR", round(ret, 3)
