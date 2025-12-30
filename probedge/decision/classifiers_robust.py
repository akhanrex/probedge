import numpy as np
import pandas as pd
from datetime import time as _time

from probedge.core import classifiers as CORE
TH_NARROW      = 1.00
TH_BODY_STRONG = 0.45
TH_BODY_WEAK   = 0.25
TH_CLV_BULL    = 0.65
TH_CLV_BEAR    = 0.35

TH_MOVE = 0.35
TH_RANGE = 0.80
TH_TINY_MOVE = 0.30
TH_POS_TOP = 0.60
TH_POS_BOTTOM = 0.40
TH_DIR = 2
TH_OVERLAP = 0.50

def _to_dt_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def slice_window(df_intraday: pd.DataFrame, start_hm, end_hm) -> pd.DataFrame:
    if df_intraday is None or df_intraday.empty:
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close"])
    d = df_intraday.copy()
    d["DateTime"] = _to_dt_series(d["DateTime"])
    t = d["DateTime"].dt.time
    return d[(t >= start_hm) & (t <= end_hm)].copy()

def prev_trading_day_ohlc(df_intraday: pd.DataFrame, day_norm: pd.Timestamp):
    if df_intraday is None or df_intraday.empty:
        return None
    dti = _to_dt_series(df_intraday["DateTime"]).dt.normalize()
    alld = sorted(dti.unique())
    p = None
    day_norm = pd.to_datetime(day_norm).normalize()
    for i in range(1, 8):
        cand = day_norm - pd.Timedelta(days=i)
        if cand in alld:
            p = cand; break
    if p is None:
        return None
    d = df_intraday[dti.eq(p)].copy()
    if d.empty:
        return None
    return {
        "open":  float(d.loc[d["DateTime"].idxmin(), "Open"]),
        "high":  float(d["High"].max()),
        "low":   float(d["Low"].min()),
        "close": float(d.loc[d["DateTime"].idxmax(), "Close"]),
    }

def compute_prevdaycontext_robust(prev_O, prev_H, prev_L, prev_C) -> str:
    H, L, O, C = float(prev_H), float(prev_L), float(prev_O), float(prev_C)
    rng = max(1e-9, H - L)
    range_pct = 100.0 * rng / max(1e-9, C)
    body_frac = abs(C - O) / rng if rng > 0 else 0.0
    clv = (C - L) / rng if rng > 0 else 0.5
    if (range_pct <= TH_NARROW) or (body_frac <= TH_BODY_WEAK):
        return "TR"
    if (clv >= TH_CLV_BULL) and (body_frac >= TH_BODY_STRONG):
        return "BULL"
    if (clv <= TH_CLV_BEAR) and (body_frac >= TH_BODY_STRONG):
        return "BEAR"
    return "TR"

def _overlap_score(df):
    if df is None or len(df) < 2: return 0.0
    hi = df['High'].to_numpy(); lo = df['Low'].to_numpy()
    ov = []
    for i in range(1, len(df)):
        num = max(0.0, min(hi[i], hi[i-1]) - max(lo[i], lo[i-1]))
        den = max(1e-9, (max(hi[i], hi[i-1]) - min(lo[i], lo[i-1])))
        ov.append(num/den)
    return float(np.mean(ov)) if ov else 0.0

def _dir_count(df):
    up = (df['Close'] > df['Open']).sum()
    dn = (df['Close'] < df['Open']).sum()
    return int(up - dn)

def compute_openingtrend_robust(df_day_intraday: pd.DataFrame) -> str:
    win = slice_window(df_day_intraday, _time(9,15), _time(9,40))
    if win.empty: return "TR"
    win = win.sort_values("DateTime")
    O0 = float(win['Open'].iloc[0]); Cn = float(win['Close'].iloc[-1])
    Hmax = float(win['High'].max()); Lmin = float(win['Low'].min())
    move_pct  = 100.0 * (Cn - O0) / max(1e-9, O0)
    range_pct = 100.0 * (Hmax - Lmin) / max(1e-9, O0)
    pos = 0.5 if Hmax<=Lmin else (Cn - Lmin) / (Hmax - Lmin)
    dcount = _dir_count(win); ovl = _overlap_score(win)
    if (range_pct < TH_RANGE) and (abs(move_pct) < TH_TINY_MOVE) and (ovl > TH_OVERLAP):
        return "TR"
    v_dist =  1 if move_pct >= +TH_MOVE else (-1 if move_pct <= -TH_MOVE else 0)
    v_pos  =  1 if pos      >= TH_POS_TOP else (-1 if pos <= TH_POS_BOTTOM else 0)
    v_pers =  1 if dcount   >= TH_DIR     else (-1 if dcount <= -TH_DIR     else 0)
    S = v_dist + v_pos + v_pers
    return "BULL" if S >= +2 else ("BEAR" if S <= -2 else "TR")

def compute_openlocation(day_open: float, prev_ohlc) -> str:
    if prev_ohlc is None or day_open is None or pd.isna(day_open):
        return ""
    H = float(prev_ohlc.get("high", np.nan)); L = float(prev_ohlc.get("low",  np.nan))
    if pd.isna(H) or pd.isna(L) or H <= L: return ""
    rng = H - L; o = float(day_open)
    if o < L:            return "OBR"
    if o <= L + 0.3*rng: return "OOL"
    if o > H:            return "OAR"
    if o >= H - 0.3*rng: return "OOH"
    return "OIM"

def compute_openlocation_from_df(df_day_intraday: pd.DataFrame, prev_ohlc=None) -> str:
    if df_day_intraday is None or df_day_intraday.empty or prev_ohlc is None:
        return ""
    d = df_day_intraday.sort_values("DateTime")
    try:
        day_open = float(d["Open"].iloc[0])
    except Exception:
        return ""
    return compute_openlocation(day_open, prev_ohlc)

# ---- Canonical delegation (single source of truth; avoid drift) ----
slice_window = CORE.slice_window
prev_trading_day_ohlc = CORE.prev_trading_day_ohlc
compute_prevdaycontext_robust = CORE.compute_prevdaycontext_robust
compute_openingtrend_robust = CORE.compute_openingtrend_robust
compute_openlocation = CORE.compute_openlocation
compute_openlocation_from_df = CORE.compute_openlocation_from_df
