# probedge/core/classifiers.py
"""
Unified robust tag classifiers for ProbEdge.

Exports (single source of truth):
- slice_window(df, start_hm, end_hm) -> DataFrame
- prev_trading_day_ohlc(df_intraday, day_norm) -> dict|None

Core tags:
- compute_prevdaycontext_robust(prev_ohlc_or_O, prev_H=None, prev_L=None, prev_C=None) -> str
- compute_openingtrend_robust(df_or_intraday, day_norm: pd.Timestamp | None = None) -> str
- compute_result_0940_1505(df_day_intraday) -> (str, float)
- compute_openlocation(day_open, prev_ohlc) -> str
- compute_openlocation_from_df(df_or_intraday, day_or_prev, maybe_prev=None) -> str
- compute_first_candletype(df_day_intraday) -> str
- compute_rangestatus(df_day_intraday) -> str

This file is backwards-compatible with:
- old Colab / batch code (four-arg PDC, day-level OT/OL)
- new repo code (dict-based PDC, multi-day intraday + day for OT/OL).
"""

from __future__ import annotations
from datetime import time as _time
from typing import Optional, Dict, Tuple

import numpy as np
import pandas as pd

__all__ = [
    "slice_window",
    "prev_trading_day_ohlc",
    "compute_prevdaycontext_robust",
    "compute_openingtrend_robust",
    "compute_result_0940_1505",
    "compute_openlocation",
    "compute_openlocation_from_df",
    "compute_first_candletype",
    "compute_rangestatus",
]

# ---------- Helpers ----------

def _to_dt_series(s: pd.Series) -> pd.Series:
    """
    Parse to datetime and strip timezone so everything is IST-naive wall time.
    This avoids 'tz-aware vs tz-naive' comparison issues.
    """
    s = pd.to_datetime(s, errors="coerce")
    # If tz-aware, convert to naive
    try:
        if s.dt.tz is not None:
            s = s.dt.tz_localize(None)
    except AttributeError:
        # non-datetime series or already naive
        pass
    return s


def slice_window(df_intraday: pd.DataFrame, start_hm, end_hm) -> pd.DataFrame:
    """Return a copy of df filtered to [start_hm, end_hm] inclusive on DateTime."""
    if df_intraday is None or df_intraday.empty:
        return pd.DataFrame(columns=["DateTime", "Open", "High", "Low", "Close"])
    d = df_intraday.copy()
    d["DateTime"] = _to_dt_series(d["DateTime"])
    t = d["DateTime"].dt.time
    return d[(t >= start_hm) & (t <= end_hm)].copy()


def prev_trading_day_ohlc(df_intraday: pd.DataFrame, day_norm: pd.Timestamp) -> Optional[Dict[str, float]]:
    """
    Find the previous AVAILABLE trading day in df_intraday and return its daily OHLC.
    Avoids weekends/holidays automatically by using the actual dates present.

    Returns dict with keys: open, high, low, close; or None.
    """
    if df_intraday is None or df_intraday.empty:
        return None
    dti = _to_dt_series(df_intraday["DateTime"]).dt.normalize()
    alld = sorted(dti.unique())
    p = None
    day_norm = pd.to_datetime(day_norm).normalize()
    # look back up to a week
    for i in range(1, 8):
        cand = day_norm - pd.Timedelta(days=i)
        if cand in alld:
            p = cand
            break
    if p is None:
        return None
    d = df_intraday[dti.eq(p)].copy()
    if d.empty:
        return None
    # open = first bar's open; close = last bar's close of that day
    return {
        "open":  float(d.loc[d["DateTime"].idxmin(), "Open"]),
        "high":  float(d["High"].max()),
        "low":   float(d["Low"].min()),
        "close": float(d.loc[d["DateTime"].idxmax(), "Close"]),
    }

# ---------- PrevDayContext (robust) ----------

TH_NARROW       = 1.00    # % small day => TR
TH_BODY_STRONG  = 0.45
TH_BODY_WEAK    = 0.25
TH_CLV_BULL     = 0.65
TH_CLV_BEAR     = 0.35

def compute_prevdaycontext_robust(prev_O, prev_H=None, prev_L=None, prev_C=None) -> str:
    """
    Backwards-compatible PDC:

    Two calling styles supported:

      1) New repo:
         prev_ohlc = prev_trading_day_ohlc(...)
         compute_prevdaycontext_robust(prev_ohlc)

      2) Old Colab:
         compute_prevdaycontext_robust(prev_O, prev_H, prev_L, prev_C)

    Returns one of {"BULL","BEAR","TR"}.
    """
    # dict-style call
    if prev_H is None and prev_L is None and prev_C is None and isinstance(prev_O, dict):
        prev_dict = prev_O
        O = prev_dict.get("open", np.nan)
        H = prev_dict.get("high", np.nan)
        L = prev_dict.get("low",  np.nan)
        C = prev_dict.get("close", np.nan)
    else:
        O, H, L, C = prev_O, prev_H, prev_L, prev_C

    try:
        H = float(H); L = float(L); O = float(O); C = float(C)
    except Exception:
        return "TR"

    rng = max(1e-9, H - L)
    range_pct = 100.0 * rng / max(1e-9, C)
    body_frac = abs(C - O) / rng if rng > 0 else 0.0
    clv = (C - L) / rng if rng > 0 else 0.5  # 0=close@low … 1=close@high

    if (range_pct <= TH_NARROW) or (body_frac <= TH_BODY_WEAK):
        return "TR"
    if (clv >= TH_CLV_BULL) and (body_frac >= TH_BODY_STRONG):
        return "BULL"
    if (clv <= TH_CLV_BEAR) and (body_frac >= TH_BODY_STRONG):
        return "BEAR"
    return "TR"

# ---------- OpeningTrend (robust; 09:15–09:40) ----------

TH_MOVE = 0.35       # % move from 09:15 open to 09:40 close
TH_RANGE = 0.80      # % total range considered “tight”
TH_TINY_MOVE = 0.30  # % tiny net move (for chop override)
TH_POS_TOP = 0.60    # close position near top -> bull vote
TH_POS_BOTTOM = 0.40 # close position near bottom -> bear vote
TH_DIR = 2           # min (#up - #down) for persistence vote
TH_OVERLAP = 0.50    # avg consecutive overlap fraction for packed bars

def _overlap_score(df):
    if df is None or len(df) < 2:
        return 0.0
    hi = df["High"].to_numpy()
    lo = df["Low"].to_numpy()
    ov = []
    for i in range(1, len(df)):
        num = max(0.0, min(hi[i], hi[i - 1]) - max(lo[i], lo[i - 1]))
        den = max(1e-9, (max(hi[i], hi[i - 1]) - min(lo[i], lo[i - 1])))
        ov.append(num / den)
    return float(np.mean(ov)) if ov else 0.0

def _dir_count(df):
    up = (df["Close"] > df["Open"]).sum()
    dn = (df["Close"] < df["Open"]).sum()
    return int(up - dn)

def compute_openingtrend_robust(df_or_intraday: pd.DataFrame,
                                day_norm: Optional[pd.Timestamp] = None) -> str:
    """
    Backwards-compatible OT:

      1) Old style: compute_openingtrend_robust(df_day_intraday)
         -> df_or_intraday is already a single-day dataframe.

      2) New repo style: compute_openingtrend_robust(i5_multi_day, day_norm)
         -> we slice out that day first using DateTime.

    Returns one of {"BULL","BEAR","TR"} for 09:15–09:40 window.
    """
    if df_or_intraday is None or df_or_intraday.empty:
        return "TR"

    if day_norm is not None:
        dti = _to_dt_series(df_or_intraday["DateTime"]).dt.normalize()
        day_norm = pd.to_datetime(day_norm).normalize()
        df_day = df_or_intraday[dti.eq(day_norm)].copy()
    else:
        df_day = df_or_intraday

    win = slice_window(df_day, _time(9, 15), _time(9, 40))
    if win.empty:
        return "TR"
    win = win.sort_values("DateTime")

    O0 = float(win["Open"].iloc[0])
    Cn = float(win["Close"].iloc[-1])
    Hmax = float(win["High"].max())
    Lmin = float(win["Low"].min())

    move_pct  = 100.0 * (Cn - O0) / max(1e-9, O0)
    range_pct = 100.0 * (Hmax - Lmin) / max(1e-9, O0)
    pos = 0.5 if Hmax <= Lmin else (Cn - Lmin) / (Hmax - Lmin)
    dcount = _dir_count(win)
    ovl = _overlap_score(win)

    # chop override: tight, tiny move, packed bars => TR
    if (range_pct < TH_RANGE) and (abs(move_pct) < TH_TINY_MOVE) and (ovl > TH_OVERLAP):
        return "TR"

    v_dist =  1 if move_pct >= +TH_MOVE      else (-1 if move_pct <= -TH_MOVE      else 0)
    v_pos  =  1 if pos      >= TH_POS_TOP    else (-1 if pos      <= TH_POS_BOTTOM else 0)
    v_pers =  1 if dcount   >= TH_DIR        else (-1 if dcount   <= -TH_DIR       else 0)
    S = v_dist + v_pos + v_pers
    return "BULL" if S >= +2 else ("BEAR" if S <= -2 else "TR")

# ---------- Result_0940_1505 (robust; 09:40–15:05) ----------

T_POST = 0.60  # % threshold for BULL/BEAR; else TR

def compute_result_0940_1505(df_day_intraday: pd.DataFrame) -> Tuple[str, float]:
    """
    Returns (label, ret_pct) for 09:40→15:05 based on first open & last close.
    label in {"BULL","BEAR","TR"}; ret_pct is float percent rounded(3).
    """
    win = slice_window(df_day_intraday, _time(9, 40), _time(15, 5))
    if win.empty:
        return "TR", 0.0
    win = win.sort_values("DateTime")
    o = float(win["Open"].iloc[0]); c = float(win["Close"].iloc[-1])
    if o == 0:
        return "TR", 0.0
    ret = 100.0 * (c - o) / o
    if ret >=  T_POST:
        return "BULL", round(ret, 3)
    if ret <= -T_POST:
        return "BEAR", round(ret, 3)
    return "TR", round(ret, 3)

# ---------- Legacy-driven helpers we keep centralized ----------

def compute_openlocation(day_open: float, prev_ohlc: Optional[Dict[str, float]]) -> str:
    """
    Return one of: {"OAR","OOH","OIM","OOL","OBR"} based on today's open vs prev-day range.
    Uses the exact thresholds from your old weekly updater (0.30 band).
    """
    if prev_ohlc is None or day_open is None or pd.isna(day_open):
        return ""
    H = float(prev_ohlc.get("high", np.nan))
    L = float(prev_ohlc.get("low",  np.nan))
    if pd.isna(H) or pd.isna(L) or H <= L:
        return ""
    rng = H - L
    o = float(day_open)
    if o < L:            return "OBR"
    if o <= L + 0.3 * rng: return "OOL"
    if o > H:            return "OAR"
    if o >= H - 0.3 * rng: return "OOH"
    return "OIM"

def compute_first_candletype(df_day_intraday: pd.DataFrame,
                             prev_ohlc: Optional[Dict[str, float]] = None) -> str:
    """
    Exact logic from the old weekly updater:
      - PURE DOJI test on the first bar (body <= 50% range, centered, wicks present)
      - HUGE OPEN if first-bar range > 0.7 * prevDayRange
        OR if any of bars 2..5 expand extremes > 0.9 * prevDayRange vs bar1 extremes
      - else NORMAL
    Requires prev_ohlc (uses prev high/low). Without prev_ohlc, returns "".
    """
    if df_day_intraday is None or df_day_intraday.empty or prev_ohlc is None:
        return ""

    d = df_day_intraday.sort_values("DateTime").reset_index(drop=True)
    if len(d) == 0:
        return ""

    # first bar O/H/L/C
    try:
        o = float(d.loc[0, "Open"]);   h = float(d.loc[0, "High"])
        l = float(d.loc[0, "Low"]);    c = float(d.loc[0, "Close"])
    except Exception:
        return ""

    # prev-day range
    try:
        pdh = float(prev_ohlc.get("high", np.nan))
        pdl = float(prev_ohlc.get("low",  np.nan))
    except Exception:
        pdh = pdl = np.nan
    if pd.isna(pdh) or pd.isna(pdl) or (pdh - pdl) <= 0:
        return ""

    rng_prev = pdh - pdl
    rng_bar1 = h - l
    if rng_bar1 <= 0:
        return ""

    # pure doji (identical to your old helper)
    def _is_pure_doji(_o, _c, _h, _l, body_pct=0.5, center_pct=0.2):
        _rng = _h - _l
        if any(pd.isna([_o, _c, _h, _l])) or _rng == 0:
            return False
        body = abs(_c - _o)
        if body > body_pct * _rng:
            return False
        body_center = (_o + _c) / 2.0
        range_center = (_h + _l) / 2.0
        if abs(body_center - range_center) > center_pct * _rng:
            return False
        if (_h - max(_o, _c)) < 0.05 * _rng or (min(_o, _c) - _l) < 0.05 * _rng:
            return False
        return True

    if _is_pure_doji(o, c, h, l):
        return "DOJI"

    # HUGE OPEN tests
    if rng_bar1 > 0.7 * rng_prev:
        return "HUGE OPEN"

    # check bars 2..5 expansion vs first-bar extremes
    n = min(5, len(d))
    for i in range(1, n):
        hi = float(d.loc[i, "High"]); lo = float(d.loc[i, "Low"])
        # any extreme distance from bar1 extremes > 0.9 * prev-range → HUGE OPEN
        if any(abs(x - y) > 0.9 * rng_prev for x in (h, l) for y in (hi, lo)):
            return "HUGE OPEN"

    return "NORMAL"

def compute_rangestatus(df_day_intraday: pd.DataFrame,
                        open_location_tag: str,
                        prev_ohlc: Optional[Dict[str, float]]) -> str:
    """
    Exact translation of your old `new_range_status`:
      Inputs:
        - first 5 bars of today
        - prev-day high/low
        - today's OpenLocation tag
      Output in {"SBR","WAR","SWR","SAR","WBR",""}
    """
    if df_day_intraday is None or df_day_intraday.empty or prev_ohlc is None:
        return ""

    d = df_day_intraday.sort_values("DateTime").reset_index(drop=True)
    n = min(5, len(d))
    if n == 0:
        return ""

    try:
        H = float(prev_ohlc.get("high", np.nan))
        L = float(prev_ohlc.get("low",  np.nan))
    except Exception:
        H = L = np.nan
    if pd.isna(H) or pd.isna(L) or H <= L:
        return ""

    # scan bars 1..5 for relation to prev-day range
    in_range = False
    above = False
    below = False
    for i in range(n):
        o = float(d.loc[i, "Open"]); c = float(d.loc[i, "Close"])
        if (L <= o <= H) or (L <= c <= H):
            in_range = True
        if (o > H) or (c > H):
            above = True
        if (o < L) or (c < L):
            below = True

    ol = (open_location_tag or "").upper()

    if ol == "OBR":
        if (not in_range) and below: return "SBR"
        if in_range and above:       return "WAR"
        if in_range and (not above): return "SWR"
        if above and (not in_range): return "WAR"
    elif ol == "OAR":
        if (not in_range) and above: return "SAR"
        if in_range and below:       return "WBR"
        if in_range and (not below): return "SWR"
        if below and (not in_range): return "WBR"
    else:  # OOH / OIM / OOL
        if above and (not below):                    return "WAR"
        if below and (not above):                    return "WBR"
        if in_range and (not above) and (not below): return "SWR"

    return ""

# ---- wrapper used by backfill/master updaters ----

def compute_openlocation_from_df(df_or_intraday: pd.DataFrame,
                                 day_or_prev,
                                 maybe_prev: Optional[Dict[str, float]] = None) -> str:
    """
    Convenience + compatibility wrapper:

      1) Old style:
         compute_openlocation_from_df(df_day_intraday, prev_ohlc)

      2) New repo style:
         compute_openlocation_from_df(i5_multi_day, day_norm, prev_ohlc)

    In both cases we end up calling compute_openlocation(day_open, prev_ohlc).
    Returns one of {"OAR","OOH","OIM","OOL","OBR"} or "" if not computable.
    """
    # pattern (df_day, prev_ohlc)
    if maybe_prev is None:
        df_day_intraday = df_or_intraday
        prev_ohlc = day_or_prev
    else:
        # pattern (i5_multi_day, day_norm, prev_ohlc)
        df_intraday = df_or_intraday
        prev_ohlc = maybe_prev
        if df_intraday is None or df_intraday.empty or prev_ohlc is None:
            return ""
        dti = _to_dt_series(df_intraday["DateTime"]).dt.normalize()
        day_norm = pd.to_datetime(day_or_prev).normalize()
        df_day_intraday = df_intraday[dti.eq(day_norm)].copy()

    if df_day_intraday is None or df_day_intraday.empty or prev_ohlc is None:
        return ""
    d = df_day_intraday.sort_values("DateTime")
    try:
        day_open = float(d["Open"].iloc[0])
    except Exception:
        return ""
    return compute_openlocation(day_open, prev_ohlc)
