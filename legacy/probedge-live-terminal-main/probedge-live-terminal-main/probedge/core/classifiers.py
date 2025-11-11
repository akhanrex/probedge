# probedge/core/classifiers.py
"""
Unified robust tag classifiers for ProbEdge.

Exports (single source of truth):
- compute_prevdaycontext_robust(prev_O, prev_H, prev_L, prev_C) -> str
- compute_openingtrend_robust(df_day_intraday) -> str
- compute_result_0940_1505(df_day_intraday) -> (str, float)
- compute_openlocation(day_open, prev_ohlc) -> str
- compute_openlocation_from_df(df_day_intraday, prev_ohlc) -> str
- compute_first_candletype(df_day_intraday) -> str
- compute_rangestatus(df_day_intraday) -> str
- prev_trading_day_ohlc(df_intraday, day_norm) -> dict|None
- slice_window(df, start_hm, end_hm) -> DataFrame

All functions treat DateTime as IST-naive wall time.
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
    s = pd.to_datetime(s, errors="coerce")
    return s  # expected to be IST-naive already elsewhere


def slice_window(df_intraday: pd.DataFrame, start_hm, end_hm) -> pd.DataFrame:
    """Return a copy of df filtered to [start_hm, end_hm] inclusive on DateTime."""
    if df_intraday is None or df_intraday.empty:
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close"])
    d = df_intraday.copy()
    d["DateTime"] = _to_dt_series(d["DateTime"])
    t = d["DateTime"].dt.time
    return d[(t >= start_hm) & (t <= end_hm)].copy()


def prev_trading_day_ohlc(df_intraday: pd.DataFrame, day_norm: pd.Timestamp) -> Optional[Dict[str, float]]:
    """
    Find the previous AVAILABLE trading day in df_intraday and return its daily OHLC.
    Avoids weekends/holidays automatically by using the actual dates present.
    """
    if df_intraday is None or df_intraday.empty:
        return None
    dti = _to_dt_series(df_intraday["DateTime"]).dt.normalize()
    alld = sorted(dti.unique())
    p = None
    day_norm = pd.to_datetime(day_norm).normalize()
    for i in range(1, 8):  # look back up to a week
        cand = day_norm - pd.Timedelta(days=i)
        if cand in alld:
            p = cand; break
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

def compute_prevdaycontext_robust(prev_O, prev_H, prev_L, prev_C) -> str:
    H, L, O, C = float(prev_H), float(prev_L), float(prev_O), float(prev_C)
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
    """Return BULL/BEAR/TR for the 09:15–09:40 window."""
    win = slice_window(df_day_intraday, _time(9,15), _time(9,40))
    if win.empty: return "TR"
    win = win.sort_values("DateTime")

    O0 = float(win['Open'].iloc[0])
    Cn = float(win['Close'].iloc[-1])
    Hmax = float(win['High'].max())
    Lmin = float(win['Low'].min())

    move_pct  = 100.0 * (Cn - O0) / max(1e-9, O0)
    range_pct = 100.0 * (Hmax - Lmin) / max(1e-9, O0)
    pos = 0.5 if Hmax<=Lmin else (Cn - Lmin) / (Hmax - Lmin)
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
    win = slice_window(df_day_intraday, _time(9,40), _time(15,5))
    if win.empty: return "TR", 0.0
    win = win.sort_values("DateTime")
    o = float(win['Open'].iloc[0]); c = float(win['Close'].iloc[-1])
    if o == 0: return "TR", 0.0
    ret = 100.0 * (c - o) / o
    if ret >=  T_POST: return "BULL", round(ret, 3)
    if ret <= -T_POST: return "BEAR", round(ret, 3)
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
    if o <= L + 0.3*rng: return "OOL"
    if o > H:            return "OAR"
    if o >= H - 0.3*rng: return "OOH"
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
        if above and (not below):          return "WAR"
        if below and (not above):          return "WBR"
        if in_range and (not above) and (not below): return "SWR"

    return ""

# ---- wrapper used by backfill/master updaters ----
def compute_openlocation_from_df(df_day_intraday: pd.DataFrame,
                                 prev_ohlc: Optional[Dict[str, float]]) -> str:
    """
    Convenience wrapper: take today's first bar open from df and
    return OpenLocation using the same 0.30 band logic.
    Returns one of {"OAR","OOH","OIM","OOL","OBR"} or "" if not computable.
    """
    if df_day_intraday is None or df_day_intraday.empty or prev_ohlc is None:
        return ""
    d = df_day_intraday.sort_values("DateTime")
    try:
        day_open = float(d["Open"].iloc[0])
    except Exception:
        return ""
    return compute_openlocation(day_open, prev_ohlc)


# ===== Glue for terminal adapter =====
# Expose a single entrypoint the server-side adapter will call.
# It accepts: bars_1_to_5 (list of dicts with Open/High/Low/Close),
#             prev_ohlc_tuple: (O,H,L,C) or None,
#             today_open: float or None
# Returns a dict with at least: PDC, OL, OT, plus two more tags.

from datetime import datetime as _dt, time as _time

def _bars_to_df_first5(bars_1_to_5):
    """
    Convert list of first 5 bars (dicts with O/H/L/C) into a DataFrame
    with a synthetic DateTime timeline 09:15, 09:20, 09:25, 09:30, 09:35 IST.
    """
    if not bars_1_to_5:
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close"])

    # Normalize now() to today's date (IST-naive is fine for our logic)
    day = pd.Timestamp.today().normalize()
    times = [_time(9,15), _time(9,20), _time(9,25), _time(9,30), _time(9,35)]
    rows = []
    for i, b in enumerate(bars_1_to_5[:5]):
        t = times[i] if i < len(times) else times[-1]
        dt = pd.Timestamp.combine(day, t)
        rows.append({
            "DateTime": dt,
            "Open":  float(b.get("Open",  float("nan"))),
            "High":  float(b.get("High",  float("nan"))),
            "Low":   float(b.get("Low",   float("nan"))),
            "Close": float(b.get("Close", float("nan"))),
        })
    df = pd.DataFrame(rows)
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    return df

def _tuple_to_prev_dict(prev_ohlc_tuple):
    """
    Convert (O,H,L,C) -> {"open":..., "high":..., "low":..., "close":...}
    """
    if not prev_ohlc_tuple or any(pd.isna(x) for x in prev_ohlc_tuple):
        return None
    O, H, L, C = prev_ohlc_tuple[0], prev_ohlc_tuple[1], prev_ohlc_tuple[2], prev_ohlc_tuple[3]
    # If caller sent (o,h,l,c) in different order, re-map safely:
    # We expect order (O,H,L,C) from the server’s helper.
    return {"open": float(O), "high": float(H), "low": float(L), "close": float(C)}

def compute_all_tags(bars_1_to_5, prev_ohlc_tuple, today_open) -> dict:
    """
    Main entrypoint used by the terminal. Returns a dict with 5 tags:
      PDC, OL, OT, CANDLE, RANGE
    """
    prev_d = _tuple_to_prev_dict(prev_ohlc_tuple)
    df5 = _bars_to_df_first5(bars_1_to_5)

    # PDC from robust prev-day logic
    if prev_d is not None:
        PDC = compute_prevdaycontext_robust(prev_d["open"], prev_d["high"], prev_d["low"], prev_d["close"])
    else:
        PDC = "TR"

    # OL from robust open-location band
    try:
        OL = compute_openlocation(float(today_open), prev_d) if (today_open is not None and prev_d is not None) else ""
        OL = OL or "OIM"  # default to OIM if blank
    except Exception:
        OL = "OIM"

    # OT from robust opening-trend window (09:15–09:40)
    try:
        OT = compute_openingtrend_robust(df5)
    except Exception:
        OT = "TR"

    # Extra tags (don’t affect terminal direction today, but we expose them)
    try:
        CANDLE = compute_first_candletype(df5, prev_d) or "-"
    except Exception:
        CANDLE = "-"
    try:
        RANGE = compute_rangestatus(df5, OL, prev_d) or "-"
    except Exception:
        RANGE = "-"

    return {
        "PDC": PDC,
        "OL":  OL,
        "OT":  OT,
        "CANDLE": CANDLE,
        "RANGE":  RANGE,
    }
