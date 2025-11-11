# app/views/strength.py — Intraday Strength Profiler (09:40→15:05) + Direction Pick (locked rule)

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Optional theme hook; fall back to simple colors
try:
    from app.ui import _current_theme
except Exception:
    def _current_theme() -> Dict[str, str]:
        return {
            "bg": "#0b1020",
            "card": "#111827",
            "grid": "#1f2937",
            "text": "#e5e7eb",
            "muted": "#94a3b8",
            "border": "#1f2937",
            "green": "#10b981",
            "red": "#ef4444",
            "primary": "#7C3AED",
        }

TZ = _current_theme()

# ------------------------------------
# Tunables (mirrors your result window)
# ------------------------------------
T0 = dtime(9, 40)
T1 = dtime(15, 5)
T_POST = 0.60        # % threshold for BULL/BEAR else TR
NEAR_ZERO_BAND = 0.20  # % band to treat as "flat" for TR diagnostics

# --- UI toggles ---
SHOW_DECISION_CARD   = True    # keep card, but we’ll hide it behind "Show computation"
SHOW_CONF_UI         = True    # the 4 mini-bars; also moved behind "Show computation"
SHOW_LEGACY_CHARTS   = False   # old strength bar + reach bar (off by default)
SHOW_SUMMARY_TABLE   = True    # summary table; also behind "Show computation"
SHOW_PLAN_HINTS      = False   # hide Stop/Target line in card for now
# --- small UI toggles ---
SHOW_TM5_SOURCE = False   # hide "tm5min source: ..." line

# New compact top UI
SHOW_COMPACT_TOP     = True    # show just Direction + Confidence %
DETAILS_DEFAULT_OPEN = False   # "Show computation" expander closed by default

# Heavier emphasis on Frequency & Reach; lighter on Strength & Persistence
CONF_WEIGHTS = {"freq": 0.50, "strength": 0.10, "reach": 0.30, "persist": 0.10}
CONF_APPLY_QUALITY = True

# Confidence shaping knobs
CONF_TEMP = 1.6            # >1 sharpens pillar edges; try 1.4–1.8
CONF_QUALITY_FLOOR = 0.60  # never penalize more than 40% for small samples
# Locked decision thresholds
GAP_THRESH_PP = 10.0  # 10 percentage points
N_MIN = 40            # per-side minimum to trust frequency
TIF_WEAK = 0.70       # 70% time-in-favor = weak threshold for overrule guard
# Confidence → color + gating (percent bands)
ABSTAIN_BELOW = 30  # show ABSTAIN (no side) when conf% < this

# --- Target & Risk Guide (under the confidence meter) ---
TRG_SHOW = False          # was True — turn OFF to hide the whole block
TRG_RUNGS = [0.5, 1.0, 1.5]
TRG_MIN_N = 20
TRG_SHOW_IF_ABSTAIN = False

# bands are [lo, hi, color]
CONF_BANDS = [
    (0,   30, "#ef4444"),  # red   : <30%
    (30,  40, "#f59e0b"),  # amber : 30–39%
    (40, 101, "#10b981"),  # green : ≥40%  (moved down so you still see green)
]

# ------------------------------------
# Robust loader for tm5min (primary + fallback)
# ------------------------------------

@dataclass
class TM5LoadInfo:
    source: str  # "primary" | "fallback" | "merged" | "missing"
    rows: int
    start: Optional[pd.Timestamp]
    end: Optional[pd.Timestamp]


@st.cache_data(show_spinner=False)
def load_tm5min(
    inst_key: str = "tm",
) -> Tuple[pd.DataFrame, TM5LoadInfo]:
    # Map instrument → primary/fallback 5m CSVs
    primary_map = {
        "tm":   "data/intraday/tm5min.csv",
        "lt":   "data/intraday/lt5min.csv",
        "sbin": "data/intraday/sbin5min.csv",
        "ae":   "data/intraday/adanient5min.csv",
    }
    # Optional repo-style fallbacks
    fallback_map = {
        "tm":   "data/intraday/TATAMOTORS/tm5min.csv",
        "lt":   "data/intraday/LT/lt5min.csv",
        "sbin": "data/intraday/SBIN/sbin5min.csv",
        "ae":   "data/intraday/ADANIENT/adanient5min.csv",
    }

    def _read_one(p: Path) -> pd.DataFrame:
        if not p.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(p)
        except Exception:
            return pd.DataFrame()
        cols = {c.lower(): c for c in df.columns}
        def c_(x): return cols.get(x, x)
        df = df.rename(
            columns={
                c_("datetime"): "DateTime",
                c_("date_time"): "DateTime",
                c_("date"): "Date",
                c_("open"): "Open",
                c_("high"): "High",
                c_("low"): "Low",
                c_("close"): "Close",
                c_("volume"): "Volume",
            }
        )
        if "DateTime" not in df.columns:
            time_col = None
            for cand in ("Time","time","Timestamp","timestamp"):
                if cand in df.columns:
                    time_col = cand; break
            if "Date" in df.columns and time_col:
                df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df[time_col].astype(str), errors="coerce")
            elif "Date" in df.columns:
                df["DateTime"] = pd.to_datetime(df["Date"], errors="coerce")
        else:
            df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
        keep = ["DateTime","Open","High","Low","Close","Volume"]
        for k in keep:
            if k not in df.columns:
                df[k] = np.nan
        df = df.dropna(subset=["DateTime","Open","High","Low","Close"]).copy()
        for k in ("Open","High","Low","Close","Volume"):
            df[k] = pd.to_numeric(df[k], errors="coerce")
        df = df.dropna(subset=["Open","High","Low","Close"]).copy()
        return df.sort_values("DateTime").reset_index(drop=True)

    pk = (inst_key or "tm").lower()
    p_primary = Path(primary_map.get(pk, primary_map["tm"])).resolve()
    p_fallback = Path(fallback_map.get(pk, fallback_map["tm"])).resolve()

    d1 = _read_one(p_primary)
    d2 = _read_one(p_fallback)

    if d1.empty and d2.empty:
        info = TM5LoadInfo("missing", 0, None, None)
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume"]).astype({"DateTime":"datetime64[ns]"}), info
    if not d1.empty and d2.empty:
        info = TM5LoadInfo("primary", len(d1), d1["DateTime"].min(), d1["DateTime"].max()); return d1, info
    if d1.empty and not d2.empty:
        info = TM5LoadInfo("fallback", len(d2), d2["DateTime"].min(), d2["DateTime"].max()); return d2, info

    d1["__src"] = 1; d2["__src"] = 2
    both = pd.concat([d1, d2], ignore_index=True)
    both = (both.sort_values(["DateTime","__src"])
                 .drop_duplicates(subset=["DateTime"], keep="first")
                 .drop(columns=["__src"])
                 .sort_values("DateTime").reset_index(drop=True))
    info = TM5LoadInfo("merged", len(both), both["DateTime"].min(), both["DateTime"].max())
    return both, info



# ------------------------------------
# Window logic & per-day metrics
# ------------------------------------

def _slice_window(df_day: pd.DataFrame, t0: dtime, t1: dtime) -> pd.DataFrame:
    if df_day is None or df_day.empty:
        return pd.DataFrame()
    s = df_day.copy()
    s["Date"] = pd.to_datetime(s["DateTime"], errors="coerce").dt.date
    s["Time"] = pd.to_datetime(s["DateTime"], errors="coerce").dt.time
    mask = (s["Time"] >= t0) & (s["Time"] <= t1)
    s = s.loc[mask, ["DateTime", "Open", "High", "Low", "Close", "Volume", "Date"]]
    return s.sort_values("DateTime")


def _compute_result_0940_1505(df_day_intraday: pd.DataFrame) -> Tuple[str, float]:
    """Return (label, ret_pct) from first Open @09:40 to last Close @15:05.
    label in {"BULL","BEAR","TR"}, ret_pct is float rounded(3).
    """
    win = _slice_window(df_day_intraday, T0, T1)
    if win.empty:
        return "TR", 0.0
    o = float(win["Open"].iloc[0])
    c = float(win["Close"].iloc[-1])
    if not np.isfinite(o) or o == 0:
        return "TR", 0.0
    ret = 100.0 * (c - o) / o
    if ret >= T_POST:
        return "BULL", round(ret, 3)
    if ret <= -T_POST:
        return "BEAR", round(ret, 3)
    return "TR", round(ret, 3)


def _day_strength_metrics(df_day_intraday: pd.DataFrame) -> Dict[str, float]:
    """Compute per-day strength diagnostics vs 09:40 anchor."""
    win = _slice_window(df_day_intraday, T0, T1)
    zero = {
        "end_ret": 0.0, "run_up_max": 0.0, "drawdown_max": 0.0,
        "auc_pos": 0.0, "auc_neg": 0.0, "time_pos": 0.0, "time_neg": 0.0,
        "near_zero_frac": 0.0,
    }
    if win.empty:
        return zero
    o = float(win["Open"].iloc[0])
    closes = win["Close"].astype(float).to_numpy()
    if not np.isfinite(o) or o == 0 or closes.size == 0:
        return zero
    rets = 100.0 * (closes - o) / o  # percent path vs 09:40 open
    end_ret = float(rets[-1])
    run_up_max = float(np.nanmax(rets))
    drawdown_max = float(max(0.0, -np.nanmin(rets)))
    pos = np.clip(rets, 0, None)
    neg = np.clip(-rets, 0, None)
    auc_pos = float(np.nanmean(pos)) if pos.size else 0.0
    auc_neg = float(np.nanmean(neg)) if neg.size else 0.0
    time_pos = float(np.mean(rets >= 0))
    time_neg = float(np.mean(rets <= 0))
    near_zero_frac = float(np.mean(np.abs(rets) <= NEAR_ZERO_BAND))
    return {
        "end_ret": end_ret,
        "run_up_max": run_up_max,
        "drawdown_max": drawdown_max,
        "auc_pos": auc_pos,
        "auc_neg": auc_neg,
        "time_pos": time_pos,
        "time_neg": time_neg,
        "near_zero_frac": near_zero_frac,
    }

def _conf_color(conf_pct: int) -> str:
    try:
        v = int(conf_pct)
    except Exception:
        v = 0
    for lo, hi, col in CONF_BANDS:
        if lo <= v < hi:
            return col
    return TZ.get("muted", "#94a3b8")

def _pct_to_rupees(pct: float, entry_price: float) -> Optional[float]:
    try:
        if entry_price is None: return None
        ep = float(entry_price)
        if not np.isfinite(ep) or ep <= 0: return None
        v = float(pct)
        if not np.isfinite(v): return None
        return (v / 100.0) * ep
    except Exception:
        return None

def _target_risk_stats(res: pd.DataFrame, side: str) -> Dict[str, object]:
    """
    Build stats for the picked side using per-day max excursions in the 09:40→15:05 window:
      - Favorable:  run_up_max for BULL, drawdown_max for BEAR
      - Adverse:    drawdown_max for BULL, run_up_max for BEAR
      - Rung hit probabilities: P(MFE ≥ rung%)
    """
    side = (side or "").upper()
    dd = res[res["label"] == side].copy() if "label" in res.columns else pd.DataFrame()
    if dd.empty or side not in ("BULL", "BEAR"):
        return {"n": 0}

    fav = dd["run_up_max"].astype(float) if side == "BULL" else dd["drawdown_max"].astype(float)
    adv = dd["drawdown_max"].astype(float) if side == "BULL" else dd["run_up_max"].astype(float)
    fav = fav.replace([np.inf, -np.inf], np.nan).dropna()
    adv = adv.replace([np.inf, -np.inf], np.nan).dropna()

    if fav.empty or adv.empty:
        return {"n": int(len(dd))}

    def pctl(s: pd.Series, q: float) -> float:
        try: return float(np.nanpercentile(s, q))
        except Exception: return np.nan

    def hit_prob(s: pd.Series, rung: float) -> float:
        try:
            s = s.dropna()
            if s.size == 0: return np.nan
            return 100.0 * float(np.mean(s >= float(rung)))
        except Exception:
            return np.nan

    stats = {
        "n": int(len(dd)),
        "fav_p50": pctl(fav, 50),
        "fav_p80": pctl(fav, 80),
        "adv_p50": pctl(adv, 50),
        "adv_p80": pctl(adv, 80),
        "rungs": [(r, hit_prob(fav, r)) for r in TRG_RUNGS],
    }
    return stats
    
# ------------------------------------
# Decision helper (locked rule)
# ------------------------------------

def _freq_from_matches(df_matches: pd.DataFrame) -> Dict[str, float]:
    """Return frequency-based BULL/BEAR % (ignores TR), plus counts."""
    lab = (
        df_matches.get("Result", pd.Series(dtype=str))
        .astype(str).str.strip().str.upper()
    )
    b = int((lab == "BULL").sum())
    r = int((lab == "BEAR").sum())
    total = b + r
    if total == 0:
        return {"bull_pct": np.nan, "bear_pct": np.nan, "bull_n": b, "bear_n": r, "gap_pp": np.nan}
    bull_pct = 100.0 * b / total
    bear_pct = 100.0 * r / total
    return {"bull_pct": bull_pct, "bear_pct": bear_pct, "bull_n": b, "bear_n": r, "gap_pp": abs(bull_pct - bear_pct)}

def _ladder_pick(agg_row_bull: pd.Series, agg_row_bear: pd.Series) -> Tuple[str, Dict[str, float]]:
    """Return ('BULL'|'BEAR'|'ABSTAIN', details) using 2–1 ladder."""
    def safe(v): return float(v) if pd.notna(v) else np.nan

    # BULL metrics
    tif_b = safe(agg_row_bull.get("time_in_favor"))
    reach_b = safe(agg_row_bull.get("run_up_p80"))
    adv_b = safe(agg_row_bull.get("drawdown_p80"))

    # BEAR metrics
    tif_r = safe(agg_row_bear.get("time_in_favor"))
    reach_r = safe(agg_row_bear.get("drawdown_p80"))
    adv_r = safe(agg_row_bear.get("run_up_p80"))

    wins_b = int(tif_b > tif_r) + int(reach_b > reach_r) + int(adv_b < adv_r)
    wins_r = int(tif_r > tif_b) + int(reach_r > reach_b) + int(adv_r < adv_b)

    if wins_b > wins_r:
        return "BULL", {"wins_b": wins_b, "wins_r": wins_r, "tif_b": tif_b, "tif_r": tif_r, "reach_b": reach_b, "reach_r": reach_r, "adv_b": adv_b, "adv_r": adv_r}
    elif wins_r > wins_b:
        return "BEAR", {"wins_b": wins_b, "wins_r": wins_r, "tif_b": tif_b, "tif_r": tif_r, "reach_b": reach_b, "reach_r": reach_r, "adv_b": adv_b, "adv_r": adv_r}
    else:
        return "ABSTAIN", {"wins_b": wins_b, "wins_r": wins_r, "tif_b": tif_b, "tif_r": tif_r, "reach_b": reach_b, "reach_r": reach_r, "adv_b": adv_b, "adv_r": adv_r}

def _overrule_allowed(freq_side: str, agg_row_f: pd.Series, agg_row_o: pd.Series) -> bool:
    """Only allow strength to overrule frequency when:
       - favored side TIF < 0.70, AND
       - other side adverse ≤ 2/3 of favored adverse, AND
       - other side reach ≥ favored reach."""
    def safe(v): return float(v) if pd.notna(v) else np.nan

    if freq_side == "BULL":
        tif_f = safe(agg_row_f.get("time_in_favor"))
        adv_f = safe(agg_row_f.get("drawdown_p80"))
        reach_f = safe(agg_row_f.get("run_up_p80"))

        adv_o = safe(agg_row_o.get("run_up_p80"))        # BEAR adverse
        reach_o = safe(agg_row_o.get("drawdown_p80"))    # BEAR reach
    else:  # BEAR favored
        tif_f = safe(agg_row_f.get("time_in_favor"))
        adv_f = safe(agg_row_f.get("run_up_p80"))        # BEAR adverse
        reach_f = safe(agg_row_f.get("drawdown_p80"))    # BEAR reach

        adv_o = safe(agg_row_o.get("drawdown_p80"))      # BULL adverse
        reach_o = safe(agg_row_o.get("run_up_p80"))      # BULL reach

    cond_tif = (pd.notna(tif_f) and tif_f < TIF_WEAK)
    cond_adv = (pd.notna(adv_f) and pd.notna(adv_o) and adv_o <= (2.0/3.0) * adv_f)
    cond_reach = (pd.notna(reach_o) and pd.notna(reach_f) and reach_o >= reach_f)
    return bool(cond_tif and cond_adv and cond_reach)

def _format_pct(x: float, digits=2) -> str:
    try:
        return f"{x:.{digits}f}%"
    except Exception:
        return "—"

def _prob_ratio(a: float, b: float) -> float:
    """Return a/(a+b) in [0,1], or 0.5 if not computable."""
    try:
        a = float(a); b = float(b)
        if not np.isfinite(a) or not np.isfinite(b): return 0.5
        s = a + b
        return 0.5 if s <= 0 else float(a / s)
    except Exception:
        return 0.5
def _sharpen(p: float, beta: float = 1.0) -> float:
    """Symmetric power transform around 0.5; beta>1 sharpens edges."""
    try:
        p = float(p)
    except Exception:
        return 0.5
    if not np.isfinite(p):
        return 0.5
    p = min(max(p, 0.0), 1.0)
    if beta <= 1.0:
        return p
    # Equivalent to: p' = p**beta / (p**beta + (1-p)**beta)
    num = p ** beta
    den = num + (1.0 - p) ** beta
    return 0.5 if den <= 0 else float(num / den)
    
def _combined_confidence(
    pick: str,
    bull_pct: float, bear_pct: float,
    str_b: float, str_r: float,
    reach_b: float, reach_r: float,
    tif_b: float, tif_r: float,
    n_bull: int, n_bear: int,
    weights: Dict[str, float],
    apply_quality: bool = True,
    temp: float = CONF_TEMP,
) -> Tuple[int, Dict[str, float]]:
    """
    Build a single % confidence from Frequency, Strength, Reach, Persistence.
    Steps:
      1) Convert each pillar into a win probability for `pick` via ratio.
      2) Sharpen edges with a symmetric power transform (temp > 1).
      3) Weighted average by `weights`.
      4) Light quality attenuation based on min(n_bull, n_bear).
    """
    # 1) Pillar probabilities aligned to pick
    if pick == "BULL":
        p_freq = _prob_ratio(bull_pct, bear_pct)
        p_str  = _prob_ratio(str_b, str_r)
        p_reach= _prob_ratio(reach_b, reach_r)
        p_pers = _prob_ratio(tif_b, tif_r)
    elif pick == "BEAR":
        p_freq = _prob_ratio(bear_pct, bull_pct)
        p_str  = _prob_ratio(str_r, str_b)
        p_reach= _prob_ratio(reach_r, reach_b)
        p_pers = _prob_ratio(tif_r, tif_b)
    else:
        # ABSTAIN → symmetric view (still informative for the %)
        p_freq = _prob_ratio(max(bull_pct, bear_pct), min(bull_pct, bear_pct))
        p_str  = _prob_ratio(max(str_b, str_r),      min(str_b, str_r))
        p_reach= _prob_ratio(max(reach_b, reach_r),  min(reach_b, reach_r))
        p_pers = _prob_ratio(max(tif_b, tif_r),      min(tif_b, tif_r))

    # 2) Sharpen edges so clear spreads read higher
    beta = max(1.0, float(temp))
    p_freq   = _sharpen(p_freq,   beta)
    p_str    = _sharpen(p_str,    beta)
    p_reach  = _sharpen(p_reach,  beta)
    p_pers   = _sharpen(p_pers,   beta)

    # 3) Weighted blend (F/S/R/P)
    w = weights or {"freq":0.50, "strength":0.10, "reach":0.30, "persist":0.10}
    base = (
        w["freq"]     * p_freq +
        w["strength"] * p_str  +
        w["reach"]    * p_reach+
        w["persist"]  * p_pers
    )

    # 4) Softer quality attenuation
    quality = 1.0
    if apply_quality:
        try:
            m = min(int(n_bull), int(n_bear))
            r = m / float(N_MIN if N_MIN > 0 else 1)
            # gentler curve + floor (e.g., m=16 vs N_MIN=40 → sqrt(0.4)=0.632 → floored to 0.60)
            quality = float(np.clip(r ** 0.5, CONF_QUALITY_FLOOR, 1.0))
        except Exception:
            quality = 1.0

    conf = base * quality
    conf_pct = int(round(100.0 * conf))

    return conf_pct, {
        "p_freq": p_freq, "p_strength": p_str, "p_reach": p_reach, "p_persist": p_pers,
        "base": base, "quality": quality, "beta": beta
    }

# ------------------------------------
# Public API — render function
# ------------------------------------

def render_strength_profiler(df_matches: pd.DataFrame, *, inst_key: str = "tm") -> None:
    """Render the Intraday Strength Profiler just below the donuts."""
    st.markdown("### 🧭 Direction & Confidence")
    # --- Recompute guard: only when the filtered set changes ---
    try:
        dts = (
            pd.to_datetime(df_matches["Date"], errors="coerce")
            .dropna().dt.normalize().sort_values()
        )
        sig_key = (len(dts), dts.iloc[0] if len(dts) else None, dts.iloc[-1] if len(dts) else None)
    except Exception:
        sig_key = None

    sig_state_key = f"__isp_last_key_{inst_key}"
    if st.session_state.get(sig_state_key) == sig_key:
        return  # skip all heavy work; nothing changed since last Apply

    # 1) Load tm5min
    # profiler enabled for all instruments (tm/lt/sbin/…)
    pass

    tm5, info = load_tm5min(inst_key=inst_key)
    if info.source == "missing" or tm5.empty:
        st.warning("tm5min not found. Place it at data/intraday/tm5min.csv or data/intraday/TATAMOTORS/tm5min.csv")
        return

    src_label = {"primary": "primary", "fallback": "fallback", "merged": "merged"}.get(info.source, info.source)

    if SHOW_TM5_SOURCE:
        try:
            st.caption(
                f"tm5min source: {src_label} · rows={info.rows:,} · {pd.to_datetime(info.start).date()} → {pd.to_datetime(info.end).date()}"
            )
        except Exception:
            st.caption(f"tm5min source: {src_label} · rows={info.rows:,}")

    # 2) Get dates to evaluate
    if df_matches is None or df_matches.empty or "Date" not in df_matches.columns:
        st.info("No matched days in view.")
        return

    dates = (
        pd.to_datetime(df_matches["Date"], errors="coerce")
        .dt.normalize()
        .dropna()
        .drop_duplicates()
        .sort_values()
    )
    if dates.empty:
        st.info("No valid dates in current selection.")
        return

    # 3) Index tm5min by day for fast slicing
    tm5 = tm5.copy()
    tm5["Date"] = pd.to_datetime(tm5["DateTime"], errors="coerce").dt.normalize()
    by_day = {k: v for k, v in tm5.groupby("Date")}

    rows: List[Dict[str, float]] = []
    have = 0
    for d in dates:
        day_df = by_day.get(pd.to_datetime(d).normalize())
        if day_df is None or day_df.empty:
            continue
        have += 1
        label, _ = _compute_result_0940_1505(day_df)
        met = _day_strength_metrics(day_df)
        rows.append({"Date": pd.to_datetime(d), "label": label, **met})

    if not rows:
        st.info("None of the matched days have intraday data in tm5min.")
        return

    # Coverage
    cov = 100.0 * have / float(len(dates))
    st.caption(f"Coverage: {have}/{len(dates)} matched days have intraday bars ({cov:.1f}%).")

    res = pd.DataFrame(rows)
    # Intraday frequency (from our computed 09:40–15:05 labels)
    intr_bull = int((res["label"] == "BULL").sum())
    intr_bear = int((res["label"] == "BEAR").sum())

    # 4) Aggregate by label
    def _agg_for(lbl: str) -> pd.Series:
        df = res[res["label"] == lbl]
        if df.empty:
            return pd.Series(
                {
                    "n": 0,
                    "end_ret_mean": np.nan,
                    "end_ret_med": np.nan,
                    "auc_pos_mean": np.nan,
                    "auc_neg_mean": np.nan,
                    "run_up_p80": np.nan,
                    "drawdown_p80": np.nan,
                    "time_in_favor": np.nan,  # fraction
                    "near_zero_mean": np.nan,
                }
            )
        if lbl == "BULL":
            time_in_favor = float(np.mean(df["time_pos"]))
            auc_fav = float(np.mean(df["auc_pos"]))
        elif lbl == "BEAR":
            time_in_favor = float(np.mean(df["time_neg"]))
            auc_fav = float(np.mean(df["auc_neg"]))
        else:  # TR
            time_in_favor = float(np.mean(df["near_zero_frac"]))
            auc_fav = float(np.mean((df["auc_pos"] + df["auc_neg"])))
        return pd.Series(
            {
                "n": len(df),
                "end_ret_mean": float(df["end_ret"].mean()),
                "end_ret_med": float(df["end_ret"].median()),
                "auc_pos_mean": float(df["auc_pos"].mean()),
                "auc_neg_mean": float(df["auc_neg"].mean()),
                "run_up_p80": float(np.nanpercentile(df["run_up_max"], 80)),
                "drawdown_p80": float(np.nanpercentile(df["drawdown_max"], 80)),
                "time_in_favor": time_in_favor,
                "auc_fav_mean": auc_fav,
                "near_zero_mean": float(df["near_zero_frac"].mean()),
            }
        )

    agg = (
        pd.concat({lbl: _agg_for(lbl) for lbl in ["BULL", "BEAR", "TR"]}, axis=1)
          .T.reset_index().rename(columns={"index": "Label"})
    )
    # ---- Strength score (always compute; used by table & UI)
    def _score_row(r: pd.Series) -> float:
        if r["Label"] == "BULL":
            auc = r["auc_pos_mean"]; reach = r["run_up_p80"]; end = max(0.0, r["end_ret_mean"])
        elif r["Label"] == "BEAR":
            auc = r["auc_neg_mean"]; reach = r["drawdown_p80"]; end = max(0.0, -r["end_ret_mean"])
        else:
            auc = max(0.0, 2.0 - (r["auc_pos_mean"] + r["auc_neg_mean"]))
            reach = max(0.0, 2.0 - max(r["run_up_p80"], r["drawdown_p80"]))
            end = max(0.0, 1.0 - abs(r["end_ret_mean"]))
        return float(0.5 * auc + 0.3 * reach + 0.2 * end)
    
    agg["strength_score"] = agg.apply(_score_row, axis=1)

    # -----------------------------
    # Direction Pick (locked rule)
    # -----------------------------
    # Try master frequency first, then fall back to intraday labels
    freq = _freq_from_matches(df_matches)
    bull_pct = freq["bull_pct"]
    bear_pct = freq["bear_pct"]
    bull_n = freq["bull_n"]
    bear_n = freq["bear_n"]
    gap_pp = freq["gap_pp"]

    # Fallback if no BULL/BEAR in df_matches
    if (bull_n + bear_n) == 0 or not (pd.notna(bull_pct) and pd.notna(bear_pct)):
        total = intr_bull + intr_bear
        if total > 0:
            bull_n, bear_n = intr_bull, intr_bear
            bull_pct = 100.0 * bull_n / total
            bear_pct = 100.0 * bear_n / total
            gap_pp = abs(bull_pct - bear_pct)
            freq_basis_note = " (using intraday 09:40–15:05 labels)"
        else:
            freq_basis_note = ""
    else:
        freq_basis_note = " (from master Result)"

    # pull strength rows
    row_b = agg[agg["Label"] == "BULL"].iloc[0] if (agg["Label"] == "BULL").any() else pd.Series(dtype=float)
    row_r = agg[agg["Label"] == "BEAR"].iloc[0] if (agg["Label"] == "BEAR").any() else pd.Series(dtype=float)

    # helper to form risk plan for a side
    def _plan(side: str) -> Tuple[float, float]:
        if side == "BULL":
            stop = float(row_b["drawdown_p80"]) if "drawdown_p80" in row_b else np.nan
            tgt  = float(row_b["run_up_p80"])   if "run_up_p80" in row_b else np.nan
        else:
            stop = float(row_r["run_up_p80"])   if "run_up_p80" in row_r else np.nan
            tgt  = float(row_r["drawdown_p80"]) if "drawdown_p80" in row_r else np.nan
        return stop, tgt

    pick = "ABSTAIN"
    reason = ""
    plan_stop = np.nan
    plan_tgt = np.nan

    # decide
    both_have_N = (bull_n >= N_MIN) and (bear_n >= N_MIN)
    if both_have_N and pd.notna(bull_pct) and pd.notna(bear_pct) and pd.notna(gap_pp):
        fav = "BULL" if bull_pct > bear_pct else "BEAR"
        oth = "BEAR" if fav == "BULL" else "BULL"

        if gap_pp >= GAP_THRESH_PP:
            # frequency decides unless overrule criteria met
            row_fav = row_b if fav == "BULL" else row_r
            row_oth = row_r if fav == "BULL" else row_b
            if _overrule_allowed(fav, row_fav, row_oth):
                # strength says flip
                pick = oth
                reason = (
                    f"Overrule: {fav} freq favored "
                    f"({_format_pct(bull_pct if fav=='BULL' else bear_pct,0)} vs "
                    f"{_format_pct(bear_pct if fav=='BULL' else bull_pct,0)}; gap {gap_pp:.0f}pp) "
                    f"but strength clearly better for {oth}"
                )
            else:
                pick = fav
                reason = f"Frequency edge: {fav} (gap {gap_pp:.0f}pp; N={bull_n}/{bear_n}){freq_basis_note}"
        else:
            # Tie-break: prefer Freq+Reach alignment; else weighted F/R-heavy blend; fallback to ladder
            def _g(v):
                try:
                    x = float(v)
                    return x if np.isfinite(x) else np.nan
                except Exception:
                    return np.nan

            # Leaders (only when both values are finite)
            freq_leader = (
                "BULL" if (pd.notna(bull_pct) and pd.notna(bear_pct) and bull_pct > bear_pct)
                else ("BEAR" if (pd.notna(bull_pct) and pd.notna(bear_pct) and bear_pct > bull_pct) else None)
            )
            reach_b_ = _g(row_b.get("run_up_p80", np.nan))       # BULL favorable
            reach_r_ = _g(row_r.get("drawdown_p80", np.nan))     # BEAR favorable
            reach_leader = (
                "BULL" if (pd.notna(reach_b_) and pd.notna(reach_r_) and reach_b_ > reach_r_)
                else ("BEAR" if (pd.notna(reach_b_) and pd.notna(reach_r_) and reach_r_ > reach_b_) else None)
            )

            if (freq_leader is not None) and (reach_leader is not None) and (freq_leader == reach_leader):
                # Shortcut when your two most important pillars agree
                pick = freq_leader
                reason = (
                    f"Freq+Reach align → {pick} "
                    f"(freq B:{bull_pct:.0f}%/R:{bear_pct:.0f}%, reach {reach_b_:.2f}% vs {reach_r_:.2f}%)"
                )
            else:
                # Weighted tie-break (F/R heavy; TIF & Adverse light)
                W = {"freq": 0.45, "reach": 0.35, "tif": 0.15, "adv": 0.05}

                p_freq_b = _prob_ratio(bull_pct, bear_pct)
                p_freq_r = _prob_ratio(bear_pct, bull_pct)

                p_reach_b = _prob_ratio(reach_b_, reach_r_)
                p_reach_r = _prob_ratio(reach_r_, reach_b_)

                tif_b_ = _g(row_b.get("time_in_favor", np.nan))
                tif_r_ = _g(row_r.get("time_in_favor", np.nan))
                p_tif_b = _prob_ratio(tif_b_, tif_r_)
                p_tif_r = _prob_ratio(tif_r_, tif_b_)

                # Adverse: smaller is better → invert comparison
                adv_b_ = _g(row_b.get("drawdown_p80", np.nan))   # BULL adverse
                adv_r_ = _g(row_r.get("run_up_p80", np.nan))     # BEAR adverse
                p_adv_b = _prob_ratio(adv_r_, adv_b_)            # high when adv_b_ < adv_r_
                p_adv_r = _prob_ratio(adv_b_, adv_r_)

                score_b = W["freq"]*p_freq_b + W["reach"]*p_reach_b + W["tif"]*p_tif_b + W["adv"]*p_adv_b
                score_r = W["freq"]*p_freq_r + W["reach"]*p_reach_r + W["tif"]*p_tif_r + W["adv"]*p_adv_r

                if not np.isfinite(score_b): score_b = 0.5
                if not np.isfinite(score_r): score_r = 0.5

                pick = "BULL" if score_b >= score_r else "BEAR"
                reason = f"Weighted tie-break (F/R>TIF/Adv): B {score_b:.2f} vs R {score_r:.2f}"

            if pick == "ABSTAIN":
                # Rare fallback
                side, d = _ladder_pick(row_b, row_r)
                pick = side
                reason = f"Tie-break fallback (ladder): {side}"
    else:
        # sample light or no clear freq → use ladder directly
        side, d = _ladder_pick(row_b, row_r)
        pick = side
        if bull_n + bear_n == 0:
            reason = "No BULL/BEAR frequency sample → strength ladder"
        elif (bull_n < N_MIN) or (bear_n < N_MIN):
            reason = f"Sample light (N_BULL={bull_n}, N_BEAR={bear_n}) → strength ladder"
        else:
            reason = "No decisive frequency gap → strength ladder"

    if pick in ("BULL", "BEAR"):
        plan_stop, plan_tgt = _plan(pick)

    # Chop advisory from TR near-zero (informational only)
    try:
        row_tr = agg[agg["Label"] == "TR"].iloc[0]
        chop_frac = float(row_tr.get("near_zero_mean", np.nan))
    except Exception:
        chop_frac = np.nan

    # ---------- Compact confidence inputs (from agg) ----------
    def _safe_get(s: pd.Series, key: str) -> float:
        try:
            v = float(s.get(key, np.nan))
            return v if pd.notna(v) else np.nan
        except Exception:
            return np.nan

    row_b_full = agg[agg["Label"] == "BULL"].iloc[0] if (agg["Label"] == "BULL").any() else pd.Series(dtype=float)
    row_r_full = agg[agg["Label"] == "BEAR"].iloc[0] if (agg["Label"] == "BEAR").any() else pd.Series(dtype=float)

    # Strength (composite score computed earlier)
    str_b = _safe_get(row_b_full, "strength_score")
    str_r = _safe_get(row_r_full, "strength_score")

    # Reach (typical favorable move)
    reach_b = _safe_get(row_b_full, "run_up_p80")     # BULL favorable
    reach_r = _safe_get(row_r_full, "drawdown_p80")   # BEAR favorable

    # Persistence (time in favor 0..1)
    tif_b = _safe_get(row_b_full, "time_in_favor")
    tif_r = _safe_get(row_r_full, "time_in_favor")

    # Build single confidence %
    conf_pct, conf_parts = _combined_confidence(
        pick,
        bull_pct, bear_pct,
        str_b, str_r,
        reach_b, reach_r,
        tif_b, tif_r,
        bull_n, bear_n,
        CONF_WEIGHTS,
        apply_quality=CONF_APPLY_QUALITY,
        temp=CONF_TEMP,
    )
    
    display_pick = pick if conf_pct >= ABSTAIN_BELOW else "ABSTAIN"
    bar_color = _conf_color(conf_pct)

    # === COMPACT TOP: Direction + Confidence % ===
    if SHOW_COMPACT_TOP:
        color = bar_color             # color from confidence band, not from side
        bgbar = TZ["border"]
        st.markdown(
            f"""
            <div style="border:1px solid {TZ['border']}; background:{TZ['card']}; border-radius:14px; padding:14px; margin:8px 0 14px;">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:14px;flex-wrap:wrap">
                <div style="font-weight:800;color:{TZ['text']};font-size:15px;">🧭 Direction</div>
                <div style="font-weight:900;color:{color};border:1px solid {color}40;background:{color}20;padding:6px 10px;border-radius:10px;">
                  {display_pick}
                </div>
              </div>
              {(lambda w=CONF_WEIGHTS: f"<div style='margin-top:10px;color:{TZ['muted']};font-size:12px;'>Confidence (weights F/S/R/P = {int(100*w['freq'])}/{int(100*w['strength'])}/{int(100*w['reach'])}/{int(100*w['persist'])}%)</div>")()}
              <div style="margin-top:8px;height:16px;width:100%;background:{bgbar};border-radius:10px;overflow:hidden;">
                <div style="height:100%;width:{conf_pct}%;background:{color};opacity:0.9;"></div>
              </div>
              <div style="margin-top:6px;color:{TZ['text']};font-weight:800;">{conf_pct}%</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- ADD: 🎯 Target & Risk Guide (below the meter) ---
    if TRG_SHOW and (display_pick != "ABSTAIN" or TRG_SHOW_IF_ABSTAIN):
        side_for_stats = pick if pick in ("BULL", "BEAR") else None
        if side_for_stats is not None:
            trg = _target_risk_stats(res, side_for_stats)
            n_side = int(trg.get("n", 0))
            if n_side >= 1:
                # Optional entry price (₹) from the user
                ep_str = st.text_input(
                    "Entry price (₹) — optional",
                    value="",
                    placeholder="e.g., 995.50",
                    key=f"trg_entry_{inst_key}"
                )
                try:
                    entry_px = float(ep_str) if ep_str.strip() != "" else None
                except Exception:
                    entry_px = None

                fav_p50 = trg.get("fav_p50", np.nan)
                fav_p80 = trg.get("fav_p80", np.nan)
                adv_p50 = trg.get("adv_p50", np.nan)
                adv_p80 = trg.get("adv_p80", np.nan)

                # Convert to ₹ if entry provided
                fav50_rs = _pct_to_rupees(fav_p50, entry_px)
                fav80_rs = _pct_to_rupees(fav_p80, entry_px)
                adv50_rs = _pct_to_rupees(adv_p50, entry_px)
                adv80_rs = _pct_to_rupees(adv_p80, entry_px)

                # Heading + small chips
                chips = []
                if n_side < TRG_MIN_N:
                    chips.append(("LOW SAMPLE", "#f59e0b"))
                if display_pick == "ABSTAIN":
                    chips.append(("ABSTAIN MODE", "#94a3b8"))

                chip_html = "".join(
                    f"<span style='padding:3px 8px;margin-left:6px;border-radius:10px;border:1px solid {c}40;color:{c};background:{c}20;font-size:11px;font-weight:700'>{t}</span>"
                    for (t, c) in chips
                )

                st.markdown(
                    f"""
                    <div style="border:1px solid {TZ['border']}; background:{TZ['card']}; border-radius:12px; padding:12px; margin:-2px 0 12px;">
                      <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;flex-wrap:wrap">
                        <div style="font-weight:800;color:{TZ['text']}">🎯 Target & Risk Guide (historical 09:40→15:05)</div>
                        <div>{chip_html}</div>
                      </div>
                      <div style="margin-top:6px;color:{TZ['muted']};font-size:12px">
                        Based on {n_side} matched <b>{side_for_stats}</b> days (touch-probability; percent moves vs 09:40 open).
                      </div>

                      <div style="display:grid;grid-template-columns: repeat(2, minmax(180px,1fr)); gap:10px; margin-top:10px;">
                        <div style="border:1px solid {TZ['border']};border-radius:10px;padding:10px;">
                          <div style="color:{TZ['muted']};font-size:12px;">Typical favorable reach</div>
                          <div style="font-weight:800;color:{TZ['text']};margin-top:4px">
                            p50: {(_format_pct(fav_p50,2) if pd.notna(fav_p50) else '—')}
                            {(' · ₹' + f"{fav50_rs:.2f}" if fav50_rs is not None else '')}<br/>
                            p80: {(_format_pct(fav_p80,2) if pd.notna(fav_p80) else '—')}
                            {(' · ₹' + f"{fav80_rs:.2f}" if fav80_rs is not None else '')}
                          </div>
                        </div>
                        <div style="border:1px solid {TZ['border']};border-radius:10px;padding:10px;">
                          <div style="color:{TZ['muted']};font-size:12px;">Typical adverse move</div>
                          <div style="font-weight:800;color:{TZ['text']};margin-top:4px">
                            p50: {(_format_pct(adv_p50,2) if pd.notna(adv_p50) else '—')}
                            {(' · ₹' + f"{adv50_rs:.2f}" if adv50_rs is not None else '')}<br/>
                            p80: {(_format_pct(adv_p80,2) if pd.notna(adv_p80) else '—')}
                            {(' · ₹' + f"{adv80_rs:.2f}" if adv80_rs is not None else '')}
                          </div>
                        </div>
                      </div>

                      <div style="margin-top:10px;">
                        <div style="color:{TZ['muted']};font-size:12px;margin-bottom:4px;">Hit probability to common rungs (touch):</div>
                        <div style="display:flex;gap:8px;flex-wrap:wrap;">
                          {
                            "".join(
                              f"<span style='padding:6px 8px;border-radius:10px;border:1px solid {TZ['border']};background:{TZ['bg']};color:{TZ['text']};font-weight:700;font-size:12px'>"
                              f"{r:.1f}% → {p:.0f}%"
                              f"{(' · ₹' + f'{_pct_to_rupees(r, entry_px):.2f}' if entry_px is not None else '')}"
                              f"</span>"
                              for (r, p) in (trg.get('rungs') or [])
                              if pd.notna(p)
                            )
                          }
                        </div>
                      </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.info("Target & Risk Guide: no matched days for the picked side.")
                
    # =======================
    # DETAILS: Show computation
    # =======================
    with st.expander("Show details", expanded=DETAILS_DEFAULT_OPEN):

        if SHOW_DECISION_CARD:
            # (hide Stop/Target line unless SHOW_PLAN_HINTS)
            stop_target_html = ""
            if SHOW_PLAN_HINTS and np.isfinite(plan_stop) and np.isfinite(plan_tgt):
                stop_target_html = f"<div>Stop: {_format_pct(plan_stop)} • Target: {_format_pct(plan_tgt)}</div>"

            # Decision card (inside details)
            st.markdown(
                f"""
                <div style="border:1px solid {TZ['border']}; background:{TZ['card']}; border-radius:12px; padding:12px; margin:6px 0 14px;">
                  <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap">
                    <div style="font-weight:800;color:{TZ['text']}">🧭 Direction Pick (locked rule)</div>
                    <div style="font-weight:800; padding:6px 10px; border-radius:10px; background:{('#16a34a20' if pick=='BULL' else '#ef444420' if pick=='BEAR' else '#94a3b820')}; color:{('#16a34a' if pick=='BULL' else '#ef4444' if pick=='BEAR' else '#94a3b8')}; border:1px solid {('#16a34a40' if pick=='BULL' else '#ef444440' if pick=='BEAR' else '#94a3b840')};">
                      {pick}
                    </div>
                  </div>
                  <div style="color:{TZ['muted']}; margin-top:6px;">{reason}</div>
                  <div style="margin-top:8px; display:flex; gap:16px; flex-wrap:wrap; color:{TZ['text']}">
                    <div>Prob (B:{_format_pct(bull_pct,0) if pd.notna(bull_pct) else '—'} / R:{_format_pct(bear_pct,0) if pd.notna(bear_pct) else '—'})</div>
                    <div>Samples (B:{bull_n} / R:{bear_n})</div>
                    <div style="color:{TZ['muted']};">{"Choppy backdrop (TR near-zero ≥30%)" if pd.notna(chop_frac) and (100*chop_frac >= 30) else ""}</div>
                  </div>
                  {stop_target_html}
                </div>
                """,
                unsafe_allow_html=True,
            )

        # --- Legacy charts (optional) ---
        if SHOW_LEGACY_CHARTS:
            c1, c2 = st.columns([1.2, 1.0])
            with c1:
                bb = agg[
                    agg["Label"].isin(["BULL", "BEAR"]) & agg["strength_score"].notna()
                ][["Label", "strength_score", "time_in_favor"]].copy()
                if bb.empty:
                    st.info("Not enough bull/bear days to compare strength.")
                else:
                    fig = go.Figure(
                        go.Bar(
                            x=bb["Label"],
                            y=bb["strength_score"],
                            text=[f"{v:.2f}" for v in bb["strength_score"]],
                            textposition="outside",
                        )
                    )
                    fig.update_layout(
                        height=320,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor=TZ["card"],
                        plot_bgcolor=TZ["card"],
                        xaxis=dict(title=None, gridcolor=TZ["grid"]),
                        yaxis=dict(title="Strength (composite)", gridcolor=TZ["grid"]),
                    )
                    st.plotly_chart(fig, use_container_width=True, key="isp_strength_bb")

            with c2:
                rch = agg.copy()
                rch["Reach"] = np.where(
                    rch["Label"] == "BULL", rch["run_up_p80"],
                    np.where(rch["Label"] == "BEAR", rch["drawdown_p80"], np.maximum(rch["run_up_p80"], rch["drawdown_p80"]))
                )
                rch = rch[rch["Label"].isin(["BULL", "BEAR"]) & rch["Reach"].notna()][["Label", "Reach"]]
                if not rch.empty:
                    fig2 = go.Figure(
                        go.Bar(
                            x=rch["Label"],
                            y=rch["Reach"],
                            text=[f"{v:.2f}%" for v in rch["Reach"]],
                            textposition="outside",
                        )
                    )
                    fig2.update_layout(
                        height=320,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor=TZ["card"],
                        plot_bgcolor=TZ["card"],
                        xaxis=dict(title=None, gridcolor=TZ["grid"]),
                        yaxis=dict(title="80th pct favorable move (%)", gridcolor=TZ["grid"]),
                    )
                    st.plotly_chart(fig2, use_container_width=True, key="isp_reach")

        # --- Summary table (optional) ---
        if SHOW_SUMMARY_TABLE:
            show = agg.copy().rename(columns={
                "Label": "Bucket",
                "n": "Days",
                "end_ret_mean": "Avg End %",
                "end_ret_med": "Median End %",
                "auc_pos_mean": "Avg +AUC %",
                "auc_neg_mean": "Avg −AUC %",
                "run_up_p80": "P80 Run-up %",
                "drawdown_p80": "P80 Drawdown %",
                "time_in_favor": "Time in favor",
                "near_zero_mean": "Near-zero %",
                "auc_fav_mean": "Avg AUC (in-favor) %",
                "strength_score": "Strength (score)",
            })

            def _fmt_pct_tbl(x):
                try: return f"{x:.2f}%"
                except Exception: return "—"

            def _fmt_fr_tbl(x):
                try: return f"{100*x:.0f}%"
                except Exception: return "—"

            show["Avg End %"]             = show["Avg End %"].map(_fmt_pct_tbl)
            show["Median End %"]          = show["Median End %"].map(_fmt_pct_tbl)
            show["Avg +AUC %"]            = show["Avg +AUC %"].map(_fmt_pct_tbl)
            show["Avg −AUC %"]            = show["Avg −AUC %"].map(_fmt_pct_tbl)
            show["P80 Run-up %"]          = show["P80 Run-up %"].map(_fmt_pct_tbl)
            show["P80 Drawdown %"]        = show["P80 Drawdown %"].map(_fmt_pct_tbl)
            show["Avg AUC (in-favor) %"]  = show["Avg AUC (in-favor) %"].map(_fmt_pct_tbl)
            show["Time in favor"]         = show["Time in favor"].map(_fmt_fr_tbl)
            show["Near-zero %"]           = show["Near-zero %"].map(_fmt_fr_tbl)
            show["Strength (score)"]      = show["Strength (score)"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "—")

            st.dataframe(
                show[[
                    "Bucket","Days","Avg End %","Median End %","Avg AUC (in-favor) %",
                    "P80 Run-up %","P80 Drawdown %","Time in favor","Near-zero %","Strength (score)"
                ]],
                use_container_width=True,
                height=360,
            )

            st.caption(
                "Definitions: AUC = mean excursion vs 09:40 open over the window; "
                "'in-favor' uses +AUC for BULL, −AUC for BEAR; Reach = 80th pct of daily max favorable move; "
                "Time in favor = fraction of bars aligned with bucket."
            )

        # --- Confidence breakdown (four mini-bars) ---
        if SHOW_CONF_UI:
            st.markdown("#### Confidence breakdown (read-only)")

            # rows for bull/bear (safe getters)
            def _row(lbl: str) -> pd.Series:
                try:
                    return agg[agg["Label"] == lbl].iloc[0]
                except Exception:
                    return pd.Series(dtype=float)

            row_b = _row("BULL")
            row_r = _row("BEAR")

            def _safe(s: pd.Series, key: str) -> float:
                try:
                    v = float(s.get(key, np.nan))
                    return v if pd.notna(v) else np.nan
                except Exception:
                    return np.nan

            str_b2 = _safe(row_b, "strength_score")
            str_r2 = _safe(row_r, "strength_score")
            reach_b2 = _safe(row_b, "run_up_p80")
            reach_r2 = _safe(row_r, "drawdown_p80")
            tif_b2 = _safe(row_b, "time_in_favor")
            tif_r2 = _safe(row_r, "time_in_favor")

            chips = []
            try:
                row_tr = agg[agg["Label"] == "TR"].iloc[0]
                tr_near = _safe(row_tr, "near_zero_mean")
                if pd.notna(tr_near) and (100.0 * tr_near >= 30.0):
                    chips.append(("CHOPPY", "#f59e0b"))
            except Exception:
                tr_near = np.nan

            if cov < 90.0:
                chips.append(("LOW COVERAGE", "#f59e0b"))
            try:
                _nb = int(bull_n) if pd.notna(bull_n) else 0
                _nr = int(bear_n) if pd.notna(bear_n) else 0
            except Exception:
                _nb, _nr = 0, 0
            if min(_nb, _nr) < 20:
                chips.append(("IMBALANCED", "#94a3b8"))

            # ---- Row 1: Frequency & Strength
            r1c1, r1c2 = st.columns(2)
            with r1c1:
                if pd.notna(bull_pct) and pd.notna(bear_pct):
                    figF = go.Figure(
                        go.Bar(
                            x=["BULL", "BEAR"],
                            y=[bull_pct, bear_pct],
                            text=[f"{bull_pct:.0f}%", f"{bear_pct:.0f}%"],
                            textposition="outside",
                            marker_color=[TZ["green"], TZ["red"]],
                            showlegend=False,
                        )
                    )
                    figF.update_layout(
                        height=220,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor=TZ["card"],
                        plot_bgcolor=TZ["card"],
                        xaxis=dict(title=None, gridcolor=TZ["grid"]),
                        yaxis=dict(title="Frequency (%)", gridcolor=TZ["grid"]),
                    )
                    st.plotly_chart(figF, use_container_width=True, key=f"isp_conf_freq_{inst_key}")
                    gap_text = f"gap {gap_pp:.0f}pp" if pd.notna(gap_pp) else "gap —"
                    basis = freq_basis_note.strip() if isinstance(freq_basis_note, str) else ""
                    st.caption(f"Basis{basis}: BULL {_nb} vs BEAR {_nr} • {gap_text}")
                else:
                    st.info("Frequency: no BULL/BEAR sample available.")

            with r1c2:
                if pd.notna(str_b2) or pd.notna(str_r2):
                    mx = np.nanmax([str_b2, str_r2]) if np.any(np.isfinite([str_b2, str_r2])) else 1.0
                    y_b = (str_b2 / mx) if pd.notna(str_b2) and mx > 0 else 0
                    y_r = (str_r2 / mx) if pd.notna(str_r2) and mx > 0 else 0
                    figS = go.Figure(
                        go.Bar(
                            x=["BULL", "BEAR"],
                            y=[y_b, y_r],
                            text=[f"{str_b2:.2f}" if pd.notna(str_b2) else "—",
                                  f"{str_r2:.2f}" if pd.notna(str_r2) else "—"],
                            textposition="outside",
                            marker_color=[TZ["green"], TZ["red"]],
                            showlegend=False,
                        )
                    )
                    figS.update_layout(
                        height=220,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor=TZ["card"],
                        plot_bgcolor=TZ["card"],
                        xaxis=dict(title=None, gridcolor=TZ["grid"]),
                        yaxis=dict(title="Strength (relative)", showticklabels=False, gridcolor=TZ["grid"]),
                    )
                    st.plotly_chart(figS, use_container_width=True, key=f"isp_conf_strength_{inst_key}")
                    st.caption("Numbers on bars are the exact strength scores (unnormalized).")
                else:
                    st.info("Strength: unavailable.")

            # ---- Row 2: Reach & Persistence
            r2c1, r2c2 = st.columns(2)
            with r2c1:
                if pd.notna(reach_b2) or pd.notna(reach_r2):
                    figR = go.Figure(
                        go.Bar(
                            x=["BULL", "BEAR"],
                            y=[reach_b2 if pd.notna(reach_b2) else 0, reach_r2 if pd.notna(reach_r2) else 0],
                            text=[f"{reach_b2:.2f}%" if pd.notna(reach_b2) else "—",
                                  f"{reach_r2:.2f}%" if pd.notna(reach_r2) else "—"],
                            textposition="outside",
                            marker_color=[TZ["green"], TZ["red"]],
                            showlegend=False,
                        )
                    )
                    figR.update_layout(
                        height=220,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor=TZ["card"],
                        plot_bgcolor=TZ["card"],
                        xaxis=dict(title=None, gridcolor=TZ["grid"]),
                        yaxis=dict(title="Reach (P80 favorable, %)", gridcolor=TZ["grid"]),
                    )
                    st.plotly_chart(figR, use_container_width=True, key=f"isp_conf_reach_{inst_key}")
                else:
                    st.info("Reach: unavailable.")

            with r2c2:
                if pd.notna(tif_b2) or pd.notna(tif_r2):
                    figP = go.Figure(
                        go.Bar(
                            x=["BULL", "BEAR"],
                            y=[100.0 * tif_b2 if pd.notna(tif_b2) else 0, 100.0 * tif_r2 if pd.notna(tif_r2) else 0],
                            text=[f"{100.0*tif_b2:.0f}%" if pd.notna(tif_b2) else "—",
                                  f"{100.0*tif_r2:.0f}%" if pd.notna(tif_r2) else "—"],
                            textposition="outside",
                            marker_color=[TZ["green"], TZ["red"]],
                            showlegend=False,
                        )
                    )
                    figP.update_layout(
                        height=220,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor=TZ["card"],
                        plot_bgcolor=TZ["card"],
                        xaxis=dict(title=None, gridcolor=TZ["grid"]),
                        yaxis=dict(title="Persistence (time in favor, %)", gridcolor=TZ["grid"]),
                    )
                    st.plotly_chart(figP, use_container_width=True, key=f"isp_conf_persist_{inst_key}")
                else:
                    st.info("Persistence: unavailable.")

            # Context chips line
            if chips:
                spans = "".join(
                    f"<span style='padding:4px 8px;margin-right:6px;border-radius:10px;"
                    f"border:1px solid {col}40;color:{col};background:{col}20;font-size:12px;font-weight:700'>{lab}</span>"
                    for (lab, col) in chips
                )
                st.markdown(f"<div style='margin-top:4px'>{spans}</div>", unsafe_allow_html=True)

            # Raw numbers (optional mini-expander so the main button reveals charts/table fully)
            with st.expander("Raw numbers"):
                st.write({
                    "Frequency": {
                        "basis": (freq_basis_note.strip() if isinstance(freq_basis_note, str) else ""),
                        "bull_pct": None if not pd.notna(bull_pct) else round(bull_pct, 2),
                        "bear_pct": None if not pd.notna(bear_pct) else round(bear_pct, 2),
                        "bull_n": _nb,
                        "bear_n": _nr,
                        "gap_pp": None if not pd.notna(gap_pp) else round(gap_pp, 1),
                        "coverage_pct": round(cov, 1),
                    },
                    "Strength": {
                        "score_bull": None if not pd.notna(str_b2) else round(str_b2, 2),
                        "score_bear": None if not pd.notna(str_r2) else round(str_r2, 2),
                    },
                    "Reach": {
                        "p80_favorable_bull_%": None if not pd.notna(reach_b2) else round(reach_b2, 2),
                        "p80_favorable_bear_%": None if not pd.notna(reach_r2) else round(reach_r2, 2),
                    },
                    "Persistence": {
                        "time_in_favor_bull_%": None if not pd.notna(tif_b2) else int(round(100.0 * tif_b2)),
                        "time_in_favor_bear_%": None if not pd.notna(tif_r2) else int(round(100.0 * tif_r2)),
                    },
                    "TR_context": {
                        "near_zero_mean_%": None if not pd.notna(tr_near) else int(round(100.0 * tr_near))
                    },
                    "Confidence blend": {
                        "p_freq": round(conf_parts.get("p_freq", np.nan), 3),
                        "p_strength": round(conf_parts.get("p_strength", np.nan), 3),
                        "p_reach": round(conf_parts.get("p_reach", np.nan), 3),
                        "p_persist": round(conf_parts.get("p_persist", np.nan), 3),
                        "weighted_base": round(conf_parts.get("base", np.nan), 3),
                        "quality_factor": round(conf_parts.get("quality", np.nan), 3),
                        "final_conf_pct": conf_pct,
                    }
                })
                try:
                    small = agg[agg["Label"].isin(["BULL", "BEAR"])][[
                        "Label", "n", "strength_score", "run_up_p80", "drawdown_p80", "time_in_favor"
                    ]].copy()
                    small["time_in_favor"] = (100.0 * small["time_in_favor"]).round(0)
                    st.dataframe(small.rename(columns={
                        "Label": "Bucket",
                        "n": "Days",
                        "strength_score": "Strength (score)",
                        "run_up_p80": "P80 in-favor (BULL)",
                        "drawdown_p80": "P80 in-favor (BEAR)",
                        "time_in_favor": "Time in favor (%)",
                    }), use_container_width=True, height=180)
                except Exception:
                    pass
