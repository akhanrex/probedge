from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd
import streamlit as st

from app.config import HALF_LIFE_DAYS, now_ist
from app.instruments import INSTRUMENTS, get_tm_thresholds
from app.ui import _current_theme, donut, _render_signature_chips_inline

# Safe import (_canon_cast may not exist)
try:
    from probedge.core.rules import _canon_tag_value
except Exception:
    # Minimal fallback (very defensive)
    def _canon_tag_value(col: str, v) -> str:
        s = ("" if v is None else str(v)).strip().upper()
        return "UNKNOWN" if not s else s

from probedge.core.stats import (
    probedge_adv_from_results,
    _eff_weights_by_recency_from_ref,
    refined_quality_score_advanced,
    _stable_ref_date,
)

# ---------------------------------------
# Optional live weekly updater (Kite)
# ---------------------------------------
compute_live_weekly_tags = None
try:
    from probedge.updater.weekly import compute_live_weekly_tags  # preferred
except Exception:
    try:
        from app.updater.weekly import compute_live_weekly_tags  # alt
    except Exception:
        try:
            from weekly import compute_live_weekly_tags  # project-root fallback
        except Exception:
            compute_live_weekly_tags = None

# Only these tags are used for the live signature + donuts
SIG_KEYS = ["PrevDayContext", "OpenLocation", "OpeningTrend"]

# -------------------------
# Cache for today’s live read
# -------------------------
LIVE_CACHE_PATH = os.path.join("/mnt/data", "tm_live_cache.json")


def _prune_tags(tags: dict) -> dict:
    """Keep only SIG_KEYS, drop blanks/UNKNOWN."""
    return {
        k: str(tags.get(k))
        for k in SIG_KEYS
        if tags.get(k) not in (None, "", "UNKNOWN")
    }


def _in_live_window() -> bool:
    """Market live window (IST). Adjust as needed."""
    t = now_ist().time()
    return (t >= pd.to_datetime("09:15").time()) and (t <= pd.to_datetime("15:30").time())


def _save_tm_live_cache(day_key: str, chips_html: str, pill_html: str, tags: dict, meta: dict) -> None:
    try:
        data = {}
        if os.path.exists(LIVE_CACHE_PATH):
            with open(LIVE_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        data[day_key] = {
            "chips_html": chips_html,
            "pill_html": pill_html,
            "tags": tags,
            "meta": meta,
        }
        with open(LIVE_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass


def _load_tm_live_cache(day_key: str) -> dict:
    try:
        if os.path.exists(LIVE_CACHE_PATH):
            with open(LIVE_CACHE_PATH, "r", encoding="utf-8") as f:
                return (json.load(f) or {}).get(day_key, {})
    except Exception:
        pass
    return {}


# ----------
# Public API
# ----------
_HAS_FRAGMENT = hasattr(st, "fragment")

if _HAS_FRAGMENT:
    @st.fragment(run_every=5)
    def render_live_tracker(master_df: pd.DataFrame, *, kite=None):
        _render_live_core(master_df, kite)
else:
    # very old streamlit fallback
    try:
        from streamlit_autorefresh import st_autorefresh
    except Exception:
        st_autorefresh = None

    def render_live_tracker(master_df: pd.DataFrame, *, kite=None):
        if st_autorefresh:
            st_autorefresh(interval=5000, key="live_refresh")
        _render_live_core(master_df, kite)


# ------------------------
# Core render (single path)
# ------------------------
def _render_live_core(master_df: pd.DataFrame, kite=None) -> None:
    st.markdown("### Live Tracker — Weekly signature")

    day_key = now_ist().date().isoformat()
    cached = _load_tm_live_cache(day_key)

    # If we cannot recompute (no updater/kite or outside live window), show cache if present
    can_recompute = (compute_live_weekly_tags is not None) and (kite is not None) and _in_live_window()

    if not can_recompute and cached:
        _render_from_cached(cached)
        return

    # If we get here, we’ll compute (or fallback to latest master row)
    if master_df is None or master_df.empty:
        st.warning("Master CSV not found or empty.")
        if cached:
            st.caption("Falling back to cached view.")
            _render_from_cached(cached)
        return

    # Try live compute first
    state: Dict = {}
    tags: Dict = {}
    ready: Dict = {}
    if can_recompute:
        try:
            state = compute_live_weekly_tags(master_df, kite, symbol="NSE:TATAMOTORS")
            ready = state.get("ready", {}) or {}
            tags = _prune_tags(state.get("tags", {}) or {})
        except Exception as e:
            st.warning(f"Live updater error; falling back to historical-only estimate. ({e})")

    # Fallback to last master row if live tags not available
    if not tags:
        if "Date" in master_df.columns and not master_df.empty:
            last_row = master_df.sort_values("Date").iloc[-1]
            for k in SIG_KEYS:
                v = last_row.get(k, None)
                if pd.notna(v):
                    tags[k] = str(v)
        tags = _prune_tags(tags)

    # Manual override support (optional)
    manual_today = st.session_state.get("manual_today_tags") or {}
    for _k in SIG_KEYS:
        if manual_today.get(_k):
            tags[_k] = manual_today[_k]
    tags = _prune_tags(tags)

    # Signature chips
    st.markdown("#### Today’s Tag Signature")
    chips_html = _render_signature_chips_inline(tags)
    st.markdown(chips_html, unsafe_allow_html=True)

    if not ready.get("bar5", False) and can_recompute:
        st.caption("Live bars not ready — showing estimate based on historical data.")

    # Build df_view by matching canonicalized weekly tag values
    df_view = master_df.copy()
    for col in SIG_KEYS:
        if col not in df_view.columns:
            continue
        weekly_val = _canon_tag_value(col, tags.get(col))
        if weekly_val == "UNKNOWN":
            continue
        df_view = df_view[
            df_view[col].astype(object).apply(lambda v: _canon_tag_value(col, v)) == weekly_val
        ]

    # Ensure Date is datetime for downstream stats
    if "Date" in df_view.columns:
        df_view["Date"] = pd.to_datetime(df_view["Date"], errors="coerce")

    ref_date = _stable_ref_date(master_df)
    adv = probedge_adv_from_results(df_view, ref_date=ref_date, half_life_days=HALF_LIFE_DAYS)

    # adv returns percentages (0–100)
    bull_pct_final = float(adv.get("bull_pct", 50.0))
    bear_pct_final = float(adv.get("bear_pct", 50.0))
    pB = float(adv.get("pB", bull_pct_final))
    pR = float(adv.get("pR", bear_pct_final))

    edge_pp = abs(bull_pct_final - bear_pct_final)
    dominant_is_bull = bull_pct_final >= bear_pct_final
    chosen_pct = bull_pct_final if dominant_is_bull else bear_pct_final
    indecisive = edge_pp < 2.0

    # Completeness across SIG_KEYS + Result (when present)
    comp_cols = [c for c in (SIG_KEYS + ["Result"]) if c in df_view.columns]
    completeness = (
        float(df_view[comp_cols].replace("", None).notna().mean().mean())
        if len(df_view) and comp_cols
        else 0.0
    )

    # Effective sample size
    if len(df_view):
        w = _eff_weights_by_recency_from_ref(df_view["Date"], HALF_LIFE_DAYS, ref_date)
        n_eff = float(w.sum())
    else:
        n_eff = 0.0

    # Normalize depth
    th = get_tm_thresholds()
    depth_norm = float(th.get("depth_norm", 25.0))
    pct_eff = 100.0 * min(n_eff, depth_norm) / depth_norm if depth_norm > 0 else 0.0

    # Quality score — expects probability in 0–1 (scale pB/pR from %)
    prob_for_quality = (pB if dominant_is_bull else pR) / 100.0
    q_score = refined_quality_score_advanced(n_eff, completeness, prob_for_quality)

    # A+ threshold check
    inst = INSTRUMENTS.get("tm")
    if inst:
        best_side_pct = max(bull_pct_final, bear_pct_final)
        aplus = (
            (n_eff >= getattr(inst, "aplus_min_eff", 0))
            and (best_side_pct >= getattr(inst, "aplus_rate", 100))
            and (completeness > getattr(inst, "aplus_comp", 1.0))
        )
    else:
        aplus = False

    # Decide pill purely from direction donut
    if indecisive:
        pill_html = "<span style='padding:6px 12px;border-radius:14px;background:#94a3b820;border:1px solid #94a3b840;color:#94a3b8;font-weight:700;font-size:13px;'>ABSTAIN</span>"
    elif dominant_is_bull:
        pill_html = "<span style='padding:6px 12px;border-radius:14px;background:#16a34a20;border:1px solid #16a34a40;color:#16a34a;font-weight:700;font-size:13px;'>BULL BIAS</span>"
    else:
        pill_html = "<span style='padding:6px 12px;border-radius:14px;background:#ef444420;border:1px solid #ef444440;color:#ef4444;font-weight:700;font-size:13px;'>BEAR BIAS</span>"

    # Persist in session + cache for the day
    st.session_state["tm_live_chips_html"] = chips_html
    st.session_state["tm_live_pill_html"] = pill_html

    meta = {
        "chosen_pct": chosen_pct,
        "indecisive": bool(indecisive),
        "dominant_is_bull": bool(dominant_is_bull),
        "pct_eff": pct_eff,
        "q_score": q_score,
        "aplus": bool(aplus),
        "ts": now_ist().strftime("%H:%M:%S"),
    }
    _save_tm_live_cache(day_key, chips_html, pill_html, tags, meta)

    # Donuts
    st.markdown("#### Today’s Read — Donuts")
    k1, k2, k3, k4 = st.columns(4)

    with k1:
        dir_col = "#1E90FF" if dominant_is_bull else _current_theme()["red"]
        st.plotly_chart(
            donut(
                chosen_pct,
                f"{int(round(chosen_pct))}%",
                "Indecisive" if indecisive else ("Bull leg" if dominant_is_bull else "Bear leg"),
                "#f59e0b" if indecisive else dir_col,
            ),
            use_container_width=True,
            key="live_dir",
        )
    with k2:
        from app.ui import traffic_color
        st.plotly_chart(
            donut(
                pct_eff,
                f"{int(round(pct_eff))}%",
                "Sample Depth (eff-N)",
                traffic_color(pct_eff, green_at=100.0, amber_at=66.0),
            ),
            use_container_width=True,
            key="live_eff",
        )
    with k3:
        from app.ui import traffic_color
        st.plotly_chart(
            donut(
                q_score,
                f"{int(round(q_score))}%",
                "Model Quality",
                traffic_color(q_score, green_at=75.0, amber_at=60.0),
            ),
            use_container_width=True,
            key="live_qual",
        )
    with k4:
        st.plotly_chart(
            donut(
                100 if aplus else 0,
                "A+" if aplus else "—",
                "A+ Threshold",
                "#8b5cf6" if aplus else "#dbe5f3",
                show_percent=False,
            ),
            use_container_width=True,
            key="live_aplus",
        )


# -----------------
# Cached-only render
# -----------------
def _render_from_cached(cached: dict) -> None:
    """Render from cached payload (signature + donuts)."""
    st.markdown("#### Today’s Tag Signature (cached)")
    chips_html_cached = cached.get("chips_html", "") or _render_signature_chips_inline(
        _prune_tags(cached.get("tags", {}) or {})
    )
    st.markdown(chips_html_cached, unsafe_allow_html=True)

    meta = cached.get("meta", {}) or {}
    chosen_pct = float(meta.get("chosen_pct", 50.0))
    indecisive = bool(meta.get("indecisive", False))
    dominant_is_bull = bool(meta.get("dominant_is_bull", True))
    pct_eff = float(meta.get("pct_eff", 0.0))
    q_score = float(meta.get("q_score", 0.0))
    aplus = bool(meta.get("aplus", False))

    st.session_state["tm_live_chips_html"] = chips_html_cached
    st.session_state["tm_live_pill_html"] = cached.get("pill_html", "")

    # Donuts
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        dir_col = "#1E90FF" if dominant_is_bull else _current_theme()["red"]
        st.plotly_chart(
            donut(
                chosen_pct,
                f"{int(round(chosen_pct))}%",
                ("Indecisive" if indecisive else ("Bull leg" if dominant_is_bull else "Bear leg")),
                "#f59e0b" if indecisive else dir_col,
            ),
            use_container_width=True,
            key="terminal_donut_dir_tm",
        )
    with k2:
        from app.ui import traffic_color
        st.plotly_chart(
            donut(pct_eff, f"{int(round(pct_eff))}%", "Sample Depth (eff-N)",
                  traffic_color(pct_eff, green_at=100.0, amber_at=66.0)),
            use_container_width=True,
        )
    with k3:
        from app.ui import traffic_color
        st.plotly_chart(
            donut(q_score, f"{int(round(q_score))}%", "Model Quality",
                  traffic_color(q_score, green_at=75.0, amber_at=60.0)),
            use_container_width=True,
        )
    with k4:
        st.plotly_chart(
            donut(100 if aplus else 0, "A+" if aplus else "—", "A+ Threshold",
                  "#8b5cf6" if aplus else "#dbe5f3", show_percent=False),
            use_container_width=True,
        )


# -----------------------------
# Minimal live state utilities
# -----------------------------
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
    if df is None or df.empty:
        return None, None
    df = df.sort_values("datetime")
    row = df.iloc[-1]
    ts = pd.to_datetime(row["datetime"])
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    ohlc = dict(open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]))
    return ts, ohlc


def _prev_day_hilo(df: pd.DataFrame):
    if df is None or df.empty:
        return None, None
    df = df.copy()
    if "date" in df.columns:
        df["date_ts"] = pd.to_datetime(df["date"]).dt.normalize()
    else:
        df["date_ts"] = pd.to_datetime(df["datetime"]).dt.normalize()
    last_day = df["date_ts"].max()
    prev_day = df.loc[df["date_ts"] < last_day, "date_ts"].max() if last_day is not None else None
    if prev_day is None:
        return None, None
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
            "datetime": "DateTime", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"
        }).copy()
        # normalize tz-naive for classifiers
        dt0 = pd.to_datetime(df_day2["DateTime"].iloc[0])
        if getattr(dt0, "tzinfo", None) is not None:
            df_day2["DateTime"] = pd.to_datetime(df_day2["DateTime"]).dt.tz_convert(IST).dt.tz_localize(None)
        else:
            df_day2["DateTime"] = pd.to_datetime(df_day2["DateTime"])

        prev = prev_trading_day_ohlc(df_all.rename(columns={
            "datetime": "DateTime", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"
        }), pd.to_datetime(df_day2["DateTime"].iloc[0]).normalize())
        prev_ctx = "TR"
        if prev and all(k in prev for k in ("open", "high", "low", "close")):
            prev_ctx = compute_prevdaycontext_robust(prev["open"], prev["high"], prev["low"], prev["close"])
        open_loc = compute_openlocation_from_df(df_day2, prev)
        opening_trend = compute_openingtrend_robust(df_day2)
        return {"PrevDayContext": prev_ctx, "OpenLocation": open_loc, "OpeningTrend": opening_trend}
    except Exception:
        return {}


def poll_once(inst_key: str, symbol_label: str) -> LiveState:
    g = _load_5m(inst_key)
    if g.empty:
        return LiveState(symbol=symbol_label, tags={}, signal={}, meta={"error": "no_5m"})
    ts, ohlc = _latest_bar(g)
    # choose day by max normalized date
    g = g.copy()
    if "date" in g.columns:
        g["date_ts"] = pd.to_datetime(g["date"]).dt.normalize()
    else:
        g["date_ts"] = pd.to_datetime(g["datetime"]).dt.normalize()
    day = g[g["date_ts"].eq(g["date_ts"].max())].copy()
    tags = compute_minimal_tags_for_day(day, g) if not day.empty else {}
    hi, lo = _prev_day_hilo(g)
    return LiveState(
        symbol=symbol_label, last_bar_ts=ts, last_ohlc=ohlc,
        prev_day_hi=hi, prev_day_lo=lo, tags=tags, signal={}, meta={}
    )
