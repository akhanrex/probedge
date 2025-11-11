# app/views/terminal.py
import os, json, hashlib
from datetime import time as dtime
from typing import Dict, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# -------------------------
# small utils
# -------------------------
def _file_mtime_safe(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return 0.0

def _hash_for_filters(inst_key: str, sd_dt, ed_dt, sel: dict) -> str:
    js = json.dumps({
        "inst": inst_key,
        "sd": str(pd.to_datetime(sd_dt).date()),
        "ed": str(pd.to_datetime(ed_dt).date()),
        "sel": sel,
    }, sort_keys=True)
    return hashlib.md5(js.encode("utf-8")).hexdigest()

# ---- SAFE import of intraday helpers (do not crash UI) ----
try:
    import app.intraday_utils as iu
except Exception:
    iu = None

def _iu_has(name: str) -> bool:
    return (iu is not None) and hasattr(iu, name)

# Bind functions with safe fallbacks
def _load_intraday_all(inst_key: str, **kw):
    if _iu_has("load_intraday_all"):
        return iu.load_intraday_all(inst_key, **kw)
    import pandas as _pd
    return _pd.DataFrame()

def _full_path_for(inst_key: str) -> str:
    if _iu_has("_full_path_for"):
        return iu._full_path_for(inst_key)
    return ""

def _sync_master_full_from_5m(inst_key: str, master_path: str):
    if _iu_has("sync_master_full_from_5m"):
        return iu.sync_master_full_from_5m(inst_key, master_path)
    return {"rows_added": 0, "rows_updated": 0, "path": master_path}

def _ensure_5m_and_master_up_to_date(inst_key: str, master_path: str, kite=None):
    if _iu_has("ensure_5m_and_master_up_to_date"):
        return iu.ensure_5m_and_master_up_to_date(inst_key, master_path, kite=kite)
    # Fallback: just rebuild master from existing 5m
    r = _sync_master_full_from_5m(inst_key, master_path)
    return {"dates_5m_added": 0, "bars_5m_appended": 0, "master_rows_added": (r.get("rows_added", 0) + r.get("rows_updated", 0))}

# Optional helpers for thumbnails (None if missing)
latest_dates_from_matches = getattr(iu, "latest_dates_from_matches", None)
slice_intraday_by_dates = getattr(iu, "slice_intraday_by_dates", None)
try_fetch_kite_5m_for_dates = getattr(iu, "try_fetch_kite_5m_for_dates", None)

from probedge.core.rules import DEFAULT_TAG_COLS, _canon_tag_value
from app.views.strength import render_strength_profiler
from probedge.core.stats import (
    probedge_adv_from_results,
    _eff_weights_by_recency_from_ref,
    refined_quality_score_advanced,
    _stable_ref_date,
)
from probedge.ui_adapters.components.theme import (
    cols as ui_cols,
    thumb_height as ui_thumb_h,
    big_height as ui_big_h,
)

from app.config import HALF_LIFE_DAYS, now_ist, SHOW_FINAL_SIGNAL
from app.instruments import InstrumentConfig, get_tm_thresholds
from app.ui import _current_theme, donut, _render_signature_chips_inline

# Optional import: robust tag calculator used by the live engine
try:
    from live_app.engine.signal import compute_live_tags_and_direction as _compute_tags_live
except Exception:
    _compute_tags_live = None

SHOW_FATIGUE = False
LIVE_CACHE_PATH = "/mnt/data/tm_live_cache.json"

# -------------------------
# live one-liner cache (TM)
# -------------------------
def _load_tm_live_cache(day_key: str) -> dict:
    try:
        if os.path.exists(LIVE_CACHE_PATH):
            with open(LIVE_CACHE_PATH, "r", encoding="utf-8") as f:
                return (json.load(f) or {}).get(day_key, {})
    except Exception:
        pass
    return {}

def _maybe_seed_live_signature_from_cache():
    if st.session_state.get("tm_live_chips_html") and st.session_state.get("tm_live_pill_html"):
        return
    day_key = now_ist().date().isoformat()
    cached = _load_tm_live_cache(day_key)
    if not cached:
        return
    chips = None
    pill = cached.get("pill_html")
    tags = cached.get("tags", {})
    tags = {k: tags.get(k) for k in ["PrevDayContext","OpenLocation","OpeningTrend"] if tags.get(k)}
    chips = _render_signature_chips_inline(tags)
    if chips and "tm_live_chips_html" not in st.session_state:
        st.session_state["tm_live_chips_html"] = chips
    if pill and "tm_live_pill_html" not in st.session_state:
        st.session_state["tm_live_pill_html"] = pill

# -------------------------
# fatigue cache
# -------------------------
@st.cache_data(show_spinner=False)
def compute_fatigue_timeseries(
    df_view: pd.DataFrame,
    *,
    half_life_days: float = HALF_LIFE_DAYS,
    lookback_days: int = 120,
    min_points: int = 20,
    step: int = 6,
) -> pd.DataFrame:
    if df_view is None or df_view.empty or "Date" not in df_view.columns:
        return pd.DataFrame()
    g = df_view.dropna(subset=["Date"]).sort_values("Date").copy()
    g["Date"] = pd.to_datetime(g["Date"], errors="coerce")
    g = g[g["Date"].notna()]
    if g.empty:
        return pd.DataFrame()
    uniq_dates = g["Date"].drop_duplicates().sort_values()
    rows = []
    for i, ref in enumerate(uniq_dates):
        if (i % max(1, step)) != 0:
            continue
        overall = probedge_adv_from_results(g[g["Date"] <= ref], ref_date=ref, half_life_days=half_life_days)
        recent_cut = ref.normalize() - pd.Timedelta(days=lookback_days)
        recent = g[g["Date"] >= recent_cut]
        if recent.empty:
            continue
        recent_adv = probedge_adv_from_results(recent, ref_date=ref, half_life_days=half_life_days)
        fat_bull = 100.0 * (recent_adv["pB"] - overall["pB"])
        fat_bear = 100.0 * (recent_adv["pR"] - overall["pR"])
        rows.append({"Date": ref.normalize(), "fat_bull_pp": round(fat_bull, 2), "fat_bear_pp": round(fat_bear, 2)})
    return pd.DataFrame(rows)

# -------------------------
# chart bits
# -------------------------
def _mini_candle(fig, df_day: pd.DataFrame, *, height=220, prev_tail=None, prev_hi=None, prev_lo=None, day_start_ts=None):
    TZ = _current_theme()
    def _norm(df):
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["datetime","open","high","low","close","volume"])
        g = df.copy()
        cols = {c.lower(): c for c in g.columns}
        def col(name): return cols.get(name, name)
        for want in ("datetime","open","high","low","close","volume"):
            if want not in g.columns:
                src = col(want)
                if src in g.columns:
                    g.rename(columns={src: want}, inplace=True)
        g["datetime"] = pd.to_datetime(g["datetime"], errors="coerce")
        g = g.dropna(subset=["datetime","open","high","low","close"])
        return g.sort_values("datetime")

    df_day = _norm(df_day)
    if df_day.empty:
        fig.update_layout(
            height=height, margin=dict(l=8, r=8, t=8, b=8),
            paper_bgcolor=TZ["card"], plot_bgcolor=TZ["card"],
            xaxis=dict(title=None, showgrid=False),
            yaxis=dict(title=None, showgrid=True, gridcolor=TZ["grid"]),
        )
        return fig

    fig.add_trace(go.Candlestick(
        x=df_day["datetime"],
        open=df_day["open"], high=df_day["high"], low=df_day["low"], close=df_day["close"],
        name="", increasing_line_color="#10b981", decreasing_line_color="#ef4444",
        increasing_fillcolor="#10b981", decreasing_fillcolor="#ef4444",
        whiskerwidth=0.4, showlegend=False,
    ))

    if day_start_ts is None:
        day_start_ts = pd.to_datetime(df_day["datetime"].iloc[0])

    shapes = [dict(type="line", xref="x", x0=day_start_ts, x1=day_start_ts, yref="paper", y0=0, y1=1, line=dict(color="#9ca3af", width=1))]
    START_T = dtime(9, 15); END_T = dtime(15, 30)
    rangebreaks = [dict(bounds=["sat","mon"]), dict(bounds=[END_T.strftime("%H:%M"), START_T.strftime("%H:%M")])]

    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor=TZ["card"], plot_bgcolor=TZ["card"],
        shapes=shapes, dragmode="pan", hovermode="x unified",
        xaxis=dict(title=None, showgrid=False, rangebreaks=rangebreaks, tickfont=dict(size=10), gridcolor=TZ["grid"]),
        yaxis=dict(title=None, autorange=True, fixedrange=False, tickfont=dict(size=10), gridcolor=TZ["grid"]),
    )
    return fig

# -------------------------
# cached terminal bundle (fast “Apply filters”)
# -------------------------
@st.cache_data(show_spinner=False)
def _compute_terminal_bundle_cached(
    inst_key: str,
    master_mtime: float,
    df_all: pd.DataFrame,
    sd_dt: pd.Timestamp,
    ed_dt: pd.Timestamp,
    sel: dict,
    ref_date,
    half_life_days: float,
):
    gfast = df_all.copy()
    mask = (gfast["Date"] >= sd_dt) & (gfast["Date"] <= ed_dt)
    df_view = gfast.loc[mask].copy()
    for k, v in sel.items():
        if v != "All" and f"{k}_C" in df_view.columns:
            df_view = df_view.loc[df_view[f"{k}_C"] == v]

    df_effective = df_view.rename(columns=lambda c: str(c).strip())
    if "Result" not in df_effective.columns:
        for c in list(df_effective.columns):
            if str(c).strip().lower() == "result":
                df_effective.rename(columns={c: "Result"}, inplace=True)
                break

    adv = probedge_adv_from_results(df_effective, ref_date=ref_date, half_life_days=half_life_days)
    base_cols = {"PrevDayContext_C","OpenLocation_C","OpeningTrend_C","Result"}
    comp_cols = [c for c in base_cols if c in df_effective.columns]
    completeness = float(df_effective[comp_cols].replace("", np.nan).notna().mean().mean()) if len(df_effective) and comp_cols else 0.0

    if len(df_view):
        w = _eff_weights_by_recency_from_ref(df_view["Date"], half_life_days, ref_date)
        adv_n_eff = float(w.sum())
    else:
        adv_n_eff = 0.0

    bundle = {
        "df_view": df_view,
        "bull_pct": float(adv.get("bull_pct", 0.0)),
        "bear_pct": float(adv.get("bear_pct", 0.0)),
        "pB": float(adv.get("pB", 0.5)),
        "pR": float(adv.get("pR", 0.5)),
        "n_eff": adv_n_eff,
        "n_matches": int(len(df_view)),
        "completeness": float(completeness),
    }
    return bundle

# -------------------------
# direction / confidence (unified across TM, LT, SBIN)
# -------------------------
def _symbol_from_inst_key(inst_key: str) -> str:
    m = {"tm":"TATAMOTORS","lt":"LT","sbin":"SBIN","ae":"ADANIENT"}
    return m.get(inst_key, inst_key).upper()

def _intraday_csv_candidates_for_symbol(symbol_no_ns: str):
    s = symbol_no_ns.upper()
    return [
        f"data/intraday/{s}_5minute.csv",
        f"data/intraday/{s}/{s}_5minute.csv",
        f"data/intraday/{s}.csv",
    ]

@st.cache_data(show_spinner=False)
def _load_today_5m_quick(symbol_no_ns: str) -> pd.DataFrame:
    today = pd.Timestamp.now(tz="Asia/Kolkata").date()
    for p in _intraday_csv_candidates_for_symbol(symbol_no_ns):
        try:
            if not os.path.exists(p):
                continue
            df = pd.read_csv(p)
            cols = {c.lower(): c for c in df.columns}
            def c_(x): return cols.get(x, x)
            df.rename(columns={
                c_("datetime"):"datetime",
                c_("date"):"date",
                c_("time"):"time",
                c_("open"):"open",
                c_("high"):"high",
                c_("low"):"low",
                c_("close"):"close",
                c_("volume"):"volume",
            }, inplace=True)
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                df = df.dropna(subset=["datetime"])
                df = df[df["datetime"].dt.date == today]
            elif {"date","time"}.issubset(df.columns):
                df["datetime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
                df = df.dropna(subset=["datetime"])
                df = df[df["datetime"].dt.date == today]
            else:
                continue
            need = {"datetime","open","high","low","close"}
            if not need.issubset(df.columns):
                continue
            g = df.sort_values("datetime").copy()
            g.rename(columns={
                "datetime":"DateTime","open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"
            }, inplace=True)
            return g
        except Exception:
            continue
    return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume"])

def _prevday_ohlc_from_master(df_all: pd.DataFrame) -> Dict[str, Optional[float]]:
    try:
        today = pd.Timestamp.now(tz="Asia/Kolkata").normalize()
        g = df_all.copy()
        g["Date"] = pd.to_datetime(g["Date"], errors="coerce")
        g = g.dropna(subset=["Date"]).sort_values("Date")
        prev = g[g["Date"] < today]
        if prev.empty:
            prev = g.tail(1)
        row = prev.iloc[-1]
        # Be tolerant about column names
        O = float(row.get("Open")) if "Open" in row else None
        H = float(row.get("High")) if "High" in row else None
        L = float(row.get("Low"))  if "Low" in row else None
        C = float(row.get("Close")) if "Close" in row else None
        return {"O": O, "H": H, "L": L, "C": C}
    except Exception:
        return {"O": None, "H": None, "L": None, "C": None}

def _compute_tags_for_today(df_day_5m: pd.DataFrame, df_all_master: pd.DataFrame) -> Dict[str, str]:
    """
    Reuse the same robust classifier used by the live engine if available.
    """
    if _compute_tags_live is None or df_day_5m is None or df_day_5m.empty:
        return {}
    try:
        prev = _prevday_ohlc_from_master(df_all_master)
        tags_ext = _compute_tags_live(
            df_day=df_day_5m,  # already in DateTime/Open/High/Low/Close
            prev_ohlc=prev,
        )
        # map to the names the Strength logic expects in 'tags'
        return {
            "PrevDayContext": tags_ext.get("PDC"),
            "OpenLocation": tags_ext.get("OL"),
            "OpeningTrend": tags_ext.get("OT"),
        }
    except Exception:
        return {}

def _render_direction_confidence_panel(inst_key: str, df_all_master: pd.DataFrame):
    """
    EXACT same source of truth as TATAMOTORS:
    strength.compute_direction_confidence(df_day, tags)
    where df_day is today's 5m and tags are computed via the robust classifier (when available).
    """
    # strength
    try:
        try:
            from app.strength import compute_direction_confidence as _dirconf
        except Exception:
            from probedge.core.strength import compute_direction_confidence as _dirconf
    except Exception:
        st.caption("Direction/Confidence not available in this build.")
        return

    sym = _symbol_from_inst_key(inst_key)
    df_day = _load_today_5m_quick(sym)
    if df_day is None or df_day.empty:
        st.caption("Today’s intraday data not found for this instrument.")
        return

    # tags (robust path → same as live)
    tags = _compute_tags_for_today(df_day, df_all_master) if df_all_master is not None else {}

    try:
        side_s, conf_v = _dirconf(df_day, tags)
        side = ("BULL" if str(side_s).upper()=="LONG"
                else "BEAR" if str(side_s).upper()=="SHORT" else "FLAT")
        try:
            conf = float(str(conf_v).rstrip("%"))
        except Exception:
            conf = float(conf_v) if conf_v is not None else 0.0

        tone = "#10b981" if side=="BULL" else "#ef4444" if side=="BEAR" else "#94a3b8"
        st.markdown(
            f"<div style='margin-top:6px;padding:10px 12px;border:1px solid {_current_theme()['border']};"
            f"border-radius:10px;background:{_current_theme()['card']};display:flex;gap:18px;align-items:center;flex-wrap:wrap'>"
            f"<div style='font-weight:700'>Direction:</div>"
            f"<div style='font-weight:800;color:{tone}'>{side}</div>"
            f"<div style='font-weight:700'>Confidence:</div>"
            f"<div style='font-weight:800'>{int(round(conf))}%</div>"
            f"</div>",
            unsafe_allow_html=True
        )
    except Exception:
        st.caption("Direction/Confidence not available.")

# -------------------------
# TM live one-liner in header
# -------------------------
def render_terminal_live_one_liner():
    if "tm_live_chips_html" not in st.session_state:
        _maybe_seed_live_signature_from_cache()
    chips_html = st.session_state.get("tm_live_chips_html")
    if not chips_html:
        _maybe_seed_live_signature_from_cache()
        chips_html = st.session_state.get("tm_live_chips_html")
    pill_html = (
        st.session_state.get("tm_live_pill_html")
        or "<span style='padding:6px 12px;border-radius:14px;background:#94a3b820;border:1px solid #94a3b840;color:#94a3b8;font-weight:800;font-size:13px;'>LIVE PENDING</span>"
    )
    if chips_html:
        st.markdown(
            f"""
            <div style="padding:10px;border:1px solid #e5e7eb;border-radius:10px;">
              <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
                <div style="display:flex;gap:8px;flex-wrap:wrap">{chips_html}</div>
                <div>{pill_html}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div style='padding:10px;border:1px dashed #e5e7eb;border-radius:10px;color:#94a3b8'>Waiting for Live Tracker…</div>",
            unsafe_allow_html=True,
        )

# -------------------------
# MAIN
# -------------------------
def render_terminal(df_all: pd.DataFrame, inst: InstrumentConfig, *, kite=None):
    from app.ui import ui_title_with_metrics

    symbol_map = {"tm":"NSE:TATAMOTORS","lt":"NSE:LT","sbin":"NSE:SBIN","ae":"NSE:ADANIENT"}
    label_map  = {"tm":"TATAMOTORS","lt":"LT","sbin":"SBIN","ae":"ADANIENT"}
    icon_map   = {"tm":None,"lt":None,"sbin":None,"ae":None}

    ui_title_with_metrics(
        h=f"{inst.label} — Structure Engine",
        sub="Historical edge model for today’s tag signature",
        symbol=symbol_map.get(inst.key,""),
        label=label_map.get(inst.key,inst.label.upper()),
        icon_url=icon_map.get(inst.key),
    )

    if inst.key == "tm":
        render_terminal_live_one_liner()

    if df_all is None or df_all.empty:
        st.warning(f"{inst.label} master data not found or empty. Place the master CSV.")
        return

    tags_list = [t for t in DEFAULT_TAG_COLS if t not in inst.hide_tags]
    min_dt, max_dt = df_all["Date"].min().date(), df_all["Date"].max().date()
    ref_date = _stable_ref_date(df_all)

    with st.form(f"{inst.key}_filters", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            sd = st.date_input("Start Date", min_value=min_dt, max_value=max_dt, value=min_dt, key=f"{inst.key}_sd")
        with c2:
            ed = st.date_input("End Date", min_value=min_dt, max_value=max_dt, value=max_dt, key=f"{inst.key}_ed")
        cols_tf = st.columns(5)
        sel = {}
        for i, c in enumerate(tags_list):
            pre = st.session_state.get(f"{inst.key}_pre", {"g": pd.DataFrame()})
            gfast = pre["g"]
            opts = (
                ["All"]
                + sorted(
                    gfast.get(f"{c}_C", pd.Series(dtype=str))
                    .dropna()
                    .astype(str).str.strip()
                    .unique().tolist()
                )
                if f"{c}_C" in gfast.columns else ["All"]
            )
            sel[c] = cols_tf[i].selectbox(c, opts, index=0, key=f"{inst.key}_tag_{c}")
        submitted = st.form_submit_button("Apply filters", type="primary", use_container_width=True)

    if pd.to_datetime(sd) > pd.to_datetime(ed):
        st.error("Start date must be ≤ End date")
        return

    init_flag_key = f"{inst.key}_init_done"
    if init_flag_key not in st.session_state:
        st.session_state[init_flag_key] = True

    if submitted or st.session_state[init_flag_key]:
        with st.spinner("Updating…"):
            st.session_state[init_flag_key] = False
            sd_dt = pd.to_datetime(sd); ed_dt = pd.to_datetime(ed)

            # source fast DF from session (your code already populates this)
            pre = st.session_state.get(
                f"{inst.key}_pre",
                {"g": pd.DataFrame(), "oh": pd.DataFrame(), "dates": pd.Series(dtype="datetime64[ns]")}
            )
            gfast = pre["g"]

            master_path = getattr(inst, "master_path", "") or _full_path_for(inst.key)
            master_mtime = _file_mtime_safe(master_path)

            bundle = _compute_terminal_bundle_cached(
                inst_key=inst.key,
                master_mtime=master_mtime,
                df_all=gfast,
                sd_dt=sd_dt,
                ed_dt=ed_dt,
                sel=sel,
                ref_date=ref_date,
                half_life_days=HALF_LIFE_DAYS,
            )
            st.session_state[f"{inst.key}_bundle"] = bundle

            chips_tags = {}
            for _lbl in ["PrevDayContext","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus"]:
                val = sel.get(_lbl, "All")
                chips_tags[_lbl] = val if val != "All" else "Unknown"
            st.session_state[f"{inst.key}_terminal_chips_html"] = _render_signature_chips_inline(chips_tags)
            if inst.key == "tm" and not st.session_state.get("tm_live_chips_html"):
                st.session_state["tm_preview_chips_html"] = st.session_state[f"{inst.key}_terminal_chips_html"]

    bundle = st.session_state.get(f"{inst.key}_bundle")
    if not bundle:
        st.info("Adjust filters, then click Apply.")
        return

    df_view = bundle["df_view"]
    bull_pct_final = float(bundle["bull_pct"])
    bear_pct_final = float(bundle["bear_pct"])
    adv_n_eff = float(bundle["n_eff"])
    n_matches = int(bundle["n_matches"])
    completeness = float(bundle["completeness"])

    edge_pp = abs(bull_pct_final - bear_pct_final)
    dominant_is_bull = bull_pct_final >= bear_pct_final
    chosen_pct = bull_pct_final if dominant_is_bull else bear_pct_final
    pB = float(bundle.get("pB", 0.5)); pR = float(bundle.get("pR", 0.5))
    adv_p_for_dir = pB if dominant_is_bull else pR
    indecisive = edge_pp < 2.0

    q_score = refined_quality_score_advanced(adv_n_eff, completeness, adv_p_for_dir)
    best_side_pct = max(bull_pct_final, bear_pct_final)
    aplus = ((adv_n_eff >= inst.aplus_min_eff) and (best_side_pct >= inst.aplus_rate) and (completeness > inst.aplus_comp))

    # TM-only profiler (prevents TM file/path warnings on LT/SBIN)
    if inst.key == "tm":
        render_strength_profiler(df_view, inst_key=inst.key)

    # Top row donuts
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        chosen_lab = "Bull leg" if dominant_is_bull else "Bear leg"
        if indecisive:
            st.plotly_chart(donut(chosen_pct, f"{int(round(chosen_pct))}%", "Indecisive", "#f59e0b"),
                            use_container_width=True, key=f"terminal_donut_dir_{inst.key}")
        else:
            dir_col = "#1E90FF" if dominant_is_bull else _current_theme()["red"]
            st.plotly_chart(donut(chosen_pct, f"{int(round(chosen_pct))}%", chosen_lab, dir_col),
                            use_container_width=True, key=f"terminal_donut_dir_{inst.key}")

    with k2:
        th = get_tm_thresholds()
        depth_norm = float(th.get("depth_norm", 25.0))
        pct_eff = 100.0 * min(float(adv_n_eff), depth_norm) / depth_norm if depth_norm > 0 else 0.0
        center_top = f"{adv_n_eff:.1f}/{depth_norm:g}"
        def _traffic_color(v): return "#16a34a" if v >= 100 else "#f59e0b" if v >= 66 else "#dc2626"
        st.plotly_chart(donut(pct_eff, center_top, "Sample Depth (eff-N)", _traffic_color(pct_eff)),
                        use_container_width=True, key=f"terminal_donut_depth_{inst.key}")

    with k3:
        def _traffic_q(v): return "#16a34a" if v >= 75 else "#f59e0b" if v >= 60 else "#dc2626"
        st.plotly_chart(donut(q_score, f"{int(round(q_score))}%", "Model Quality", _traffic_q(q_score)),
                        use_container_width=True, key=f"terminal_donut_quality_{inst.key}")

    with k4:
        st.plotly_chart(donut(100 if aplus else 0, "A+" if aplus else "—", "A+ Threshold",
                              "#8b5cf6" if aplus else "#dbe5f3", show_percent=False),
                        use_container_width=True, key=f"terminal_donut_aplus_{inst.key}")

    # <<< NEW: Direction/Confidence panel (all instruments) — same source as TM >>>
    _render_direction_confidence_panel(inst.key, df_all_master=df_all)

    # -------------------------
    # Matches
    # -------------------------
    st.markdown("#### Matches")
    st.write(f"**{n_matches}** matched days in the selected date range.")
    if n_matches:
        df_effective = df_view.copy().rename(columns=lambda c: str(c).strip())
        if "Result" not in df_effective.columns:
            for c in list(df_effective.columns):
                if str(c).strip().lower() == "result":
                    df_effective.rename(columns={c: "Result"}, inplace=True)
                    break
        has_result = "Result" in df_effective.columns
        cols_show = ["Date","PrevDayContext","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus"] + (["Result"] if has_result else [])
        table_df = df_effective.copy()
        table_df["Date"] = pd.to_datetime(table_df["Date"], errors="coerce")
        safe_tbl = table_df.dropna(subset=["Date"]).sort_values(by="Date", ascending=False).copy()
        safe_tbl["Date"] = safe_tbl["Date"].dt.date
        st.dataframe(safe_tbl[cols_show].head(500), use_container_width=True, height=420)
        if has_result:
            vc = (df_effective["Result"].fillna("Unknown").astype(str).str.strip().value_counts().reset_index(name="Count"))
            vc.columns = ["Result","Count"]
            fig_res = px.bar(vc, x="Result", y="Count", text="Count")
            fig_res.update_traces(textposition="outside")
            fig_res.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=320)
            st.plotly_chart(fig_res, use_container_width=True, key=f"res_bar_{inst.key}")
            st.caption("Result mode: Full Day (fixed)")
        else:
            st.info("This master has no ‘Result’ column. Summary chart hidden.")
    else:
        st.info("No matches for the current filters/date range.")

    # -------------------------
    # Auto-backfill status (SAFE WRAPPERS)
    # -------------------------
    try:
        stats = _ensure_5m_and_master_up_to_date(inst.key, inst.master_path, kite=kite)
        intr = _load_intraday_all(inst.key, force_reload=True)
        last_5m = pd.to_datetime(intr["datetime"]).max() if not intr.empty else None
        master_last = pd.to_datetime(df_all["Date"]).max() if not df_all.empty else None
        fpath = _full_path_for(inst.key)
        try:
            mtime = pd.to_datetime(os.path.getmtime(fpath), unit="s").tz_localize("UTC").tz_convert("Asia/Kolkata")
        except Exception:
            mtime = None
        st.caption(
            f"Auto-backfill → +{stats.get('dates_5m_added',0)} day(s) in 5m, "
            f"+{stats.get('bars_5m_appended',0)} bar(s) in 5m, "
            f"+{stats.get('master_rows_added',0)} row(s) in master • "
            f"5m last bar: {last_5m} • Master last date: {getattr(master_last, 'date', lambda: None)()} • "
            f"5m mtime: {mtime} • Kite: {'ON' if kite else 'OFF'}"
        )
    except Exception as e:
        st.caption(f"Auto-backfill: skipped ({e})")
        try:
            stats = _ensure_5m_and_master_up_to_date(inst.key, inst.master_path, kite=kite)
            intr = _load_intraday_all(inst.key, force_reload=True)
            last_5m = pd.to_datetime(intr["datetime"]).max() if not intr.empty else None
            master_last = pd.to_datetime(df_all["Date"]).max() if not df_all.empty else None
            fpath = _full_path_for(inst.key)
            try:
                mtime = pd.to_datetime(os.path.getmtime(fpath), unit="s").tz_localize("UTC").tz_convert("Asia/Kolkata") if fpath else None
            except Exception:
                mtime = None
            st.caption(
                f"Auto-backfill → +{stats.get('dates_5m_added',0)} day(s) in 5m, "
                f"+{stats.get('bars_5m_appended',0)} bar(s) in 5m, "
                f"+{stats.get('master_rows_added',0)} row(s) in master • "
                f"5m last bar: {last_5m} • Master last date: {getattr(master_last, 'date', lambda: None)()} • "
                f"5m mtime: {mtime} • Kite: {'ON' if kite else 'OFF'}"
            )
        except Exception as e2:
            st.caption(f"Auto-backfill: skipped ({e2})")

    # -------------------------
    # Thumbnails (on demand)
    # -------------------------
    show_key = f"{inst.key}_show_snaps"
    if st.button("Show thumbnails (on-demand)", key=show_key, use_container_width=True):
        st.session_state[f"{inst.key}_want_snaps"] = True

    if st.session_state.get(f"{inst.key}_want_snaps"):
        if latest_dates_from_matches is None or slice_intraday_by_dates is None:
            st.info("Intraday module not available in this build.")
        else:
            want_dates = latest_dates_from_matches(df_view, max_days=30)
            if not want_dates:
                st.info("No matched dates to preview.")
            else:
                with st.spinner("Loading intraday snapshots…"):
                    by_date = slice_intraday_by_dates(inst.key, want_dates)
                    missing = [d for d in want_dates if d not in by_date]
                    fetched = {}
                    if missing and try_fetch_kite_5m_for_dates and (kite is not None):
                        fetched = try_fetch_kite_5m_for_dates(inst.key, missing, kite)
                        by_date.update(fetched)

                cols = ui_cols(3, 2, 1)
                sel_key = f"{inst.key}_snap_sel"

                for i, dt in enumerate(want_dates):
                    c = cols[i % 3]
                    with c:
                        day_df = by_date.get(dt)
                        title_dt = pd.to_datetime(dt).date().isoformat()

                        if day_df is None or day_df.empty:
                            st.markdown(
                                f"<div style='height:190px;border:1px dashed {_current_theme()['border']};"
                                f"border-radius:8px;display:flex;align-items:center;justify-content:center;color:{_current_theme()['muted']}'>"
                                f"No data<br/>{title_dt}</div>",
                                unsafe_allow_html=True,
                            )
                        else:
                            all_keys_sorted = sorted(by_date.keys())
                            try:
                                _idx = all_keys_sorted.index(dt)
                                prev_dt = all_keys_sorted[_idx - 1] if _idx > 0 else None
                            except ValueError:
                                prev_dt = None
                            prev_hi = prev_lo = None
                            def _norm_cols(df0: pd.DataFrame) -> pd.DataFrame:
                                if df0 is None or df0.empty:
                                    return pd.DataFrame()
                                df = df0.copy()
                                rename_map = {}
                                if "Open" in df.columns: rename_map["Open"]="open"
                                if "High" in df.columns: rename_map["High"]="high"
                                if "Low" in df.columns:  rename_map["Low"]="low"
                                if "Close" in df.columns:rename_map["Close"]="close"
                                if "Volume" in df.columns:rename_map["Volume"]="volume"
                                if "datetime" not in df.columns:
                                    if "DateTime" in df.columns: rename_map["DateTime"]="datetime"
                                    elif "date" in df.columns:   rename_map["date"]="datetime"
                                df.rename(columns=rename_map, inplace=True)
                                if df.columns.duplicated().any(): df = df.loc[:, ~df.columns.duplicated()]
                                if "datetime" not in df.columns: return pd.DataFrame()
                                if "volume" not in df.columns:
                                    import numpy as _np; df["volume"] = _np.nan
                                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                                return df.dropna(subset=["datetime","open","high","low","close"]).sort_values("datetime")
                            if prev_dt is not None and prev_dt in by_date and by_date[prev_dt] is not None and not by_date[prev_dt].empty:
                                full_prev = _norm_cols(by_date[prev_dt])
                                if not full_prev.empty:
                                    prev_hi = float(full_prev["high"].max()); prev_lo = float(full_prev["low"].min())
                            _dd = _norm_cols(day_df)
                            day_start_ts = pd.to_datetime(_dd["datetime"].iloc[0]) if not _dd.empty else None
                            fig = go.Figure()
                            _mini_candle(fig, day_df, height=ui_thumb_h(), prev_tail=None, prev_hi=prev_hi, prev_lo=prev_lo, day_start_ts=day_start_ts)
                            _res = df_view[pd.to_datetime(df_view["Date"]).dt.normalize().eq(pd.to_datetime(dt).normalize())]["Result"]
                            sub = str(_res.iloc[0]) if len(_res) else ""
                            st.plotly_chart(fig, use_container_width=True, key=f"{inst.key}_thumb_{title_dt}")
                            st.caption(f"{title_dt}  {('— ' + sub) if sub else ''}")
                            if st.button(f"View {title_dt}", key=f"{inst.key}_open_{title_dt}"):
                                st.session_state[sel_key] = title_dt

                chosen = st.session_state.get(sel_key)
                if chosen:
                    dt = pd.to_datetime(chosen).normalize()
                    day_df = by_date.get(dt)
                    all_keys_sorted = sorted(by_date.keys())
                    try:
                        _idx = all_keys_sorted.index(dt)
                        prev_dt = all_keys_sorted[_idx - 1] if _idx > 0 else None
                    except ValueError:
                        prev_dt = None
                    prev_hi = prev_lo = None
                    day_start_ts = None
                    def _norm_cols(df0: pd.DataFrame) -> pd.DataFrame:
                        if df0 is None or df0.empty: return pd.DataFrame()
                        df = df0.copy()
                        rename_map = {}
                        if "Open" in df.columns: rename_map["Open"]="open"
                        if "High" in df.columns: rename_map["High"]="high"
                        if "Low" in df.columns:  rename_map["Low"]="low"
                        if "Close" in df.columns:rename_map["Close"]="close"
                        if "Volume" in df.columns:rename_map["Volume"]="volume"
                        if "datetime" not in df.columns:
                            if "DateTime" in df.columns: rename_map["DateTime"]="datetime"
                            elif "date" in df.columns:   rename_map["date"]="datetime"
                        df.rename(columns=rename_map, inplace=True)
                        if df.columns.duplicated().any(): df = df.loc[:, ~df.columns.duplicated()]
                        if "datetime" not in df.columns: return pd.DataFrame()
                        if "volume" not in df.columns:
                            import numpy as _np; df["volume"] = _np.nan
                        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                        return df.dropna(subset=["datetime","open","high","low","close"]).sort_values("datetime")
                    if prev_dt is not None and prev_dt in by_date and by_date[prev_dt] is not None and not by_date[prev_dt].empty:
                        full_prev = _norm_cols(by_date[prev_dt])
                        if not full_prev.empty:
                            prev_hi = float(full_prev["high"].max()); prev_lo = float(full_prev["low"].min())
                    _dd = _norm_cols(day_df)
                    if not _dd.empty:
                        day_start_ts = pd.to_datetime(_dd["datetime"].iloc[0])
                    big = go.Figure()
                    _mini_candle(big, day_df, height=ui_big_h(), prev_tail=None, prev_hi=prev_hi, prev_lo=prev_lo, day_start_ts=day_start_ts)
                    st.plotly_chart(big, use_container_width=True, key=f"{inst.key}_expanded_{chosen}")
                    if st.button("Close expanded view", key=f"{inst.key}_close_expanded"):
                        st.session_state.pop(sel_key, None)
