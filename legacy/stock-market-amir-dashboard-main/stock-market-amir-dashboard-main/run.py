# run.py — ProbEdge Terminal (Structure Engine + Live Tracker + Journal)
# Python 3.9+, Streamlit >=1.50,<2, Plotly 5.x
from probedge.perf import (
    invalidate_all, memo_data, memo_resource,
    session_master, live_refresh_controls, perf
)
from datetime import date as _date
import sys

import os
import json
import time as _time
from pathlib import Path
from typing import Dict
import pandas as pd
import streamlit as st
import importlib, sys

def _safe_import_render_terminal():
    try:
        importlib.invalidate_caches()
        if "app.views.terminal" in sys.modules:
            importlib.reload(sys.modules["app.views.terminal"])
            mod = sys.modules["app.views.terminal"]
        else:
            mod = importlib.import_module("app.views.terminal")
        return getattr(mod, "render_terminal")
    except Exception as e:
        import traceback, streamlit as st
        st.error("Import error while loading app.views.terminal")
        st.code("".join(traceback.format_exception(e)))
        def _stub(*_a, **_k):
            st.info("Terminal view unavailable due to import error.")
        return _stub


from probedge.updater.daily import (
    append_today_if_connected_and_closed,
    update_master_if_needed as update_master_daily,
)
# Optional deps inside try/except to avoid hard failures locally
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

try:
    import yaml
except Exception:
    yaml = None

try:
    from tools.update_tm5min_and_master import run_daily_update
except Exception:
    run_daily_update = None

# -------------------------------
# Repo root detection
# -------------------------------
def _detect_repo_root() -> Path:
    here = Path(__file__).resolve()
    candidates = list(here.parents)
    markers = {
        ".git",
        "requirements.txt",
        "data",
        "weekly_updater.py",
        "github_utils.py",
        "journal_utils.py",
    }
    for p in candidates:
        try:
            names = {x.name for x in p.iterdir()}
            if markers & names:
                return p
        except Exception:
            pass
    return here.parents[1] if here.parent.name.lower() == "app" else here.parent


REPO_ROOT = _detect_repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
# Live Terminal (optional)
try:
    from live_app.ui.page import render as render_live_terminal
except Exception as e:
    def render_live_terminal(*_a, **_k):
        import streamlit as st, traceback
        st.info("Live Terminal module not available in this build.")
        st.caption("Import error:")
        st.code("".join(traceback.format_exception(e)))


try:
    from live_app.feeds.kite import KiteFeed
except Exception:
    KiteFeed = None
# -------------------------------
# Import app modules
# -------------------------------
from probedge.data.io import load_master, precompute_master

from app.config import (
    get_secret_first,
    APP_PASSWORD,
    LOGO_URL,
    TM_ICON_URL,
    AE_ICON_URL,
    HALF_LIFE_DAYS,
    YEARS_BACK,
    FORCE_REBUILD_ON_SPAN,
    API_KEY,
    API_SECRET,
    IST,
    now_ist,
)
from app.ui import show_logo
from app.services.github_sync import (
    _get_github_secrets,
    _push_via_github_api,
    _push_git_compat,
)
from app.views.journal import render_journal_view
from app.instruments import INSTRUMENTS
from app.views.auth import render_login, render_connect_kite

# optional weekly & github helpers
try:
    from probedge.updater.weekly import (
        compute_live_weekly_tags,
        update_master_if_needed,
    )
except Exception:
    compute_live_weekly_tags = None

    def update_master_if_needed(*_a, **_k):
        return 0


try:
    from probedge.updater.github import push_to_github
except Exception:
    push_to_github = None

# Kite
try:
    from kiteconnect import KiteConnect
except Exception:
    KiteConnect = None

kite = None
try:
    from kite_login import login_button, handle_callback
except Exception:

    def login_button(*_a, **_k):
        return None

    def handle_callback(*_a, **_k):
        return None


# optional master bootstrap
try:
    from probedge.ingest_kite import build_master_from_kite_daily
except Exception:
    try:
        from ingest_kite import build_master_from_kite_daily
    except Exception:
        build_master_from_kite_daily = None

# -------------------------------
# Streamlit base config & theme CSS
# -------------------------------
st.set_page_config(
    page_title="ProbEdge Terminal", layout="wide", initial_sidebar_state="collapsed"
)
try:
    from probedge.ui_adapters.components.theme import init_responsive_css

    init_responsive_css()
except Exception:
    pass


def _boot_toasts_enabled() -> bool:
    return not st.session_state.get("_boot_toasts_done", False)


def _flash(msg: str, icon: str = "ℹ️") -> None:
    if not _boot_toasts_enabled():
        return
    try:
        st.toast(msg, icon=icon)
    except Exception:
        ph = st.empty()
        ph.caption(msg)
        _time.sleep(1.5)
        ph.empty()


# -------------------------------
# Journal config (YAML) loader
# -------------------------------
def _load_cfg() -> Dict:
    cfg_path = REPO_ROOT / "config" / "journal_config.yaml"
    if yaml is None or not cfg_path.exists():
        return {}
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


PROBEDGE_CFG = _load_cfg()


# Ensure data dirs exist (quality-of-life)
def _ensure_data_dirs() -> None:
    for rel in ("data/latest", "data/masters"):
        p = (REPO_ROOT / rel).resolve()
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass


# -------------------------------
# Master precompute cache helper
# -------------------------------
def _refresh_precompute_and_fatigue(k: str, df: pd.DataFrame):
    pre = precompute_master(df)
    st.session_state[f"{k}_pre"] = pre
    st.session_state[f"{k}_fat_ts"] = None
    st.session_state[f"{k}_fat_ready"] = False


# --- Sidebar: Maintenance for multiple instruments ---
def render_sidebar_maintenance(kite=None):
    st.sidebar.markdown("### Maintenance: Updates (All)")

    # Safe intraday-utils import + wrappers (no crashes if missing)
    try:
        import app.intraday_utils as iu
    except Exception:
        iu = None

    def _has(name: str) -> bool:
        return (iu is not None) and hasattr(iu, name)

    def _sync_5m_to_master(inst_key: str, master_path: str) -> dict:
        """No-Kite path: rebuild/update master from existing 5m CSV."""
        if _has("sync_master_full_from_5m"):
            return iu.sync_master_full_from_5m(inst_key, master_path)
        return {"rows_added": 0, "rows_updated": 0, "path": master_path}

    def _ensure_all_up_to_date(inst_key: str, master_path: str, kite=None) -> dict:
        """Preferred path: extend 5m (Kite if present) then sync master."""
        if _has("ensure_5m_and_master_up_to_date"):
            return iu.ensure_5m_and_master_up_to_date(inst_key, master_path, kite=kite)
        r = _sync_5m_to_master(inst_key, master_path)
        return {
            "dates_5m_added": 0,
            "bars_5m_appended": 0,
            "master_rows_added": r.get("rows_added", 0) + r.get("rows_updated", 0),
        }

    # Discover available instruments from config
    from app.instruments import INSTRUMENTS
    avail = {k: v for k, v in INSTRUMENTS.items() if getattr(v, "master_path", None)}
    labels = {k: getattr(v, "label", k.upper()) for k, v in avail.items()}

    # sensible defaults: TM, LT, SBIN if present
    default_keys = [k for k in ["tm", "lt", "sbin"] if k in avail] or list(avail.keys())

    sel = st.sidebar.multiselect(
        "Select instruments",
        options=list(avail.keys()),
        default=default_keys,
        format_func=lambda k: labels.get(k, k.upper()),
    )

    c1, c2 = st.sidebar.columns(2)

    # Button 1 — no-Kite path: 5m → master
    if c1.button("5m → master (selected)", use_container_width=True):
        logs = []
        for k in sel:
            inst = avail[k]
            try:
                res = _sync_5m_to_master(k, inst.master_path)
                logs.append(f"**{labels[k]}**: +{res.get('rows_added',0)} added, "
                            f"+{res.get('rows_updated',0)} updated → {res.get('path','')}")
            except Exception as e:
                logs.append(f"**{labels[k]}**: failed — {e}")
        st.sidebar.success("Sync complete.")
        for line in logs:
            st.sidebar.caption(line)

    # Button 2 — Kite (if connected) + sync
    if c2.button("Kite fetch + sync (selected)", use_container_width=True):
        if kite is None:
            st.sidebar.warning("Kite is not connected; falling back to 5m → master only.")
        logs = []
        for k in sel:
            inst = avail[k]
            try:
                res = _ensure_all_up_to_date(k, inst.master_path, kite=kite)
                logs.append(
                    f"**{labels[k]}**: +{res.get('dates_5m_added',0)} day(s) 5m, "
                    f"+{res.get('bars_5m_appended',0)} bar(s), "
                    f"+{res.get('master_rows_added',0)} master row(s)"
                )
            except Exception as e:
                logs.append(f"**{labels[k]}**: failed — {e}")
        st.sidebar.success("Kite+Sync complete.")
        for line in logs:
            st.sidebar.caption(line)


# -------------------------------
# Main App Tabs
# -------------------------------

# Live Tracker view (safe import with fallback)
try:
    from app.views.live import render_live_tracker  # real implementation
except Exception:
    # Fallback stub so the Terminal tab doesn't crash if live.py is missing
    def render_live_tracker(*_a, **_k):
        import streamlit as st
        st.info("Live Tracker module not available. (app/views/live.py not found)")
        
def run_main_app():
    _ = kite  # touch global to quiet static analyzers; it’s set in safe_bootstrap()
    show_logo(centered=False)
    st.markdown("""
    <style>
    /* hide status + skeleton shimmer */
    [data-testid="stStatusWidget"], [data-testid="stSkeleton"] { display:none !important; }
    
    /* remove fade/animation that causes brief white flash */
    *[data-testid="stMarkdownContainer"],
    *[data-testid="stVerticalBlock"],
    *[data-testid="stHorizontalBlock"],
    *[data-testid="stColumn"],
    *[data-testid="stContainer"] {
      animation: none !important;
      transition: none !important;
    }
    
    /* prevent height jumps -> reserve space used by live header rows */
    .live-card   { min-height: 130px; }
    .live-chips  { min-height: 38px; }
    .live-box    { border:1px solid var(--pe-border,#e5e7eb); border-radius:12px; padding:10px; }
    .live-metric { font-size:22px; font-weight:800; line-height:1.15; }
    </style>
    """, unsafe_allow_html=True)


    master_dfs: Dict[str, pd.DataFrame] = {}
    for k, inst in INSTRUMENTS.items():
        # 1) optional bootstrap if missing (TM and AE)
        if (
            KiteConnect
            and (kite is not None)
            and (build_master_from_kite_daily is not None)
            and not os.path.exists(inst.master_path)
        ):
            sym_map = {"tm": "NSE:TATAMOTORS", "ae": "NSE:ADANIENT"}
            sym = sym_map.get(inst.key)
            if sym:
                Path(inst.master_path).parent.mkdir(parents=True, exist_ok=True)
                ok = build_master_from_kite_daily(kite, sym, inst.master_path, years_back=YEARS_BACK)
                if ok:
                    st.success(f"Created master for {inst.label} from Kite history → {inst.master_path}")
                else:
                    st.warning(f"Could not auto-create master for {inst.label}.")

        # 2) load master (safe when file missing)
        if not Path(inst.master_path).exists():
            st.info(f"{inst.label}: master not found at {inst.master_path}. Skipping read.")
            df = pd.DataFrame()
        else:
            df = session_master(inst.master_path)
        _refresh_precompute_and_fatigue(k, df)

        if df is None or df.empty:
            _flash(f"Loaded: {inst.label} → path={inst.master_path} · rows=0", icon="ℹ️")
        else:
            dmin = pd.to_datetime(df["Date"]).min().date()
            dmax = pd.to_datetime(df["Date"]).max().date()
            _flash(
                f"Loaded: {inst.label} → rows={len(df)} · {dmin} → {dmax}", icon="✅"
            )

        # 3) optional force rebuild if span short
        try:
            if df is not None and not df.empty:
                dmin = pd.to_datetime(df["Date"], errors="coerce").min()
                dmax = pd.to_datetime(df["Date"], errors="coerce").max()
                span_years = (
                    (dmax - dmin).days / 365.25
                    if pd.notnull(dmin) and pd.notnull(dmax)
                    else 0
                )
            else:
                span_years = 0

            sym_map = {"tm": "NSE:TATAMOTORS", "ae": "NSE:ADANIENT"}
            sym = sym_map.get(inst.key)

            if (
                FORCE_REBUILD_ON_SPAN
                and (span_years < (YEARS_BACK - 1))
                and (build_master_from_kite_daily is not None)
                and (KiteConnect is not None)
                and (kite is not None)
                and sym
            ):
                st.info(
                    f"{inst.label}: span {span_years:.1f}y < {YEARS_BACK-1}y — forcing full rebuild ({YEARS_BACK}y)."
                )
                Path(inst.master_path).parent.mkdir(parents=True, exist_ok=True)
                build_master_from_kite_daily(
                    kite, sym, inst.master_path, years_back=YEARS_BACK
                )
                try:
                    from probedge.data.io import load_master as _lm

                    _lm.cache_clear()
                except Exception:
                    pass
                df = session_master(inst.master_path)
                _refresh_precompute_and_fatigue(k, df)
                if df is not None and not df.empty:
                    repo_path = (REPO_ROOT / inst.master_csv_default).resolve()
                    repo_path.parent.mkdir(parents=True, exist_ok=True)
                    df_out = df.copy()
                    df_out["Date"] = pd.to_datetime(
                        df_out["Date"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")
                    df_out.to_csv(repo_path, index=False)
                    _flash(f"Mirrored {inst.label} master → `{repo_path}`", icon="⬆️")
        except Exception as e:
            st.warning(f"{inst.label}: rebuild check failed: {e}")

        # 4) incremental updater (TM only)
        try:
            sym_map = {"tm": "NSE:TATAMOTORS", "ae": "NSE:ADANIENT"}
            sym = sym_map.get(inst.key)
            if (
                (inst.key == "tm")
                and update_master_if_needed
                and KiteConnect
                and (kite is not None)
            ):
                added = update_master_if_needed(kite, inst.master_path, symbol=sym)
                if added:
                    try:
                        from probedge.data.io import load_master as _lm

                        _lm.cache_clear()
                    except Exception:
                        pass
                    df = session_master(inst.master_path)
                    _refresh_precompute_and_fatigue(k, df)

                    repo_path = (REPO_ROOT / inst.master_csv_default).resolve()
                    repo_path.parent.mkdir(parents=True, exist_ok=True)
                    df_out = df.copy()
                    df_out["Date"] = pd.to_datetime(
                        df_out["Date"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")
                    df_out.to_csv(repo_path, index=False)

                    try:
                        gh, missing = _get_github_secrets()
                        if push_to_github and not missing:
                            os.environ.setdefault("GITHUB_TOKEN", gh["token"])
                            os.environ.setdefault("GITHUB_REPO", gh["repo"])
                            if gh["user"]:
                                os.environ.setdefault("GITHUB_USER", gh["user"])
                            if gh["email"]:
                                os.environ.setdefault("GITHUB_EMAIL", gh["email"])
                            if gh["branch"]:
                                os.environ.setdefault("GITHUB_BRANCH", gh["branch"])
                            pushed = _push_git_compat(
                                repo_root=str(REPO_ROOT),
                                commit_message=f"Auto-update {inst.label} master to {df_out['Date'].max()}",
                                paths=[str(repo_path)],
                            )
                        else:
                            pushed = False
                        if not pushed:
                            rel_path = inst.master_csv_default
                            api_ok = _push_via_github_api(
                                repo=gh.get("repo")
                                or get_secret_first("github_repo", "GITHUB_REPO", ""),
                                token=gh.get("token")
                                or get_secret_first("github_token", "GITHUB_TOKEN", ""),
                                path_in_repo=rel_path.replace("\\", "/"),
                                local_file=str(repo_path),
                                branch=gh.get("branch") or "main",
                                message=f"Auto-update {inst.label} master to {df_out['Date'].max()}",
                            )
                            if not api_ok:
                                st.warning(
                                    "Git push/upload did not succeed. File updated locally."
                                )
                            else:
                                st.success("Pushed to GitHub via API.")
                    except Exception as e:
                        st.warning(f"Git push failed: {e}")
                    _flash(f"Updated & mirrored master → `{repo_path}`", icon="⬆️")
        except Exception:
            pass

        master_dfs[k] = df

    # Tabs
    st.session_state["_boot_toasts_done"] = True
    # Disable global autorefresh; fragments handle their own refresh.
    # if st_autorefresh:
    #     st_autorefresh(interval=30_000, key="pe_autorefresh")

    page_tabs = st.tabs(["Terminal", "Live Terminal", "Live Tracker"])

    # Terminal (existing)
    with page_tabs[0]:
        _render_terminal = _safe_import_render_terminal()
        inst_tabs = st.tabs([inst.label for inst in INSTRUMENTS.values()])
        for (k, inst), tab in zip(INSTRUMENTS.items(), inst_tabs):
            with tab:
                _render_terminal(master_dfs[k], inst, kite=kite)

    
    # Live Terminal (Kite-backed)
    st.info("Broker is temporarily disabled. Live Terminal will be available when re-enabled.")
    
    # Live Tracker (existing)
    with page_tabs[2]:
        render_live_tracker(master_dfs.get("tm", pd.DataFrame()), kite=kite)
    


# -------------------------------
# Bootstrap (Kite callback + nav)
# -------------------------------
def safe_bootstrap():
    global kite

    _ensure_data_dirs()

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    # Handle Kite callback (if request_token in query params)
    def _get_qp():
        try:
            return dict(st.query_params)
        except Exception:
            return st.experimental_get_query_params()

    if KiteConnect and API_KEY:
        qp = _get_qp()
        # --- Guard Kite callback handling behind app login ---
        req_tok = qp.get("request_token", "")
        if isinstance(req_tok, list):
            req_tok = req_tok[0] if req_tok else ""
        
        # Only handle Kite callback after ProbEdge app login
        user_logged_in = st.session_state.get("logged_in", False)
        
        if req_tok and user_logged_in and not st.session_state.get("access_token") and not st.session_state.get("__kite_cb_done__", False):



            try:
                token = handle_callback(req_tok)
                access_token = (
                    token.get("access_token") if isinstance(token, dict)
                    else (token if isinstance(token, str) else None)
                )
                if access_token:
                    st.session_state["access_token"] = access_token
                    st.session_state["__kite_cb_done__"] = True
                    # (existing code to set globals()["kite"], stash tokens, etc.)
            except Exception as e:
                st.error(f"Kite login failed: {e}")


    # ---------- ZERODHA CONNECT PAGE ----------
    connected = bool(st.session_state.get("access_token"))
    
    if False and (not connected) and st.session_state.get("logged_in", False):
        # Hide sidebar and center content; only logo + compact button
        st.markdown("""
            <style>
            [data-testid="stSidebar"] { display: none; }
            section.main > div.block-container {
                min-height: 95vh !important;
                display: flex !important;
                flex-direction: column !important;
                justify-content: center !important;
                align-items: center !important;
                padding-top: 0 !important;
                padding-bottom: 0 !important;
            }
            </style>
        """, unsafe_allow_html=True)
    
        from app.ui import show_logo
        show_logo(centered=True)
    
        # Draw compact connect button only (no text box, no extra copy)
        from kite_login import login_button
        login_button(label="Connect to login", width_px=220)
    
        # Do NOT call handle_callback() here — top-of-function handles it silently.
        st.stop()

    # ---------- LOGIN PAGE ----------
    if not st.session_state.get("logged_in", False):
        # Hide sidebar until login
        st.markdown("""
            <style>[data-testid="stSidebar"] {display: none;}</style>
        """, unsafe_allow_html=True)
        render_login()
        return
    
    # ---------- NAVIGATION AFTER LOGIN ----------
    with st.sidebar:
        st.markdown("### Navigation")
        st.caption("Broker: disabled (temporary)")
        nav_choice = st.radio(
            "Go to", ["Terminal", "Journal"], index=0, key="pe_nav_radio"
        )
        # Multi-instrument maintenance (TM, LT, SBIN, etc.)
        render_sidebar_maintenance(kite=globals().get("kite"))

        # Instrument-level maintenance (TM/LT/SBIN)
        with st.sidebar.expander("Maintenance: Instrument updates (TM/LT/SBIN)"):
            # Discover instruments + labels
            avail = {k: v for k, v in INSTRUMENTS.items() if getattr(v, "master_path", None)}
            labels = {k: getattr(v, "label", k.upper()) for k, v in avail.items()}
            default_keys = [k for k in ["tm", "lt", "sbin"] if k in avail] or list(avail.keys())

            sel2 = st.multiselect(
                "Select instruments",
                options=list(avail.keys()),
                default=default_keys,
                format_func=lambda k: labels.get(k, k.upper()),
                key="__maint_sel2__"
            )

            # Safe intraday-utils import
            try:
                import app.intraday_utils as iu
            except Exception:
                iu = None

            def _has(name: str) -> bool:
                return (iu is not None) and hasattr(iu, name)

            def _sync_5m_to_master(inst_key: str, master_path: str) -> dict:
                if _has("sync_master_full_from_5m"):
                    return iu.sync_master_full_from_5m(inst_key, master_path)
                return {"rows_added": 0, "rows_updated": 0, "path": master_path}

            def _ensure_all_up_to_date(inst_key: str, master_path: str, kite=None) -> dict:
                if _has("ensure_5m_and_master_up_to_date"):
                    return iu.ensure_5m_and_master_up_to_date(inst_key, master_path, kite=kite)
                r = _sync_5m_to_master(inst_key, master_path)
                return {
                    "dates_5m_added": 0,
                    "bars_5m_appended": 0,
                    "master_rows_added": r.get("rows_added", 0) + r.get("rows_updated", 0),
                }

            c1, c2, c3 = st.columns(3)

            # 1) Today update for selected
            if c1.button("Run today update (selected)", use_container_width=True, key="__maint_run_today__"):
                logs = []
                for k in sel2:
                    inst = avail[k]
                    try:
                        if (k == "tm") and (run_daily_update is not None):
                            summary = run_daily_update(kite=globals().get("kite"))
                            logs.append(f"**{labels[k]}**: {summary.get('msg','ok')} {summary.get('date','')}")
                        else:
                            res = _ensure_all_up_to_date(k, inst.master_path, kite=globals().get("kite"))
                            logs.append(
                                f"**{labels[k]}**: +{res.get('dates_5m_added',0)} day(s) 5m, "
                                f"+{res.get('bars_5m_appended',0)} bar(s), "
                                f"+{res.get('master_rows_added',0)} master row(s)"
                            )
                    except Exception as e:
                        logs.append(f"**{labels[k]}**: failed — {e}")
                st.success("Today update complete.")
                for line in logs: st.caption(line)
                try:
                    from probedge.data.io import load_master as _lm; _lm.cache_clear()
                except Exception: pass
                try:
                    st.cache_data.clear()
                except Exception: pass

            # 2) Kite fetch + sync (selected)
            if c2.button("Kite fetch + sync (selected)", use_container_width=True, key="__maint_kite_sync_sel__"):
                if globals().get("kite") is None:
                    st.warning("Kite is not connected; falling back to 5m → master only.")
                logs = []
                for k in sel2:
                    inst = avail[k]
                    try:
                        res = _ensure_all_up_to_date(k, inst.master_path, kite=globals().get("kite"))
                        logs.append(
                            f"**{labels[k]}**: +{res.get('dates_5m_added',0)} day(s) 5m, "
                            f"+{res.get('bars_5m_appended',0)} bar(s), "
                            f"+{res.get('master_rows_added',0)} master row(s)"
                        )
                    except Exception as e:
                        logs.append(f"**{labels[k]}**: failed — {e}")
                st.success("Kite+Sync complete.")
                for line in logs: st.caption(line)

            # 3) 5m → master (selected)
            if c3.button("5m → master (selected)", use_container_width=True, key="__maint_5m_to_master_sel__"):
                logs = []
                for k in sel2:
                    inst = avail[k]
                    try:
                        res = _sync_5m_to_master(k, inst.master_path)
                        logs.append(
                            f"**{labels[k]}**: +{res.get('rows_added',0)} added, "
                            f"+{res.get('rows_updated',0)} updated → {res.get('path','')}"
                        )
                    except Exception as e:
                        logs.append(f"**{labels[k]}**: failed — {e}")
                st.success("Sync complete.")
                for line in logs: st.caption(line)

            st.divider()
            st.caption("Backfill (selected instruments)")

            start2 = st.date_input("Backfill start", format="YYYY-MM-DD", key="__maint_bf_start__")
            end2   = st.date_input("Backfill end",   format="YYYY-MM-DD", key="__maint_bf_end__")
            if st.button("Backfill 5-min + master (selected range)", use_container_width=True, key="__maint_bf_go__"):
                logs = []
                try_fetch = getattr(iu, "try_fetch_kite_5m_for_dates", None)
                for k in sel2:
                    inst = avail[k]
                    try:
                        s_str = start2.strftime("%Y-%m-%d") if isinstance(start2, _date) else None
                        e_str = end2.strftime("%Y-%m-%d")   if isinstance(end2, _date)   else None

                        if (k == "tm"):
                            try:
                                from tools.update_tm5min_and_master import update_tm5min, update_master_from_tm5min
                                res = update_tm5min(kite=globals().get("kite"), start=s_str, end=e_str)
                                logs.append(f"**{labels[k]}** tm5min: +{res.get('rows_added',0)} — {res.get('msg','')}")
                                if s_str and e_str:
                                    for d in pd.date_range(s_str, e_str, freq="B"):
                                        update_master_from_tm5min(d.strftime("%Y-%m-%d"))
                                else:
                                    _sync_5m_to_master(k, inst.master_path)
                            except Exception:
                                if try_fetch and s_str and e_str and globals().get("kite"):
                                    days = [d.strftime("%Y-%m-%d") for d in pd.date_range(s_str, e_str, freq="B")]
                                    try_fetch(k, days, globals().get("kite"))
                                _sync_5m_to_master(k, inst.master_path)
                                logs.append(f"**{labels[k]}**: backfilled via generic path.")
                        else:
                            if try_fetch and s_str and e_str and globals().get("kite"):
                                days = [d.strftime("%Y-%m-%d") for d in pd.date_range(s_str, e_str, freq="B")]
                                try_fetch(k, days, globals().get("kite"))
                            _sync_5m_to_master(k, inst.master_path)
                            logs.append(f"**{labels[k]}**: backfilled & synced.")
                    except Exception as e:
                        logs.append(f"**{labels[k]}**: backfill failed — {e}")
                st.success("Backfill complete.")
                for line in logs: st.caption(line)
                try:
                    from probedge.data.io import load_master as _lm; _lm.cache_clear()
                except Exception: pass
                try:
                    st.cache_data.clear()
                except Exception: pass
                    
    # Rehydrate kite handle if needed
    if (
        KiteConnect
        and API_KEY
        and st.session_state.get("access_token")
        and (globals().get("kite") is None)
    ):
        try:
            kite_local = KiteConnect(api_key=API_KEY)  # type: ignore[assignment]
            kite_local.set_access_token(st.session_state["access_token"])
            st.session_state["kite"] = kite_local
            globals()["kite"] = kite_local
        except Exception as e:
            st.warning(f"Kite connection unavailable: {e}")
            globals()["kite"] = None

    # ---- Daily append (post-close opportunistic) ----
    try:
        from probedge.data.io import load_master as _lm
    except Exception:
        _lm = None

    try:
        sym_map = {"tm": "NSE:TATAMOTORS", "ae": "NSE:ADANIENT"}

        added_tm = append_today_if_connected_and_closed(
            globals().get("kite"),
            INSTRUMENTS["tm"].master_path,
            symbol=sym_map["tm"],
        )
        added_lt = append_today_if_connected_and_closed(
            globals().get("kite"),
            INSTRUMENTS["lt"].master_path,
            symbol="NSE:LT",
        ) or 0
        
        added_sbin = append_today_if_connected_and_closed(
            globals().get("kite"),
            INSTRUMENTS["sbin"].master_path,
            symbol="NSE:SBIN",
        ) or 0

        added_ae = 0
        inst_ae = INSTRUMENTS.get("ae")
        if inst_ae:
            added_ae = append_today_if_connected_and_closed(
                globals().get("kite"),
                inst_ae.master_path,
                symbol=sym_map.get("ae", ""),
            ) or 0

        total_added = (added_tm or 0) + (added_lt or 0) + (added_sbin or 0) + (added_ae or 0)

        if total_added:
            # clear caches
            try:
                if _lm: _lm.cache_clear()
            except Exception:
                pass
            try:
                st.cache_data.clear()
            except Exception:
                pass

            # mirror TM (and AE if present) to repo path like in the incremental path
            for key in ("tm", "lt", "sbin", "ae"):
                inst = INSTRUMENTS.get(key)
                if not inst:
                    continue
                try:
                    df_cur = load_master(inst.master_path)
                    if df_cur is None or df_cur.empty:
                        continue
                    repo_path = (REPO_ROOT / inst.master_csv_default).resolve()
                    repo_path.parent.mkdir(parents=True, exist_ok=True)
                    df_out = df_cur.copy()
                    df_out["Date"] = pd.to_datetime(df_out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
                    df_out.to_csv(repo_path, index=False)

                    # try GitHub push via API first (same helper you already use)
                    try:
                        gh, missing = _get_github_secrets()
                        pushed = False
                        if not missing:
                            os.environ.setdefault("GITHUB_TOKEN", gh["token"])
                            os.environ.setdefault("GITHUB_REPO", gh["repo"])
                            if gh["user"]:
                                os.environ.setdefault("GITHUB_USER", gh["user"])
                            if gh["email"]:
                                os.environ.setdefault("GITHUB_EMAIL", gh["email"])
                            if gh["branch"]:
                                os.environ.setdefault("GITHUB_BRANCH", gh["branch"])
                            # fallback: direct API (path relative to repo root)
                            rel_path = inst.master_csv_default
                            pushed = _push_via_github_api(
                                repo=gh.get("repo") or get_secret_first("github_repo", "GITHUB_REPO", ""),
                                token=gh.get("token") or get_secret_first("github_token", "GITHUB_TOKEN", ""),
                                path_in_repo=rel_path.replace("\\", "/"),
                                local_file=str(repo_path),
                                branch=gh.get("branch") or "main",
                                message=f"Auto-update {inst.label} master to {df_out['Date'].max()}",
                            )
                        if not pushed:
                            st.info(f"Updated locally: {repo_path.name} (GitHub push skipped or not configured).")
                    except Exception as e:
                        st.warning(f"Git push (daily append) failed: {e}")

                except Exception as e:
                    st.warning(f"Mirror after daily append failed for {inst.label}: {e}")

            st.success(
                f"Masters updated (TM +{added_tm}, LT +{added_lt}, SBIN +{added_sbin}, AE +{added_ae})."
            )

        else:
            st.caption("Daily append check: no new rows (either pre-close, holiday/weekend, or already appended).")

    except Exception as e:
        st.warning(f"Daily append check failed: {e}")


    # Route
    try:
        if nav_choice == "Terminal":
            run_main_app()

        elif nav_choice == "Journal":
            # Pass an explicit default to be extra clear (journal also loads YAML internally)
            cfg = PROBEDGE_CFG.copy() if isinstance(PROBEDGE_CFG, dict) else {}
            j = cfg.get("journal") or {}
            j.setdefault("data_dir", str((REPO_ROOT / "data" / "latest").resolve()))
            cfg["journal"] = j
            # Make path available to any adapter that relies on env
            os.environ["PE_JOURNAL_DATA_DIR"] = j["data_dir"]
            render_journal_view(cfg)


    except Exception as e:
        st.error("Unexpected error while rendering the page.")
        st.exception(e)

# -------------------------------
# Entry
# -------------------------------
if __name__ == "__main__":
    safe_bootstrap()
