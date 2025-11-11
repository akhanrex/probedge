# probedge/perf.py — App-wide performance controls (caching, refresh, fast I/O)

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import pandas as pd
import streamlit as st

PERF_VERSION = "no-mmap-2025-10-19"
# NOTE: don't write to the UI from a module import; do it inside a function/page if desired.

# -------- App-wide defaults (override at runtime by editing AppPerf) --------

@dataclass
class AppPerf:
    # Default TTLs (seconds)
    data_ttl: int = 600       # general dataframe computations
    master_ttl: int = 900     # master CSV/Parquet reads
    sig_ttl: int = 60         # signature/weekly stats
    show_spinner: bool = False

# Singleton config you can tweak from any page:
perf = AppPerf()

# A session-scoped version key; weaving this into all caches means one bump
# invalidates the entire app's memoized results.
SESSION_KEY = "__PE_SESSION_VERSION__"


def _ensure_session_version() -> str:
    if SESSION_KEY not in st.session_state:
        st.session_state[SESSION_KEY] = str(time.time())
    return st.session_state[SESSION_KEY]


def invalidate_all() -> None:
    """One-shot invalidation for *all* caches."""
    try:
        st.cache_data.clear()
    except Exception:
        pass
    try:
        st.cache_resource.clear()
    except Exception:
        pass
    # bump session version so all memo_* wrappers get a new key
    st.session_state[SESSION_KEY] = str(time.time())


# ------------------------ Cache decorators (centralized) ---------------------

def memo_data(ttl: int | None = None, show_spinner: bool | None = None):
    """
    Use instead of @st.cache_data. Automatically keys on session version so
    invalidate_all() clears everything consistently.
    """
    ttl_ = perf.data_ttl if ttl is None else ttl
    spn_ = perf.show_spinner if show_spinner is None else show_spinner

    def _wrap(fn):
        @st.cache_data(ttl=ttl_, show_spinner=spn_)
        def _cached(__v: str, *a, **k):
            # __v is the session-version; including it in the key wiring forces refresh
            return fn(*a, **k)

        def _inner(*a, **k):
            return _cached(_ensure_session_version(), *a, **k)

        return _inner

    return _wrap


def memo_resource(show_spinner: bool | None = None):
    """
    Use instead of @st.cache_resource. Automatically keys on session version so
    invalidate_all() clears everything consistently.
    """
    spn_ = perf.show_spinner if show_spinner is None else show_spinner

    def _wrap(fn):
        @st.cache_resource(show_spinner=spn_)
        def _cached(__v: str, *a, **k):
            return fn(*a, **k)

        def _inner(*a, **k):
            return _cached(_ensure_session_version(), *a, **k)

        return _inner

    return _wrap


# ----------------------------- Fast I/O helpers ------------------------------

def _mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except Exception:
        return 0.0


@memo_data()  # inherits perf.data_ttl
def _read_master_csv(path: str) -> pd.DataFrame:
    """
    Safe CSV reader that does NOT memory-map (prevents 'Too many open files').
    Reads file bytes first, then parses from BytesIO so the FD closes immediately.
    """
    try:
        with open(path, "rb") as f:
            data = f.read()
        bio = io.BytesIO(data)
        try:
            df = pd.read_csv(bio, parse_dates=["Date"], engine="c", low_memory=False)
        except Exception:
            bio.seek(0)
            df = pd.read_csv(bio, parse_dates=["Date"], engine="python", low_memory=False)
        return df
    except Exception:
        # last resort (still no memory_map)
        return pd.read_csv(path, parse_dates=["Date"], low_memory=False)


DEFAULT_MASTER_COLUMNS = [
    "Date",
    "PrevDayContext", "OpenLocation", "FirstCandleType", "OpeningTrend", "RangeStatus",
    "Result",
]


def load_master_fast(path: str) -> pd.DataFrame:
    """
    Prefer Parquet if it's fresh; otherwise read CSV and write Parquet mirror.
    This keeps first paint fast and avoids CSV parse hit on every run.
    """
    p = Path(path)
    pq = p.with_suffix(".parquet")
    try:
        if pq.exists() and _mtime(pq) >= _mtime(p):
            return pd.read_parquet(pq)
    except Exception:
        pass

    df = _read_master_csv(path)
    try:
        df.to_parquet(pq, index=False)
    except Exception:
        pass
    return df


def session_master(path: str) -> pd.DataFrame:
    """
    Keep a master DataFrame pinned in session, refreshing by file mtime.
    If the CSV doesn't exist yet, return an empty DataFrame with expected columns
    so the UI can render without crashing.
    """
    key_data = f"__pe_master_df__::{path}"
    key_mtim = f"__pe_master_mtime__::{path}"
    pp = Path(path)
    if not pp.exists():
        # ensure parent dir exists for later writes
        try:
            pp.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        empty = pd.DataFrame({c: pd.Series(dtype="object") for c in DEFAULT_MASTER_COLUMNS})
        empty["Date"] = pd.to_datetime(empty["Date"]).astype("datetime64[ns]")
        st.session_state[key_data] = empty
        st.session_state[key_mtim] = 0.0
        return empty
    mt = _mtime(pp)
    cur = st.session_state.get(key_mtim, -1.0)
    if (key_data not in st.session_state) or (mt != cur):
        st.session_state[key_data] = load_master_fast(path)
        st.session_state[key_mtim] = mt
    return st.session_state[key_data]


# ----------------------------- Live refresh UI -------------------------------

def live_refresh_controls(default_ms: int = 5000) -> Tuple[int, bool]:
    """
    A tiny UI that gives you a refresh slider + pause toggle.
    Returns (refresh_ms, paused).
    """
    try:
        from streamlit_autorefresh import st_autorefresh as _st_auto
    except Exception:
        _st_auto = None

    c1, c2 = st.columns([3, 1])
    with c1:
        ms = st.slider("Refresh (ms)", 2000, 15000, default_ms, 100, key="__pe_rf_ms")
    with c2:
        paused = st.toggle("Pause", value=False, key="__pe_rf_pause")

    if not paused and _st_auto:
        _st_auto(interval=ms, key="__pe_rf_tick")
    return ms, paused


# ------------------------------ Simple page gates ----------------------------

def gate_after_login() -> bool:
    """
    Hide sidebar & paint minimal UI until logged in. Return True if logged in.
    """
    if not st.session_state.get("logged_in", False):
        st.markdown(
            "<style>[data-testid='stSidebar']{display:none !important;}</style>",
            unsafe_allow_html=True,
        )
        return False
    return True


def gate_after_kite() -> bool:
    """
    Helper to quickly check if Kite is connected (access_token present).
    """
    return bool(st.session_state.get("access_token"))
