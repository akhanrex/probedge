# app/services/intraday.py
# Intraday helpers used by Terminal + Live views
# - Reads per-day 5m CSVs from repo folders (tm5min / ad5min)
# - Can backfill missing days via Kite (if connected)
# - All functions are safe to import without Streamlit

from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import pandas as pd


# ----------------------------
# Repo paths and conventions
# ----------------------------
def _detect_repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        try:
            names = {x.name for x in p.iterdir()}
            if {".git", "requirements.txt"} & names:
                return p
        except Exception:
            continue
    return here.parents[3] if len(here.parents) >= 4 else here.parents[-1]


REPO_ROOT = _detect_repo_root()

# Folder names holding per-day 5m files
# Expected filenames: YYYY-MM-DD.csv with columns:
#   DateTime, Open, High, Low, Close, Volume  (case-insensitive accepted)
INTRA_DIRS = {"tm": "tm5min", "ae": "ad5min"}

# Map to Zerodha trading symbols for backfill via Kite
SYMBOLS = {"tm": "TATAMOTORS", "ae": "ADANIENT"}


# ----------------------------
# Small utilities / logging
# ----------------------------
def _log(msg: str) -> None:
    try:
        import streamlit as st  # optional

        st.caption(msg)
    except Exception:
        print(msg)


def _norm_cols(df0: pd.DataFrame) -> pd.DataFrame:
    """Normalize common intraday column names to: datetime, open, high, low, close, volume."""
    if df0 is None or df0.empty:
        return pd.DataFrame()
    df = df0.copy()
    cols = {c.lower(): c for c in df.columns}

    def c_(x):
        return cols.get(x, x)

    df.rename(
        columns={
            c_("DateTime".lower()): "datetime",
            c_("datetime"): "datetime",
            c_("Open".lower()): "open",
            c_("open"): "open",
            c_("High".lower()): "high",
            c_("high"): "high",
            c_("Low".lower()): "low",
            c_("low"): "low",
            c_("Close".lower()): "close",
            c_("close"): "close",
            c_("Volume".lower()): "volume",
            c_("volume"): "volume",
        },
        inplace=True,
    )
    # Coerce types
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    for numc in ("open", "high", "low", "close", "volume"):
        if numc in df.columns:
            df[numc] = pd.to_numeric(df[numc], errors="coerce")
    df = df.dropna(subset=["datetime", "open", "high", "low", "close"])
    return df.sort_values("datetime").reset_index(drop=True)


def _dir_for_key(inst_key: str) -> Path:
    dirname = INTRA_DIRS.get(inst_key, "")
    return (REPO_ROOT / dirname).resolve()


def _path_for_date(inst_key: str, dt: pd.Timestamp) -> Path:
    folder = _dir_for_key(inst_key)
    folder.mkdir(parents=True, exist_ok=True)
    fname = f"{pd.to_datetime(dt).date().isoformat()}.csv"
    return folder / fname


# ----------------------------
# Public API
# ----------------------------
def latest_dates_from_matches(
    df_view: pd.DataFrame, max_days: int = 30
) -> List[pd.Timestamp]:
    """
    From the matches table (already filtered by tags/dates), take the most recent unique dates.
    """
    if df_view is None or df_view.empty or "Date" not in df_view.columns:
        return []
    d = (
        pd.to_datetime(df_view["Date"], errors="coerce")
        .dropna()
        .dt.normalize()
        .drop_duplicates()
    )
    d = d.sort_values(ascending=False).head(int(max_days))
    return list(d)


def slice_intraday_by_dates(
    inst_key: str, dates: Iterable[pd.Timestamp]
) -> Dict[pd.Timestamp, pd.DataFrame]:
    """
    Load per-day intraday CSVs for the given dates from the repo folders.
    Returns {date -> DataFrame} (normalized columns).
    """
    out: Dict[pd.Timestamp, pd.DataFrame] = {}
    for dt in dates:
        dt_norm = pd.to_datetime(dt).normalize()
        p = _path_for_date(inst_key, dt_norm)
        if p.exists() and p.is_file():
            try:
                df = pd.read_csv(p)
                out[dt_norm] = _norm_cols(df)
            except Exception as e:
                _log(f"Could not read intraday file: {p.name} ({e})")
                out[dt_norm] = pd.DataFrame()
        else:
            out[dt_norm] = pd.DataFrame()  # not found
    return out


def _get_token_for_symbol(
    kite, tradingsymbol: str, exchange: str = "NSE"
) -> Optional[int]:
    """
    Resolve Zerodha instrument_token for the given trading symbol.
    Cached in-memory per session.
    """
    cache_key = f"_token_{exchange}_{tradingsymbol}"
    try:
        import streamlit as st

        if cache_key in st.session_state:
            return st.session_state[cache_key]
    except Exception:
        pass

    try:
        inst = kite.instruments(exchange)  # list of dicts
        for row in inst:
            if str(row.get("tradingsymbol", "")).upper() == tradingsymbol.upper():
                token = int(row.get("instrument_token"))
                try:
                    import streamlit as st

                    st.session_state[cache_key] = token
                except Exception:
                    pass
                return token
    except Exception as e:
        _log(f"Token lookup failed for {exchange}:{tradingsymbol} — {e}")
    return None


def try_fetch_kite_5m_for_dates(
    inst_key: str, dates: Iterable[pd.Timestamp], kite
) -> Dict[pd.Timestamp, pd.DataFrame]:
    """
    Attempt to backfill missing days from Zerodha (Kite) historical 5m data.
    Saves CSVs next to local files for caching. Returns {date -> DataFrame} for fetched ones.
    """
    out: Dict[pd.Timestamp, pd.DataFrame] = {}
    if kite is None:
        return out

    symbol = SYMBOLS.get(inst_key)
    if not symbol:
        return out

    token = _get_token_for_symbol(kite, symbol, exchange="NSE")
    if not token:
        return out

    for dt in dates:
        dt_norm = pd.to_datetime(dt).normalize()
        path = _path_for_date(inst_key, dt_norm)
        if path.exists():  # already present
            continue
        # Historical data API expects datetime with timezone-naive date range (inclusive -> exclusive)
        start = datetime.combine(dt_norm.date(), datetime.min.time())
        end = start + timedelta(days=1)

        try:
            candles = kite.historical_data(
                instrument_token=token,
                from_date=start,
                to_date=end,
                interval="5minute",
                continuous=False,
                oi=False,
            )
            if not candles:
                out[dt_norm] = pd.DataFrame()
                continue
            df = pd.DataFrame(candles)
            # Zerodha fields: date, open, high, low, close, volume
            # Normalize 'date' -> 'datetime'
            if "date" in df.columns:
                df.rename(columns={"date": "datetime"}, inplace=True)
            df = _norm_cols(df)
            # persist for cache
            try:
                df.to_csv(path, index=False)
            except Exception as e:
                _log(f"Could not write intraday cache: {path.name} ({e})")
            out[dt_norm] = df
        except Exception as e:
            _log(f"Kite fetch failed for {symbol} {dt_norm.date()}: {e}")
            out[dt_norm] = pd.DataFrame()
    return out


def ensure_intraday_up_to_date(inst_key: str, kite=None) -> None:
    """
    Light guard: ensure the directory exists. (Intentionally minimal.)
    You can expand this later to auto-backfill recent sessions, etc.
    """
    _dir_for_key(inst_key).mkdir(parents=True, exist_ok=True)
    # No heavy work here; widget triggers on-demand fetch when needed.
