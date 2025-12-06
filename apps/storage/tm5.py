from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from probedge.storage import masters as masters_store
from probedge.storage import tm5 as tm5_store
from probedge.storage.resolver import journal_path, state_path


# --- Master + intraday readers (API expects these names) ---


def read_master(sym: str) -> pd.DataFrame:
    """
    Unified master reader for API routes.

    Uses probedge.storage.masters.read(sym), which is wired to:
      SETTINGS.paths.masters.format(sym=...)
      + legacy fallbacks.
    """
    df = masters_store.read(sym)
    if df is None:
        return pd.DataFrame()
    # Normalize Date as datetime.date (for consistency with older code)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    return df


def read_tm5(sym: str) -> pd.DataFrame:
    """
    Thin wrapper around probedge.storage.tm5.read(sym).
    """
    df = tm5_store.read(sym)
    return df


# --- Journal + live_state readers used by /api/journal and others ---


def read_journal() -> pd.DataFrame:
    """
    Return the main journal CSV as a DataFrame.

    Path is resolved from SETTINGS.paths.journal via resolver.journal_path().
    """
    p: Path = journal_path()
    if not p.exists():
        raise FileNotFoundError(str(p))
    df = pd.read_csv(p)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def read_state_json() -> dict:
    """
    Read live_state JSON file used by Phase A spine.

    Path is resolved from SETTINGS.paths.state via resolver.state_path().
    """
    p: Path = state_path()
    if not p.exists():
        raise FileNotFoundError(str(p))
    with open(p, "r") as f:
        return json.load(f)
