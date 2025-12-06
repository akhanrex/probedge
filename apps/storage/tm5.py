from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd

from probedge.storage.resolver import locate_for_read, journal_path, state_path


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def read_master(sym: str) -> pd.DataFrame:
    """
    Unified master reader used by API routes (/api/matches, /api/superpath, /api/freq3).

    Uses resolver.locate_for_read("masters", sym), which:
      - applies symbol aliases (TATAMOTORS → TMPV)
      - respects SETTINGS.data_dir and SETTINGS.paths.masters
      - falls back to legacy patterns if needed
    """
    p = locate_for_read("masters", sym)
    df = _read_csv(p)
    if df.empty:
        return df
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


def read_journal() -> pd.DataFrame:
    """
    Return the main journal CSV as a DataFrame.
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
    Read live_state JSON written by Phase A spine.
    """
    p: Path = state_path()
    if not p.exists():
        raise FileNotFoundError(str(p))
    with open(p, "r") as f:
        return json.load(f)
