from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, List

import pandas as pd

from probedge.storage.resolver import locate_for_read, journal_path, state_path


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _legacy_master_paths(sym: str) -> List[Path]:
    """
    Locate legacy dashboard masters (if present) so we can extend history,
    mainly for TATAMOTORS.

    Layout we expect:
      legacy/stock-market-amir-dashboard-main/
        stock-market-amir-dashboard-main/
          data/masters/{SYM}_5MINUTE_MASTER.csv
    """
    base = Path("legacy/stock-market-amir-dashboard-main/stock-market-amir-dashboard-main/data/masters")
    if not base.exists():
        return []

    candidates: List[Path] = []
    # direct symbol file
    p1 = base / f"{sym}_5MINUTE_MASTER.csv"
    if p1.exists():
        candidates.append(p1)

    # TATAMOTORS → TMPV, so also check TMPV explicitly
    if sym.upper() == "TATAMOTORS":
        p2 = base / "TMPV_5MINUTE_MASTER.csv"
        if p2.exists():
            candidates.append(p2)

    # Deduplicate
    uniq: List[Path] = []
    seen = set()
    for p in candidates:
        if str(p) not in seen:
            uniq.append(p)
            seen.add(str(p))
    return uniq


def read_master(sym: str) -> pd.DataFrame:
    """
    Unified master reader used by API routes (/api/matches, /api/superpath, /api/freq3).

    - Primary source: current Probedge master via resolver.locate_for_read("masters", sym)
      (handles aliases like TATAMOTORS → TMPV).
    - For TATAMOTORS (and if present), we also merge in legacy dashboard masters
      under legacy/.../data/masters to recover full backtest history.
    """
    sym_up = sym.upper()

    # Current Probedge master
    p_curr = locate_for_read("masters", sym)
    df_curr = _read_csv(p_curr)

    # Legacy masters (if any)
    df_legacy_list: list[pd.DataFrame] = []
    for p in _legacy_master_paths(sym_up):
        df_leg = _read_csv(p)
        if not df_leg.empty:
            df_legacy_list.append(df_leg)

    if df_curr.empty and not df_legacy_list:
        return pd.DataFrame()

    all_frames = [df_curr] + df_legacy_list
    df = pd.concat(all_frames, ignore_index=True)

    # Normalize Date and drop duplicates by Date + tags + Result to avoid double counting
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    dedup_keys = [c for c in ["Date", "PrevDayContext", "OpenLocation", "OpeningTrend", "Result"] if c in df.columns]
    if dedup_keys:
        df = df.drop_duplicates(subset=dedup_keys)

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