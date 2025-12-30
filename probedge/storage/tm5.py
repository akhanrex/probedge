"""TM5 helpers.

This module intentionally keeps dependencies light. It provides:
- read_tm5_csv(path) -> DataFrame with tz-aware IST DateTime
- last_tm5_row(path) -> dict|None

The intraday CSVs in this project commonly have columns:
  date,time,open,high,low,close,volume
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def read_tm5_csv(path: Union[str, Path]) -> pd.DataFrame:
    """Read a 5-min CSV and return a DataFrame with a tz-aware IST 'DateTime' column.

    Accepts either:
    - 'DateTime' column already present
    - or 'date' + 'time' columns

    Never raises on parse issues; rows with unparseable timestamps become NaT.
    """

    p = Path(path)
    df = pd.read_csv(p)

    # Build datetime series
    if "DateTime" in df.columns:
        dt = pd.to_datetime(df["DateTime"], errors="coerce")
    elif "date" in df.columns and "time" in df.columns:
        d = df["date"].astype(str).str.strip()
        t = df["time"].astype(str).str.strip()
        dt = pd.to_datetime(d + " " + t, errors="coerce")
    elif "date" in df.columns:
        # Sometimes the file has full datetime in 'date'
        dt = pd.to_datetime(df["date"], errors="coerce")
    else:
        raise ValueError(f"TM5 CSV missing datetime columns: {p}")

    # Localize/convert to IST
    try:
        # pandas Series -> dt accessor
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize(IST, ambiguous="NaT", nonexistent="shift_forward")
        else:
            dt = dt.dt.tz_convert(IST)
    except Exception:
        # If localization fails, keep whatever we have; better than crashing planner.
        pass

    df["DateTime"] = dt

    # Convenience normalized columns
    if "date" not in df.columns:
        df["date"] = df["DateTime"].dt.date.astype(str)
    if "time" not in df.columns:
        df["time"] = df["DateTime"].dt.strftime("%H:%M:%S")

    return df


def last_tm5_row(path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """Return the last row of a TM5 CSV as a plain dict, or None if empty/unreadable."""
    try:
        df = read_tm5_csv(path)
        if df.empty:
            return None
        r = df.iloc[-1].to_dict()
        # make json-friendly
        if isinstance(r.get("DateTime"), pd.Timestamp):
            r["DateTime"] = r["DateTime"].isoformat()
        return r
    except Exception:
        return None
