from __future__ import annotations

from datetime import date, time
from functools import lru_cache
from pathlib import Path
from typing import Tuple

import pandas as pd
from fastapi import HTTPException

from probedge.storage.resolver import locate_for_read
from probedge.infra.log import get_logger

log = get_logger(__name__)

SESSION_START = time(9, 15)
SESSION_END = time(15, 30)

REQUIRED_COLUMNS = ("Date", "Open", "High", "Low", "Close")


@lru_cache(maxsize=64)
def _load_tm5(symbol: str) -> pd.DataFrame:
    """
    Load full 5-minute intraday history for a symbol.

    - Reads data/intraday/{sym}_5minute.csv via resolver.
    - Normalizes column names.
    - Parses Date column to pandas datetime.
    """
    path: Path = locate_for_read("intraday", symbol)

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"TM5 not found for {symbol}")

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        log.exception("Failed to read TM5 CSV for %s at %s", symbol, path)
        raise HTTPException(status_code=500, detail=f"Failed to read TM5: {exc}") from exc

    # Normalize column names (case-insensitive)
    colmap = {c.lower(): c for c in df.columns}

    for needed in REQUIRED_COLUMNS:
        if needed not in df.columns:
            low = needed.lower()
            if low in colmap:
                df[needed] = df.pop(colmap[low])

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        msg = f"TM5 missing columns for {symbol}: {missing}"
        log.error(msg)
        raise HTTPException(status_code=500, detail=msg)

    # Parse Date column
    try:
        df["Date"] = pd.to_datetime(df["Date"])
    except Exception as exc:
        log.exception("Failed to parse Date column for %s", symbol)
        raise HTTPException(
            status_code=500,
            detail=f"TM5 bad Date column for {symbol}: {exc}",
        ) from exc

    df = df.sort_values("Date").reset_index(drop=True)
    return df


def tm5_for_day(symbol: str, d: date) -> pd.DataFrame:
    """
    Return 5-minute intraday bars for a single trading day.

    Primary filter: rows with Date on that calendar day AND within the
    09:15–15:30 session.

    If that slice is empty but there *are* rows for that calendar day
    (e.g. CSV has weird times or date-only stamps), we fall back to
    "date-only" selection instead of raising an error.
    """
    df = _load_tm5(symbol)

    # Calendar-day mask
    date_mask = df["Date"].dt.date == d

    # Trading session mask
    time_series = df["Date"].dt.time
    sess_mask = (time_series >= SESSION_START) & (time_series <= SESSION_END)

    day_session = df[date_mask & sess_mask]

    if not day_session.empty:
        return day_session.reset_index(drop=True)

    # Fallback: if there ARE rows for that calendar day, use them all
    fallback = df[date_mask]
    if not fallback.empty:
        log.warning(
            "tm5_for_day(%s, %s): empty session slice, using date-only fallback (%d rows)",
            symbol,
            d,
            len(fallback),
        )
        return fallback.sort_values("Date").reset_index(drop=True)

    # Truly no data for that day
    raise HTTPException(
        status_code=404,
        detail=f"No intraday bars for {symbol} {d}",
    )
