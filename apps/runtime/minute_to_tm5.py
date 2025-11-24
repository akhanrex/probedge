# apps/runtime/minute_to_tm5.py
import os
from __future__ import annotations

from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger

log = get_logger(__name__)

HIST_ROOT = Path("data/hist_1m")  # where kite_hist_1m_fetch wrote files


def tm5_path_for_symbol(sym: str) -> Path:
    pattern = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    return Path(pattern.format(sym=sym))


def process_day_for_symbol(day_str: str, sym: str):
    day_dir = HIST_ROOT / day_str
    in_path = day_dir / f"{sym}_1minute.csv"
    if not in_path.exists():
        log.warning("[minute_to_tm5] missing 1m file %s", in_path)
        return None

    df = pd.read_csv(in_path)
    if df.empty:
        return None

    # Ensure datetime
    if "DateTime" not in df.columns:
        if "date" in df.columns:
            df["DateTime"] = pd.to_datetime(df["date"], errors="coerce")
        else:
            raise RuntimeError(f"No DateTime/date column in {in_path}")

    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    df = df.dropna(subset=["DateTime"]).sort_values("DateTime")

    # Restrict to trading session 09:15–15:30
    t = df["DateTime"].dt.time
    df = df[(t >= dtime(9, 15)) & (t <= dtime(15, 30))].copy()
    if df.empty:
        return None

    df = df.set_index("DateTime")

    # Resample to 5-minute bars
    ohlc = df.resample("5min").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"])

    # Reset index, rename columns to match TM5 expectations
    ohlc = ohlc.reset_index().rename(
        columns={
            "DateTime": "DateTime",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )

    # Attach Date and _mins like Colab reader does
    ohlc["Date"] = ohlc["DateTime"].dt.normalize()
    ohlc["_mins"] = ohlc["DateTime"].dt.hour * 60 + ohlc["DateTime"].dt.minute

    tm5_path = tm5_path_for_symbol(sym)
    tm5_path.parent.mkdir(parents=True, exist_ok=True)

    # Append or create
    if tm5_path.exists():
        existing = pd.read_csv(tm5_path)
        # drop any existing rows for this Date
        day_norm = pd.to_datetime(day_str).normalize()
    
        if os.path.exists(tm5_path):
            existing = pd.read_csv(tm5_path)
    
            if "Date" in existing.columns:
                # Newer shape: use Date column
                existing["Date"] = pd.to_datetime(existing["Date"]).dt.normalize()
                existing = existing[existing["Date"] != day_norm]
    
            elif "DateTime" in existing.columns:
                # Older shape: fall back to DateTime
                existing["DateTime"] = pd.to_datetime(existing["DateTime"])
                existing = existing[existing["DateTime"].dt.normalize() != day_norm]
    
            else:
                # Completely unknown shape – safest is to drop old rows for this symbol
                log.warning(
                    "[minute_to_tm5] %s has no Date/DateTime column; "
                    "dropping old data and starting fresh", tm5_path
                )
                existing = pd.DataFrame(columns=df_5.columns)
    
            combined = pd.concat([existing, df_5], ignore_index=True)
    
        else:
            combined = df_5
    
        combined.to_csv(tm5_path, index=False)
        log.info("[minute_to_tm5] updated %s with day %s", tm5_path, day_str)

    else:
        ohlc.to_csv(tm5_path, index=False)
        log.info("[minute_to_tm5] created %s with day %s", tm5_path, day_str)


def main():
    symbols = SETTINGS.symbols or [
        "TMPV",
        "SBIN",
        "RECLTD",
        "JSWENERGY",
        "LT",
        "COALINDIA",
        "ABB",
        "LICI",
        "ETERNAL",
        "JIOFIN",
    ]

    # ---- EDIT THESE DATES to match what you fetched ----
    start = "2025-08-01"
    end   = "2025-09-30"
    # ----------------------------------------------------

    days = pd.date_range(start=start, end=end, freq="D")
    for d in days:
        day_str = d.strftime("%Y-%m-%d")
        day_dir = HIST_ROOT / day_str
        if not day_dir.exists():
            continue

        for sym in symbols:
            process_day_for_symbol(day_str, sym)


if __name__ == "__main__":
    main()
