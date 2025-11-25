# apps/runtime/minute_to_tm5.py

from __future__ import annotations

import os
import logging
from datetime import datetime, time as dtime

import pandas as pd
from pathlib import Path
from typing import Dict

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger

log = get_logger(__name__)

HIST_ROOT = Path("data/hist_1m")  # where kite_hist_1m_fetch wrote files


def tm5_path_for_symbol(sym: str) -> Path:
    pattern = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    return Path(pattern.format(sym=sym))

def process_day_for_symbol(day_str: str, sym: str) -> None:
    """
    Build / update {sym}_5minute.csv for a single day.
    If the 1-minute file for that day is missing, we just log and return.
    """
    day = pd.to_datetime(day_str).date()

    # 1m input and 5m output paths
    minute_path = os.path.join(
        "data", "hist_1m", day_str, f"{sym}_1minute.csv"
    )
    tm5_path = os.path.join(
        "data", "intraday", f"{sym}_5minute.csv"
    )

    # If no 1m file, skip this symbol/day cleanly
    if not os.path.exists(minute_path):
        log.warning("[minute_to_tm5] missing 1m file %s", minute_path)
        return

    # --- build 5m from the 1m file ---
    df_1 = pd.read_csv(minute_path)

    # normalise the datetime column name
    if "DateTime" in df_1.columns:
        dt = pd.to_datetime(df_1["DateTime"])
    elif "date" in df_1.columns:
        dt = pd.to_datetime(df_1["date"])
    elif "datetime" in df_1.columns:
        dt = pd.to_datetime(df_1["datetime"])
    else:
        raise RuntimeError(f"Cannot find datetime column in {minute_path}")

    df_1["DateTime"] = dt
    df_1 = df_1.sort_values("DateTime")

    # 5-minute resample on DateTime
    df_1 = df_1.set_index("DateTime")

    ohlcv = df_1.resample("5min", label="left", closed="left").agg(
        {
            "open": "first" if "open" in df_1.columns else "first",
            "high": "max"   if "high" in df_1.columns else "max",
            "low": "min"    if "low" in df_1.columns else "min",
            "close": "last" if "close" in df_1.columns else "last",
            "volume": "sum" if "volume" in df_1.columns else "sum",
        }
    )

    ohlcv = ohlcv.dropna(how="any")

    df_5 = ohlcv.reset_index()  # DateTime back as a column
    df_5["Date"] = df_5["DateTime"].dt.normalize()

    # --- merge into existing TM5 (if any), de-duping this day ---
    if os.path.exists(tm5_path):
        existing = pd.read_csv(tm5_path)

        if "Date" in existing.columns:
            existing["Date"] = pd.to_datetime(existing["Date"]).dt.normalize()
            existing = existing[existing["Date"] != pd.to_datetime(day)]
        elif "DateTime" in existing.columns:
            dt_existing = pd.to_datetime(existing["DateTime"])
            existing = existing[dt_existing.dt.date != day]

        combined = pd.concat([existing, df_5], ignore_index=True)
    else:
        combined = df_5

    combined.to_csv(tm5_path, index=False)
    log.info("[minute_to_tm5] updated %s with day %s", tm5_path, day_str)


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
