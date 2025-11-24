# apps/runtime/self_diag_tm5_coverage.py

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SYMBOLS = [
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

TM5_PATH = ROOT / "data" / "intraday"
START_DAY = date(2025, 8, 1)
END_DAY = date(2025, 9, 30)


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    print(f"[TM5-DIAG] ROOT = {ROOT}")
    print(f"[TM5-DIAG] Checking coverage from {START_DAY} to {END_DAY}\n")

    for sym in SYMBOLS:
        path = TM5_PATH / f"{sym}_5minute.csv"
        if not path.exists():
            print(f"{sym}: MISSING FILE {path}")
            continue

        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"{sym}: ERROR reading {path}: {e}")
            continue

        # Normalized date column
        if "DateTime" not in df.columns:
            print(f"{sym}: no DateTime column in {path}")
            continue

        df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
        df = df.dropna(subset=["DateTime"])
        df["DATE"] = df["DateTime"].dt.date

        have_dates = set(df["DATE"].unique())
        missing = [d for d in daterange(START_DAY, END_DAY) if d not in have_dates]

        print(f"{sym}: have {len(have_dates)} distinct days in TM5")
        print(f"  min={min(have_dates) if have_dates else None}, max={max(have_dates) if have_dates else None}")
        if missing:
            print(f"  MISSING DAYS ({len(missing)}): "
                  f"{', '.join(d.isoformat() for d in missing[:10])}"
                  f"{' ...' if len(missing) > 10 else ''}")
        else:
            print("  FULL COVERAGE in requested range")
        print()


if __name__ == "__main__":
    main()
