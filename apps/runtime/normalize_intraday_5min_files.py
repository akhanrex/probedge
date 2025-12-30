# apps/runtime/normalize_intraday_5min_files.py
"""
Normalize all intraday 5-min CSVs to a single canonical schema:

    date,time,open,high,low,close,volume

Rules:
- Never invent candles.
- Never touch values except:
  - parse/standardize date/time
  - numeric-cast OHLCV
- Drop only completely broken rows (no date/time).
- Sort by date,time and de-duplicate on (date,time), keeping the *latest* row.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import intraday_path


CANON_COLS = ["date", "time", "open", "high", "low", "close", "volume"]

IST = ZoneInfo("Asia/Kolkata")

def _cutoff_day_iso() -> str:
    """Last weekday (trading day) in IST.
    Prevents weekend/future rows when scripts run on Sat/Sun.
    """
    d = datetime.now(tz=IST).date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()



def _pick_col(df: pd.DataFrame, cols_lower: dict[str, str], *candidates: str):
    """
    Pick the first existing column (case-insensitive) from candidates.
    Returns a Series or None.
    """
    for cand in candidates:
        key = cand.lower()
        if key in cols_lower:
            return df[cols_lower[key]]
    return None


def normalize_file(path: Path) -> None:
    print(f"[normalize] {path}")

    if not path.exists():
        print(f"[normalize] WARNING: missing file, skipping: {path}")
        return

    df = pd.read_csv(path, low_memory=False)

    if df.empty:
        print("[normalize] empty file, skipping")
        return

    # Map lowercase -> original column name
    cols_lower = {c.lower(): c for c in df.columns}

    # ---------- Build date & time ----------
    if "datetime" in cols_lower:
        # Legacy format with combined DateTime column
        dt_col = cols_lower["datetime"]
        dt = pd.to_datetime(df[dt_col], errors="coerce")
        date = dt.dt.strftime("%Y-%m-%d")
        time = dt.dt.strftime("%H:%M:%S")
    else:
        # Expect separate 'date' + 'time'
        if "date" not in cols_lower:
            raise ValueError(f"No 'date' or 'DateTime' column in {path}")

        raw_date = df[cols_lower["date"]]
        d = pd.to_datetime(raw_date, errors="coerce")
        date = d.dt.strftime("%Y-%m-%d")

        if "time" not in cols_lower:
            raise ValueError(f"No 'time' or 'DateTime' column in {path}")

        raw_time = df[cols_lower["time"]].astype(str)
        # Try to normalize time to HH:MM:SS; on failure, keep raw
        tparsed = pd.to_datetime(raw_time, errors="coerce", format="%H:%M:%S")
        time = tparsed.dt.strftime("%H:%M:%S")
        mask_bad = time.isna()
        if mask_bad.any():
            time[mask_bad] = raw_time[mask_bad]

    # ---------- OHLCV ----------
    open_s = _pick_col(df, cols_lower, "open", "o")
    high_s = _pick_col(df, cols_lower, "high", "h")
    low_s = _pick_col(df, cols_lower, "low", "l")
    close_s = _pick_col(df, cols_lower, "close", "c")
    vol_s = _pick_col(df, cols_lower, "volume", "vol", "qty")

    out = pd.DataFrame(
        {
            "date": date,
            "time": time,
            "open": pd.to_numeric(open_s, errors="coerce"),
            "high": pd.to_numeric(high_s, errors="coerce"),
            "low": pd.to_numeric(low_s, errors="coerce"),
            "close": pd.to_numeric(close_s, errors="coerce"),
            "volume": pd.to_numeric(vol_s, errors="coerce"),
        }
    )

    # Drop rows with no date/time at all
    out = out.dropna(subset=["date", "time"])

    # De-duplicate, then hard-trim weekends/future rows (important on Sat/Sun runs)
    out = out.drop_duplicates(subset=["date", "time"], keep="last")

    cutoff_s = _cutoff_day_iso()
    out = out[out["date"].astype(str) <= cutoff_s]
    dts = pd.to_datetime(out["date"], errors="coerce")
    out = out[(dts.notna()) & (dts.dt.dayofweek < 5)]

    # Sort & de-duplicate (final stability)
    out = out.sort_values(["date", "time"])
    out = out.drop_duplicates(subset=["date", "time"], keep="last")

    # Final column order
    out = out[CANON_COLS]

    # Write back
    out.to_csv(path, index=False)
    print(f"[normalize] wrote {len(out)} rows to {path}")


def main():
    print("[normalize] Normalizing intraday files for symbols:", SETTINGS.symbols)
    for sym in SETTINGS.symbols:
        p = intraday_path(sym)
        if not p.exists():
            print(f"[normalize] WARNING: missing intraday for {sym}: {p}")
            continue
        normalize_file(p)
    print("[normalize] Done.")


if __name__ == "__main__":
    main()

