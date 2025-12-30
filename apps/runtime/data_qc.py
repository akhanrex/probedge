"""
Lightweight data quality check for recent intraday + master files.

Usage:
    python -m apps.runtime.data_qc

- Checks last N_RECENT_DAYS of intraday 5-min files for each symbol
  (basic structure, duplicates, obviously missing bars).
- Checks that master files have exactly one row per recent day.
- Writes a small JSON summary into data/state/data_qc.json.
- Exits 0 on OK, 1 on any error.
"""

from datetime import datetime, date
from pathlib import Path
import json
import sys

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import intraday_path, master_path

# how many recent *trading days* to examine per symbol
N_RECENT_DAYS = 5


def _err(msg: str, errors: list[str]) -> None:
    print(f"[data_qc] ERROR: {msg}")
    errors.append(msg)


def qc_intraday(sym: str, errors: list[str]) -> list[date]:
    """
    Basic checks on intraday 5-min file:
    - file exists
    - columns: date,time,open,high,low,close,volume
    - last N_RECENT_DAYS each have data, no duplicated (date,time),
      and a reasonable #bars + time window.

    NOTE: For *today's* date we only WARN on partial sessions
    (early last bar / low bar count) so that live mornings don't
    fail QC. Older days remain strict.
    """
    path = intraday_path(sym)
    print(f"[data_qc] intraday for {sym}: {path}")

    if not path.exists():
        _err(f"Missing intraday file for {sym}: {path}", errors)
        return []

    try:
        df = pd.read_csv(path)
    except Exception as e:
        _err(f"{sym}: failed to read intraday CSV: {e}", errors)
        return []

    required = {"date", "time", "open", "high", "low", "close", "volume"}
    cols_lower = {c.lower(): c for c in df.columns}
    missing = [c for c in required if c not in cols_lower]
    if missing:
        _err(f"{sym}: intraday missing columns {missing}", errors)
        return []

    # Normalize column names
    for need in required:
        src = cols_lower[need]
        if src != need:
            df.rename(columns={src: need}, inplace=True)

    # parse date/time
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce").dt.time

    df = df.dropna(subset=["date", "time"])

    if df.empty:
        _err(f"{sym}: intraday has no valid rows after parsing date/time", errors)
        return []

    days = sorted(d for d in df["date"].unique() if pd.notna(d))
    if not days:
        _err(f"{sym}: intraday has no distinct dates", errors)
        return []

    recent = days[-N_RECENT_DAYS:]
    today = date.today()

    for d in recent:
        day_df = df[df["date"] == d].copy()
        is_today = (d == today)

        if day_df.empty:
            # For historical days → ERROR. For today → WARN only.
            msg = f"{sym}: no intraday rows for recent day {d}"
            if is_today:
                print(f"[data_qc] WARN: {msg}")
            else:
                _err(msg, errors)
            continue

        # duplicates
        if day_df.duplicated(subset=["date", "time"]).any():
            _err(f"{sym}: duplicate (date,time) rows on {d}", errors)

        # NaNs in prices (always ERROR, even for today)
        if day_df[["open", "high", "low", "close"]].isna().any().any():
            _err(f"{sym}: NaN OHLC values on {d}", errors)

        # basic time window + bar count sanity
        times = day_df["time"].sort_values()
        first = times.iloc[0]
        last = times.iloc[-1]
        n_bars = len(day_df)

        # Loose checks: for *historical* days this is ERROR.
        # For today's partial session, only WARN so live mornings pass.
        if first > datetime.strptime("09:30:00", "%H:%M:%S").time():
            msg = f"{sym}: first bar on {d} starts late at {first}"
            if is_today:
                print(f"[data_qc] WARN: {msg}")
            else:
                _err(msg, errors)

        if last < datetime.strptime("15:15:00", "%H:%M:%S").time():
            msg = f"{sym}: last bar on {d} ends early at {last}"
            if is_today:
                print(f"[data_qc] WARN: {msg}")
            else:
                _err(msg, errors)

        if n_bars < 50:
            msg = f"{sym}: only {n_bars} intraday bars on {d}"
            if is_today:
                print(f"[data_qc] WARN: {msg}")
            else:
                _err(msg, errors)

    return recent


def qc_master(sym: str, recent_days: list[date], errors: list[str]) -> None:
    """
    For each recent day that exists in intraday, ensure master has exactly 1 row.
    """
    path = master_path(sym)
    print(f"[data_qc] master for {sym}: {path}")

    if not path.exists():
        _err(f"Missing master file for {sym}: {path}", errors)
        return

    try:
        df = pd.read_csv(path)
    except Exception as e:
        _err(f"{sym}: failed to read master CSV: {e}", errors)
        return

    cols_lower = {c.lower(): c for c in df.columns}
    if "date" not in cols_lower:
        _err(f"{sym}: master missing 'Date' column", errors)
        return

    date_col = cols_lower["date"]
    df["Date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df.dropna(subset=["Date"])

    for d in recent_days:
        rows = df[df["Date"] == d]
        if len(rows) == 0:
            _err(f"{sym}: master has no row for {d}", errors)
        elif len(rows) > 1:
            _err(f"{sym}: master has {len(rows)} rows for {d}", errors)


def write_status_json(status: str, issues: list[str]) -> None:
    """
    Store QC summary as data_qc.json next to live_state.json so API/UI can read it.
    """
    state_path = Path(SETTINGS.paths.state)   # e.g. data/state/live_state.json
    state_dir = state_path.parent             # e.g. data/state/
    state_dir.mkdir(parents=True, exist_ok=True)
    qc_path = state_dir / "data_qc.json"

    payload = {
        "status": status,
        "checked_at": datetime.now().isoformat(),
        "issues": issues,
    }
    with qc_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"[data_qc] wrote status to {qc_path}")


def main() -> None:
    errors: list[str] = []

    print(f"[data_qc] Starting QC for symbols: {SETTINGS.symbols}")

    # intraday first, collect recent days per symbol
    intraday_days: dict[str, list[date]] = {}
    for sym in SETTINGS.symbols:
        recent = qc_intraday(sym, errors)
        intraday_days[sym] = recent

    # then master consistency vs intraday
    for sym, days in intraday_days.items():
        if not days:
            continue
        qc_master(sym, days, errors)

    if errors:
        print("[data_qc] QC FAILED.")
        for e in errors:
            print(f"  - {e}")
        write_status_json("ERROR", errors)
        sys.exit(1)

    print("[data_qc] QC OK.")
    write_status_json("OK", [])
    sys.exit(0)


if __name__ == "__main__":
    main()
