from __future__ import annotations

import os
import json
from datetime import date, datetime, timedelta

from datetime import timedelta

def _last_weekday(d):
    # Sat/Sun -> roll back to Friday
    while d.weekday() >= 5:
        d = d - timedelta(days=1)
    return d
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect


from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import intraday_path

TOKENS_PATH = Path("data/tokens_5min.csv")

# how many calendar days of intraday we refresh (default 10, can override via env)
DEFAULT_DAYS_BACK = 10
N_DAYS = int(os.environ.get("PROBEDGE_INTRADAY_DAYS_BACK", DEFAULT_DAYS_BACK))

SESSION_START = "09:15:00"   # keep full trading session from here


def make_kite() -> KiteConnect:
    api_key = SETTINGS.kite_api_key
    if not api_key:
        raise RuntimeError("KITE_API_KEY not configured in .env")

    sess_file = SETTINGS.kite_session_file
    if not sess_file or not sess_file.exists():
        raise RuntimeError(
            f"Kite session file not found: {sess_file}. "
            "Please login via /api/auth/login_url in your browser."
        )

    with sess_file.open("r", encoding="utf-8") as f:
        sess = json.load(f)

    access_token = sess.get("access_token")
    if not access_token:
        raise RuntimeError(
            "Kite session file has no access_token. "
            "Please login again via /api/auth/login_url."
        )

    session_day = str(sess.get("session_day") or "")
    today = _last_weekday(_last_weekday(date.today())).isoformat()
    today = date.today().isoformat()  # calendar day for Kite session validity
    if session_day != today:
        raise RuntimeError(
            f"Kite session is stale (session_day={session_day}, today={today}). "
            "Please login via /api/auth/login_url in your browser."
        )

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite



def load_tokens():
    if not TOKENS_PATH.exists():
        raise FileNotFoundError(f"Missing {TOKENS_PATH}. Run build_tokens_5min first.")
    df = pd.read_csv(TOKENS_PATH)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return {row["symbol"]: int(row["instrument_token"]) for _, row in df.iterrows()}


def fetch_5min_from_kite(kite, instrument_token: int, start: date, end: date) -> pd.DataFrame:
    """
    Fetch 5-minute candles between start and end (inclusive),
    respecting Kite's 100-day limit.
    """
    all_candles = []
    cursor = start

    while cursor <= end:
        # Kite limit ~100 days; we use 99 to be safe
        chunk_end = min(cursor + timedelta(days=99), end)
        print(f"[intraday] fetching {instrument_token} {cursor} → {chunk_end}")

        candles = kite.historical_data(
            instrument_token=instrument_token,
            from_date=cursor,
            to_date=chunk_end,
            interval="5minute",
        )
        all_candles.extend(candles)

        # Move to the next day after this chunk
        cursor = chunk_end + timedelta(days=1)

    if not all_candles:
        print("[intraday] WARNING: no candles returned")
        return pd.DataFrame(
            columns=["date", "time", "open", "high", "low", "close", "volume"]
        )

    df = pd.DataFrame(all_candles)

    # Kite returns a 'date' column with full timestamp
    df["date"] = pd.to_datetime(df["date"])
    df["time"] = df["date"].dt.strftime("%H:%M:%S")
    df["date"] = df["date"].dt.date

    df = df[["date", "time", "open", "high", "low", "close", "volume"]]
    return df



def refresh_symbol(sym: str, kite: KiteConnect, instrument_token: int, cutoff: date):
    path = intraday_path(sym)
    print(f"[intraday] {sym}: file {path}")

    # Load existing, if any
    if path.exists():
        cur = pd.read_csv(path)
        cols_lower = {c.lower(): c for c in cur.columns}

        # Try to find a date-like column: 'date' or 'datetime'
        date_col = None
        for key in ("date", "datetime"):
            if key in cols_lower:
                date_col = cols_lower[key]
                break

        if date_col:
            # Some older rows may have full ISO datetimes like 2025-12-08T11:20:00+05:30.
            # Normalize by taking only the "YYYY-MM-DD" part before parsing.
            raw_date = cur[date_col].astype(str).str.slice(0, 10)
            cur["date"] = pd.to_datetime(raw_date, errors="coerce").dt.date
            cur = cur.dropna(subset=["date"])
            keep = cur[cur["date"] < cutoff].copy()
            print(f"[intraday] {sym}: keeping {len(keep)} old rows (< {cutoff})")
        else:
            print(f"[intraday] {sym}: WARNING no date/datetime column in {path}, starting fresh")
            keep = pd.DataFrame(
                columns=["date", "time", "open", "high", "low", "close", "volume"]
            )
    else:
        keep = pd.DataFrame(
            columns=["date", "time", "open", "high", "low", "close", "volume"]
        )
        print(f"[intraday] {sym}: no existing file, starting fresh")


    today = _last_weekday(_last_weekday(date.today()))
    print(f"[intraday] {sym}: fetching 5min from {cutoff} to {today}…")
    new = fetch_5min_from_kite(kite, instrument_token, start=cutoff, end=today)
    print(f"[intraday] {sym}: fetched {len(new)} new rows")

    combined = pd.concat([keep, new], ignore_index=True)
    # sort by date + time just to be safe
    combined["date"] = pd.to_datetime(combined["date"])
    combined = combined.sort_values(["date", "time"])
    # convert date back to YYYY-MM-DD string
    combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")

    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(path, index=False)
    print(f"[intraday] {sym}: wrote {len(combined)} rows to {path}")


def main():
    kite = make_kite()
    tokens = load_tokens()

    today = _last_weekday(_last_weekday(date.today()))
    cutoff = today - timedelta(days=N_DAYS)
    print(f"[intraday] Refreshing last {N_DAYS} days from {cutoff} to {today}")

    for sym in SETTINGS.symbols:
        logical = sym.upper()
        if logical not in tokens:
            print(f"[intraday] WARNING: no token for {logical}, skipping")
            continue
        refresh_symbol(logical, kite, tokens[logical], cutoff)

    print("[intraday] Done.")


if __name__ == "__main__":
    main()
