from __future__ import annotations
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect

from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import intraday_path

TOKENS_PATH = Path("data/tokens_5min.csv")
# how many calendar days of intraday we refresh
N_DAYS = 180

SESSION_START = "09:15:00"   # keep full trading session from here


def make_kite() -> KiteConnect:
    api_key = SETTINGS.kite_api_key
    if not api_key:
        raise RuntimeError("KITE_API_KEY not configured in .env")

    sess_file = SETTINGS.kite_session_file
    if not sess_file or not sess_file.exists():
        raise RuntimeError(f"Kite session file not found: {sess_file}")

    with sess_file.open("r", encoding="utf-8") as f:
        sess = json.load(f)

    access_token = sess.get("access_token")
    if not access_token:
        raise RuntimeError("Kite session file has no access_token")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def load_tokens():
    if not TOKENS_PATH.exists():
        raise FileNotFoundError(f"Missing {TOKENS_PATH}. Run build_tokens_5min first.")
    df = pd.read_csv(TOKENS_PATH)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return {row["symbol"]: int(row["instrument_token"]) for _, row in df.iterrows()}


def fetch_5min_from_kite(kite: KiteConnect, instrument_token: int, start: date, end: date) -> pd.DataFrame:
    """
    Fetch 5-minute candles between start and end (inclusive) using Kite historical_data.
    We keep the **full trading session** for each day, starting from 09:15 onwards.
    """
    from_dt = datetime.combine(start, datetime.min.time())
    to_dt   = datetime.combine(end,   datetime.max.time())

    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval="5minute"
    )
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles)
    df["date"] = pd.to_datetime(df["date"])

    # Drop any pre-market junk before 09:15
    df["time_str"] = df["date"].dt.strftime("%H:%M:%S")
    df = df[df["time_str"] >= SESSION_START].copy()

    df["trade_date"] = df["date"].dt.date
    df["trade_time"] = df["date"].dt.strftime("%H:%M:%S")

    df = df.sort_values("date")

    df = df.rename(columns={
        "trade_date": "date",
        "trade_time": "time",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    })

    return df[["date", "time", "open", "high", "low", "close", "volume"]]


def refresh_symbol(sym: str, kite: KiteConnect, instrument_token: int, cutoff: date):
    path = intraday_path(sym)
    print(f"[intraday] {sym}: file {path}")

    # Load existing, if any
    if path.exists():
        cur = pd.read_csv(path)
        cols_lower = {c.lower(): c for c in cur.columns}

        if "date" in cols_lower:
            # Use whatever column is effectively "date" (case-insensitive)
            date_col = cols_lower["date"]
            cur["date"] = pd.to_datetime(cur[date_col]).dt.date
            # keep history strictly before cutoff
            keep = cur[cur["date"] < cutoff].copy()
            print(f"[intraday] {sym}: keeping {len(keep)} old rows (< {cutoff})")
        else:
            # Old file is too messy – treat as if we had nothing
            print(f"[intraday] {sym}: WARNING no 'date' column in {path}, starting fresh")
            keep = pd.DataFrame(columns=["date", "time", "open", "high", "low", "close", "volume"])
    else:
        keep = pd.DataFrame(columns=["date", "time", "open", "high", "low", "close", "volume"])
        print(f"[intraday] {sym}: no existing file, starting fresh")


    today = date.today()
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

    today = date.today()
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
