# apps/runtime/rebuild_intraday_5min_from_kite.py

from __future__ import annotations

import csv
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List

import pandas as pd
from kiteconnect import KiteConnect

from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import intraday_path
import yaml


LOOKBACK_DAYS = 180
SESSION_START = "09:15:00"
SESSION_END   = "15:05:00"   # last bar to keep


def load_alias_map() -> Dict[str, str]:
    """
    Use config/symbol_map.yaml to map our alias (TATAMOTORS) -> real Kite symbol (TMPV).
    If not present, fall back to identity.
    """
    sym_yaml = Path("config/symbol_map.yaml")
    if not sym_yaml.exists():
        return {}

    raw = yaml.safe_load(sym_yaml.read_text()) or {}
    aliases = raw.get("aliases", {}) or {}
    # aliases in yaml are legacy->real; we want that directly
    return {k.upper(): v.upper() for k, v in aliases.items()}


def load_tokens() -> Dict[str, int]:
    """
    Load instrument tokens from data/tokens.csv.
    We expect at least these columns:
      - tradingsymbol (or symbol)
      - instrument_token
    """
    tokens_path = Path("data/tokens.csv")
    if not tokens_path.exists():
        raise SystemExit("ERROR: data/tokens.csv not found. Please create it with tradingsymbol + instrument_token for the 10 symbols.")

    mapping: Dict[str, int] = {}
    with tokens_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = (row.get("tradingsymbol") or row.get("symbol") or "").upper().strip()
            tok = row.get("instrument_token")
            if not sym or not tok:
                continue
            try:
                mapping[sym] = int(tok)
            except ValueError:
                continue
    if not mapping:
        raise SystemExit("ERROR: No valid rows in data/tokens.csv (check columns tradingsymbol / instrument_token).")
    return mapping


def get_kite_client() -> KiteConnect:
    """
    Build a Kite client using api_key from SETTINGS and access_token from the session file.
    """
    if not SETTINGS.kite_api_key:
        raise SystemExit("ERROR: SETTINGS.kite_api_key is empty. Check your .env.")

    sess_file = SETTINGS.kite_session_file
    if not sess_file or not Path(sess_file).exists():
        raise SystemExit(f"ERROR: kite_session_file not found: {sess_file}")

    import json
    session_data = json.loads(Path(sess_file).read_text())
    access_token = session_data.get("access_token")
    if not access_token:
        raise SystemExit("ERROR: No access_token in kite_session_file. Please login via /login once first.")

    kite = KiteConnect(api_key=SETTINGS.kite_api_key)
    kite.set_access_token(access_token)
    return kite


def fetch_5min_from_kite(kite: KiteConnect, instrument_token: int, start: date, end: date) -> pd.DataFrame:
    """
    Fetch 5-minute candles between start and end (inclusive) using Kite historical_data.
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
    # Kite columns: date, open, high, low, close, volume, oi
    # Ensure 'date' is datetime
    df["date"] = pd.to_datetime(df["date"])

    # Filter by session time (09:15 - 15:05)
    df["time_str"] = df["date"].dt.strftime("%H:%M:%S")
    mask = (df["time_str"] >= SESSION_START) & (df["time_str"] <= SESSION_END)
    df = df.loc[mask].copy()

    # Split into date + time cols (if your intraday uses that style)
    df["trade_date"] = df["date"].dt.date
    df["trade_time"] = df["date"].dt.strftime("%H:%M:%S")

    # Final ordering
    df = df.sort_values("date")

    # Rename columns to something close to your intraday schema.
    # Adjust these if your intraday CSV uses different names.
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


def reload_symbol(kite: KiteConnect, alias: str, real_sym: str, token: int) -> None:
    """
    For a single symbol:
      - Load existing intraday CSV
      - Keep rows older than cutoff
      - Drop last LOOKBACK_DAYS
      - Fetch fresh 5-min for last LOOKBACK_DAYS
      - Append and write back to same file
    """
    path = intraday_path(alias)   # respects alias -> TMPV mapping
    print(f"[{alias}] intraday path = {path}")

    cutoff_date = date.today() - timedelta(days=LOOKBACK_DAYS)
    cutoff_str = cutoff_date.isoformat()

    if path.exists():
        df_old = pd.read_csv(path)
        # Try to detect date column
        if "date" in df_old.columns:
            # assume YYYY-MM-DD
            df_old["date"] = pd.to_datetime(df_old["date"]).dt.date
            df_keep = df_old[df_old["date"] < cutoff_date].copy()
        elif "ts" in df_old.columns:
            df_old["ts"] = pd.to_datetime(df_old["ts"])
            df_old["date"] = df_old["ts"].dt.date
            df_keep = df_old[df_old["date"] < cutoff_date].copy()
            df_keep = df_keep.drop(columns=["date"])
        else:
            raise SystemExit(f"[{alias}] Unknown intraday schema (no 'date' or 'ts' column). Please adjust script.")
    else:
        print(f"[{alias}] No existing intraday file; starting fresh for last {LOOKBACK_DAYS} days.")
        df_keep = pd.DataFrame()

    start_date = cutoff_date
    end_date = date.today()  # inclusive

    print(f"[{alias}] Fetching 5-min from Kite for {start_date} .. {end_date} using token {token}")
    df_new = fetch_5min_from_kite(kite, token, start_date, end_date)

    if df_new.empty:
        print(f"[{alias}] WARNING: No new 5-min data from Kite.")
    else:
        print(f"[{alias}] Fetched {len(df_new)} fresh 5-min bars.")

    # Combine: old (< cutoff) + new (>= cutoff)
    if not df_keep.empty:
        df_combined = pd.concat([df_keep, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Sort + dedupe by (date, time)
    if "date" in df_combined.columns and "time" in df_combined.columns:
        df_combined = df_combined.sort_values(["date", "time"])
        df_combined = df_combined.drop_duplicates(subset=["date", "time"], keep="last")
    elif "ts" in df_combined.columns:
        df_combined = df_combined.sort_values("ts")
        df_combined = df_combined.drop_duplicates(subset=["ts"], keep="last")

    # Write back
    path.parent.mkdir(parents=True, exist_ok=True)
    df_combined.to_csv(path, index=False)
    print(f"[{alias}] Wrote cleaned intraday to {path} (rows={len(df_combined)})")


def main():
    alias_map = load_alias_map()
    tokens_map = load_tokens()
    kite = get_kite_client()

    print("[rebuild_intraday] MODE:", SETTINGS.mode)
    print("[rebuild_intraday] Symbols:", SETTINGS.symbols)
    print("[rebuild_intraday] Lookback days:", LOOKBACK_DAYS)

    for alias in SETTINGS.symbols:
        alias_u = alias.upper()
        real_sym = alias_map.get(alias_u, alias_u)  # e.g., TATAMOTORS -> TMPV
        token = tokens_map.get(real_sym)
        if token is None:
            print(f"[{alias_u}] SKIP – no instrument_token for {real_sym} in data/tokens.csv")
            continue

        reload_symbol(kite, alias_u, real_sym, token)


if __name__ == "__main__":
    main()
