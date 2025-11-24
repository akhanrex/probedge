# apps/runtime/kite_hist_1m_fetch.py

from __future__ import annotations

import json
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List

import pandas as pd
from kiteconnect import KiteConnect

from probedge.broker.kite_session import load_session
from probedge.infra.logger import get_logger

log = get_logger(__name__)

# Optional: we’ll write a fresh token map here for visibility
TOKENS_FILE = Path("data/config/kite_tokens.json")
OUT_ROOT = Path("data/hist_1m")  # 1-minute raw store

# Our 10-symbol universe (API-level names)
SYMBOLS: List[str] = [
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


def build_token_map(kite: KiteConnect, symbols: List[str]) -> Dict[str, int]:
    """
    Build symbol -> instrument_token from live NSE instruments.

    Handles legacy names (e.g. old TATAMOTORS -> new TMPV) via ALIASES.
    """
    log.info("[hist_1m] fetching NSE instruments to build token map")
    instruments = kite.instruments("NSE")
    by_symbol = {row["tradingsymbol"].upper(): row["instrument_token"] for row in instruments}

    # Legacy → current mappings.
    # UI / API will always use TMPV; this is only to tolerate old names if they appear.
    ALIASES = {
        "TATAMOTORS": "TMPV",
    }

    out: Dict[str, int] = {}
    for sym in symbols:
        base = sym.upper()
        lookup = ALIASES.get(base, base)  # TMPV stays TMPV
        if lookup not in by_symbol:
            raise RuntimeError(f"No NSE instrument found for {sym} (lookup={lookup})")
        out[base] = by_symbol[lookup]

    log.info("[hist_1m] token map: %s", out)

    # Write a fresh tokens file so we know what is being used
    TOKENS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKENS_FILE.write_text(json.dumps(out, indent=2, sort_keys=True))
    log.info("[hist_1m] wrote fresh token map to %s", TOKENS_FILE)

    return out


def daterange(start: date, end: date) -> List[date]:
    # inclusive [start, end]
    days: List[date] = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def fetch_day_1m(kite: KiteConnect, sym: str, token: int, day: date) -> pd.DataFrame:
    # Kite expects datetime range; pull 09:00–16:00 IST to be safe
    dt_start = datetime(day.year, day.month, day.day, 9, 0)
    dt_end = datetime(day.year, day.month, day.day, 16, 0)

    log.info("[hist_1m] fetching %s 1m for %s", sym, day)
    data = kite.historical_data(
        instrument_token=token,
        from_date=dt_start,
        to_date=dt_end,
        interval="minute",
        continuous=False,
        oi=True,
    )
    if not data:
        log.warning("[hist_1m] no data for %s on %s", sym, day)
        return pd.DataFrame()

    df = pd.DataFrame(data)
    # Kite returns: date, open, high, low, close, volume, oi
    df.rename(columns={"date": "DateTime"}, inplace=True)
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    df = df.dropna(subset=["DateTime"]).sort_values("DateTime").reset_index(drop=True)
    return df


def main():
    sess = load_session()
    if not sess or "access_token" not in sess or "api_key" not in sess:
        raise RuntimeError("No Kite session on disk; run kite_auth_cli first")

    kite = KiteConnect(api_key=sess["api_key"])
    kite.set_access_token(sess["access_token"])

    # Build fresh tokens from Kite (ignoring any old invalid tokens)
    symbol_to_token = build_token_map(kite, SYMBOLS)
    log.info("[hist_1m] symbols: %s", list(symbol_to_token.keys()))

    # ---- EDIT THESE DATES FOR THE RANGE YOU WANT ----
    start_day = date(2025, 8, 1)   # e.g. 1st Aug 2025
    end_day   = date(2025, 9, 30)  # e.g. 30th Sep 2025
    # -------------------------------------------------

    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    for d in daterange(start_day, end_day):
        # Skip weekends
        if d.weekday() >= 5:
            continue

        day_dir = OUT_ROOT / d.strftime("%Y-%m-%d")
        day_dir.mkdir(parents=True, exist_ok=True)

        for sym, tok in symbol_to_token.items():
            out_path = day_dir / f"{sym}_1minute.csv"
            if out_path.exists():
                log.info("[hist_1m] already have %s for %s; skipping", sym, d)
                continue

            df = fetch_day_1m(kite, sym, tok, d)
            if df.empty:
                continue

            df.to_csv(out_path, index=False)
            log.info("[hist_1m] saved %s rows to %s", len(df), out_path)


if __name__ == "__main__":
    main()
