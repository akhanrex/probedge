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

TOKENS_FILE = Path("data/config/kite_tokens.json")
OUT_ROOT = Path("data/hist_1m")  # 1-minute raw store


def load_symbol_tokens() -> Dict[str, int]:
    if not TOKENS_FILE.exists():
        raise RuntimeError(
            f"Missing {TOKENS_FILE}. Create symbol -> instrument_token mapping first."
        )
    raw = json.loads(TOKENS_FILE.read_text())
    out: Dict[str, int] = {}
    for sym, tok in raw.items():
        out[str(sym).upper()] = int(tok)
    return out


def daterange(start: date, end: date) -> List[date]:
    # inclusive [start, end]
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


def fetch_day_1m(kite: KiteConnect, sym: str, token: int, day: date) -> pd.DataFrame:
    # Kite expects datetime range; we’ll pull 09:00–16:00 IST to be safe
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
    # Kite returns column 'date', 'open','high','low','close','volume','oi'
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

    symbol_to_token = load_symbol_tokens()
    log.info("[hist_1m] symbols: %s", list(symbol_to_token.keys()))

    # ---- EDIT THESE DATES FOR THE RANGE YOU WANT ----
    start_day = date(2025, 8, 1)   # e.g. 1st Aug
    end_day   = date(2025, 9, 30)  # e.g. 30th Sep
    # -------------------------------------------------

    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    for d in daterange(start_day, end_day):
        # Skip weekends quickly
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
