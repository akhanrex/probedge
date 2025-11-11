#!/usr/bin/env python3
import os, sys, pandas as pd
from datetime import datetime, timedelta
from infra.config import SYMBOLS
from storage import tm5min_store
from storage.master_store import update_master_tags
from decision.tags_engine import compute_tags_5

try:
    # Implement this in realtime/feed_router.py (optional). If absent, we skip.
    from realtime.feed_router import get_historical_5min
except Exception:
    get_historical_5min = None

def _date_bounds_ist(d: datetime):
    start = d.replace(hour=9, minute=15, second=0, microsecond=0)
    end   = d.replace(hour=15, minute=30, second=0, microsecond=0)
    return int(start.timestamp()), int(end.timestamp())

def backfill_day(symbols, day: datetime):
    if get_historical_5min is None:
        print("[warn] get_historical_5min not wired; skipping backfill.")
        return
    t0, t1 = _date_bounds_ist(day)
    for sym in symbols:
        print(f"[backfill] {sym} {day.date()}")
        bars = get_historical_5min(sym, t0, t1)  # list of dicts with end_ts, Open, High, Low, Close
        if not bars:
            print("  no bars")
            continue
        for b in bars:
            tm5min_store.append_bar(sym, b)

        df = pd.DataFrame(bars)
        df["DateTime"] = pd.to_datetime(df["end_ts"], unit="s")
        tags_raw = compute_tags_5(df)
        tag_map = {
            "PDC": "PDC_R",
            "OL": "OL",
            "OT": "OT_R",
            "FirstCandleType": "FIRST_CANDLE",
            "RangeStatus": "RANGE_STATUS",
        }
        tags = { tag_map[k]: v for k, v in tags_raw.items() if k in tag_map }
        ok = update_master_tags(sym, day, tags)
        print(f"  master {'updated' if ok else 'missing'} -> {tags}")

if __name__ == "__main__":
    today = datetime.now()
    prev = today - timedelta(days=1)
    while prev.weekday() >= 5:  # step back off Sat/Sun
        prev -= timedelta(days=1)
    syms = SYMBOLS
    if len(sys.argv) > 1:
        syms = [s.strip().upper() for s in sys.argv[1].split(",") if s.strip()]
    backfill_day(syms, prev)
