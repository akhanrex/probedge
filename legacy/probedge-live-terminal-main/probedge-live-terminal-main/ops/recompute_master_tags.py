#!/usr/bin/env python3
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
import pandas as pd

from storage.master_store import read_tm5, update_master_tags, most_recent_day
from decision.tags_engine import compute_tags_5
from infra.config import SYMBOLS

def run(symbols):
    for sym in symbols:
        df = read_tm5(sym)
        if df is None or df.empty:
            print(f"[skip] {sym}: no tm5 data")
            continue
        # recompute for LAST DAY ONLY (fast). Expand as needed.
        last_day = most_recent_day(df)
        if last_day is None:
            print(f"[skip] {sym}: no valid dates")
            continue
        # Pass full intraday df; engine derives prev-day internally
        tags = compute_tags_5(df)
        ok = update_master_tags(sym, last_day, tags)
        print(f"[{sym}] {last_day.date()} -> {tags} | {'updated' if ok else 'no-master'}")

if __name__ == "__main__":
    syms = SYMBOLS
    if len(sys.argv) > 1:
        syms = [s.strip().upper() for s in sys.argv[1].split(",") if s.strip()]
    run(syms)
