# apps/runtime/rebuild_masters_5min.py

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import master_path, intraday_path
from probedge.core.classifiers import build_master_for_symbol  # adjust if name slightly different


LOOKBACK_DAYS = 180


def main():
    today = date.today()
    cutoff = today - timedelta(days=LOOKBACK_DAYS)

    print("[rebuild_masters] MODE:", SETTINGS.mode)
    print("[rebuild_masters] Symbols:", SETTINGS.symbols)
    print("[rebuild_masters] Lookback days:", LOOKBACK_DAYS)

    for sym in SETTINGS.symbols:
        print(f"[{sym}] Rebuilding master for last {LOOKBACK_DAYS} days...")

        intraday_file = intraday_path(sym)
        master_file = master_path(sym)

        if not intraday_file.exists():
            print(f"[{sym}] SKIP – intraday file missing: {intraday_file}")
            continue

        # Load existing master (if any) and keep rows < cutoff
        if master_file.exists():
            df_master_old = pd.read_csv(master_file)
            if "date" in df_master_old.columns:
                df_master_old["date"] = pd.to_datetime(df_master_old["date"]).dt.date
                df_master_keep = df_master_old[df_master_old["date"] < cutoff].copy()
            else:
                df_master_keep = pd.DataFrame()
        else:
            df_master_keep = pd.DataFrame()

        # Build new master slice for last N days from intraday
        # build_master_for_symbol should encapsulate your existing logic:
        # reading intraday, computing indicators, returning a DataFrame with same schema as master.
        df_master_new = build_master_for_symbol(
            symbol=sym,
            intraday_path=intraday_file,
            start_date=cutoff,
            end_date=today,
        )

        # Combine old + new
        if not df_master_keep.empty:
            df_master = pd.concat([df_master_keep, df_master_new], ignore_index=True)
        else:
            df_master = df_master_new

        # Sort + dedupe on date
        if "date" in df_master.columns:
            df_master["date"] = pd.to_datetime(df_master["date"]).dt.date
            df_master = df_master.sort_values("date")
            df_master = df_master.drop_duplicates(subset=["date"], keep="last")

        master_file.parent.mkdir(parents=True, exist_ok=True)
        df_master.to_csv(master_file, index=False)
        print(f"[{sym}] Master written to {master_file} (rows={len(df_master)})")


if __name__ == "__main__":
    main()
