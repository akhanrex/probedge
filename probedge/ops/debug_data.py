# probedge/ops/debug_data.py

from __future__ import annotations
from pathlib import Path

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.decision.picker_batchv1 import read_tm5


# === CONFIG: set the day you want to debug ===
DAY_STR = "2025-08-06"  # change this freely, e.g. "2025-11-11"


def main() -> None:
    day = pd.to_datetime(DAY_STR).normalize()
    print("=== SETTINGS ===")
    print("MODE:", SETTINGS.mode)
    print("SYMBOLS:", SETTINGS.symbols)
    print("paths.intraday:", getattr(SETTINGS.paths, "intraday", None))
    print("paths.masters:", getattr(SETTINGS.paths, "masters", None))
    print("paths.state:", getattr(SETTINGS.paths, "state", None))
    print("DEBUG_DAY:", day.date())
    print()

    for sym in SETTINGS.symbols:
        print("=" * 60)
        print(f"SYMBOL: {sym}")

        tm5_path = SETTINGS.paths.intraday.format(sym=sym)
        master_path = SETTINGS.paths.masters.format(sym=sym)

        # --- Intraday (TM5) ---
        p_tm5 = Path(tm5_path)
        if not p_tm5.exists():
            print(f"TM5:   {p_tm5}  --> MISSING FILE")
        else:
            print(f"TM5:   {p_tm5}  --> EXISTS  (size={p_tm5.stat().st_size} bytes)")
            try:
                df_tm5 = read_tm5(str(p_tm5))
            except Exception as e:
                print(f"       ERROR reading TM5: {e}")
            else:
                if df_tm5.empty:
                    print("       TM5: EMPTY dataframe")
                else:
                    # expect a Date column after read_tm5
                    if "Date" not in df_tm5.columns:
                        print("       TM5: MISSING 'Date' column after parse")
                    else:
                        dmin = df_tm5["Date"].min()
                        dmax = df_tm5["Date"].max()
                        print(f"       TM5 Date range: {dmin.date()} -> {dmax.date()}")
                        n_day = (df_tm5["Date"] == day).sum()
                        print(f"       TM5 bars on {day.date()}: {n_day}")

                        # small sample if there are any rows that day
                        if n_day > 0:
                            sample = df_tm5[df_tm5["Date"] == day].head(3)
                            print("       Sample TM5 rows for that day:")
                            print(sample[["DateTime", "Open", "High", "Low", "Close"]].to_string(index=False))

        # --- Master ---
        p_master = Path(master_path)
        if not p_master.exists():
            print(f"MASTER: {p_master}  --> MISSING FILE")
        else:
            print(f"MASTER:{p_master}  --> EXISTS  (size={p_master.stat().st_size} bytes)")
            try:
                m = pd.read_csv(p_master)
            except Exception as e:
                print(f"       ERROR reading MASTER: {e}")
            else:
                if m.empty:
                    print("       MASTER: EMPTY dataframe")
                else:
                    if "Date" not in m.columns:
                        print("       MASTER: MISSING 'Date' column")
                    else:
                        m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
                        dmin = m["Date"].min()
                        dmax = m["Date"].max()
                        print(f"       MASTER Date range: {dmin.date()} -> {dmax.date()}")
                        n_day = (m["Date"] == day).sum()
                        print(f"       MASTER rows on {day.date()}: {n_day}")
                        if n_day > 0:
                            sample = m[m["Date"] == day].head(3)
                            print("       Sample MASTER rows for that day:")
                            print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
