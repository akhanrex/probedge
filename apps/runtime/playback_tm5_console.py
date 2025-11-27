# apps/runtime/playback_tm5_console.py

from __future__ import annotations

import argparse
import time
from datetime import datetime

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.backtest.exec_adapter import _read_tm5, T0_M, T1_M


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Playback 5-minute TM5 data for a given day, console style."
    )
    p.add_argument(
        "--day",
        required=True,
        help="Trading day in YYYY-MM-DD (must exist in data/intraday/*_5minute.csv).",
    )
    p.add_argument(
        "--speed",
        type=float,
        default=10.0,
        help=(
            "Playback speed factor: 1.0 = real-time (5 min bar every 5 min), "
            "10.0 = 10x faster (5 min bar every 30 seconds), etc."
        ),
    )
    return p.parse_args()


def load_tm5_for_day(day_str: str) -> dict[str, pd.DataFrame]:
    """
    For the configured symbol universe, load TM5 for a single day into memory.
    Uses the same reader as the backtest (Colab parity).
    """
    day = pd.to_datetime(day_str).normalize()

    # Use the same symbols the system trades
    symbols = SETTINGS.symbols
    tm5_by_sym: dict[str, pd.DataFrame] = {}

    for sym in symbols:
        try:
            df = _read_tm5(SETTINGS.paths.intraday.format(sym=sym))
        except FileNotFoundError:
            print(f"[WARN] No TM5 file for {sym}, skipping.")
            continue

        df_day = df[df["Date"] == day].copy()

        if df_day.empty:
            print(f"[WARN] No TM5 rows for {sym} on {day.date()}, skipping.")
            continue

        # For playback we can include the whole day, but it's usually
        # enough to restrict to the 09:15 -> 15:20 session.
        # We already have _mins from the reader.
        mask_session = (df_day["_mins"] >= (9 * 60 + 15)) & (df_day["_mins"] <= (15 * 60 + 20))
        df_day = df_day.loc[mask_session].sort_values("DateTime").reset_index(drop=True)

        if df_day.empty:
            print(f"[WARN] No session bars for {sym} on {day.date()}, skipping.")
            continue

        tm5_by_sym[sym] = df_day

    return tm5_by_sym


def build_time_axis(tm5_by_sym: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    """
    Union of all DateTime values across symbols, sorted.
    """
    all_ts: list[pd.Timestamp] = []
    for df in tm5_by_sym.values():
        all_ts.extend(list(df["DateTime"].unique()))
    # Drop duplicates and sort
    uniq = sorted(pd.Series(all_ts).drop_duplicates())
    return uniq


def playback_day(day_str: str, speed: float) -> None:
    tm5_by_sym = load_tm5_for_day(day_str)
    if not tm5_by_sym:
        print(f"[ERROR] No TM5 data for any symbol on {day_str}. Aborting playback.")
        return

    times = build_time_axis(tm5_by_sym)
    if not times:
        print(f"[ERROR] No DateTime axis built for {day_str}. Aborting playback.")
        return

    print(f"\n=== Playback for {day_str} ===")
    print(f"Symbols: {', '.join(sorted(tm5_by_sym.keys()))}")
    print(f"Bars in union time-axis: {len(times)}")
    print(f"Speed: {speed}x (5 min market time -> {300.0/speed:.1f} sec wall-clock)\n")

    prev_ts: datetime | None = None

    for ts in times:
        # Sleep proportionally to elapsed market minutes
        if prev_ts is not None and speed > 0:
            delta_sec = (ts - prev_ts).total_seconds()
            sleep_sec = max(0.0, delta_sec / speed)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        prev_ts = ts

        # Header for this "tick"
        print(f"\n===== {ts.strftime('%Y-%m-%d %H:%M')} =====")

        # For each symbol, if it has a bar at this time, print OHLC + volume
        for sym, df in tm5_by_sym.items():
            row = df[df["DateTime"] == ts]
            if row.empty:
                continue
            r = row.iloc[0]
            o = float(r["Open"])
            h = float(r["High"])
            l = float(r["Low"])
            c = float(r["Close"])
            v = float(r["Volume"]) if "Volume" in r else float("nan")
            print(
                f"{sym:<10s}  O={o:8.2f}  H={h:8.2f}  L={l:8.2f}  C={c:8.2f}  Vol={v:10.0f}"
            )

    print("\n=== Playback complete ===")


def main() -> None:
    args = parse_args()
    playback_day(args.day, args.speed)


if __name__ == "__main__":
    main()
