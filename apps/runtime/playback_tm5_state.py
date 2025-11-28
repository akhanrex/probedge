# apps/runtime/playback_tm5_state.py
#
# Drive a "fake live" state from TM5 bars.
# - Reads TM5 for a single day (same reader as backtest).
# - Steps through DateTime in order (like playback_tm5_console).
# - On each step, writes a live_state-like JSON file with:
#   - sim_day
#   - sim_clock
#   - per-symbol LTP + OHLC + volume.
#
# Later the WS server / web terminal can read this JSON and render it.

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.backtest.exec_adapter import _read_tm5


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Playback TM5 data and write fake-live state JSON."
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
    day = pd.to_datetime(day_str).normalize()
    symbols = SETTINGS.symbols

    tm5_by_sym: dict[str, pd.DataFrame] = {}

    for sym in symbols:
        intraday_pattern = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
        path = intraday_pattern.format(sym=sym)

        try:
            df = _read_tm5(path)
        except FileNotFoundError:
            print(f"[WARN] No TM5 file for {sym} at {path}, skipping.")
            continue

        df_day = df[df["Date"] == day].copy()
        if df_day.empty:
            print(f"[WARN] No TM5 rows for {sym} on {day.date()}, skipping.")
            continue

        # Restrict to normal session (09:15–15:20).
        mask_session = (df_day["_mins"] >= (9 * 60 + 15)) & (
            df_day["_mins"] <= (15 * 60 + 20)
        )
        df_day = df_day.loc[mask_session].sort_values("DateTime").reset_index(drop=True)

        if df_day.empty:
            print(f"[WARN] No session bars for {sym} on {day.date()}, skipping.")
            continue

        tm5_by_sym[sym] = df_day

    return tm5_by_sym


def build_time_axis(tm5_by_sym: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    all_ts: list[pd.Timestamp] = []
    for df in tm5_by_sym.values():
        all_ts.extend(list(df["DateTime"].unique()))
    if not all_ts:
        return []
    return sorted(pd.Series(all_ts).drop_duplicates())


def write_state_json(
    path: Path,
    *,
    day_str: str,
    ts,
    tm5_by_sym: dict[str, pd.DataFrame],
) -> None:
    """
    Minimal state JSON that the WS / UI can use:
    - sim_day: YYYY-MM-DD
    - sim_clock: iso timestamp of playback
    - symbols: { sym: { ltp, ohlc, volume } }
    """
    state = {
        "mode": SETTINGS.mode,  # "paper" / "live" / whatever you configured
        "sim": True,
        "sim_day": day_str,
        "sim_clock": pd.to_datetime(ts).isoformat(),
        "symbols": {},
    }

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

        state["symbols"][sym] = {
            "ltp": c,
            "ohlc": {"o": o, "h": h, "l": l, "c": c},
            "volume": v,
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp_path.replace(path)


def playback_to_state(day_str: str, speed: float) -> None:
    tm5_by_sym = load_tm5_for_day(day_str)
    if not tm5_by_sym:
        print(f"[ERROR] No TM5 data for any symbol on {day_str}. Aborting playback.")
        return

    times = build_time_axis(tm5_by_sym)
    if not times:
        print(f"[ERROR] No DateTime axis built for {day_str}. Aborting playback.")
        return

    state_path = Path(SETTINGS.paths.state or "data/state/live_state.json")

    print(f"\n=== Fake-live playback for {day_str} ===")
    print(f"Writing state to: {state_path}")
    print(f"Symbols: {', '.join(sorted(tm5_by_sym.keys()))}")
    print(
        f"Bars in union time-axis: {len(times)} | "
        f"Speed: {speed}x (5 min -> {300.0 / speed:.1f} sec)\n"
    )

    prev_ts = None

    for ts in times:
        # sleep according to speed
        if prev_ts is not None and speed > 0:
            delta_sec = (ts - prev_ts).total_seconds()
            sleep_sec = max(0.0, delta_sec / speed)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        prev_ts = ts

        # log to console (optional)
        print(f"[STATE] {ts} – updating live_state.json")

        # write JSON snapshot
        write_state_json(state_path, day_str=day_str, ts=ts, tm5_by_sym=tm5_by_sym)

    print("\n=== Fake-live playback complete ===")


def main() -> None:
    args = parse_args()
    playback_to_state(args.day, args.speed)


if __name__ == "__main__":
    main()
