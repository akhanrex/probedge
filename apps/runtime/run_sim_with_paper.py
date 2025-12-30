from __future__ import annotations

import argparse
import threading
import time
from datetime import datetime, time as dtime
from typing import Dict, List


import pandas as pd

from probedge.infra.logger import get_logger
from probedge.infra.settings import SETTINGS
from apps.runtime.daily_timeline import arm_portfolio_for_day
from apps.runtime.intraday_paper import run_intraday_paper_loop

# Reuse the SIM helpers so quotes + metadata are identical
from apps.runtime.run_sim_from_intraday import (
    _ensure_state_base,
    _load_intraday_for_day,
    _write_bar,
)

T_PLAN = dtime(9, 40, 0)  # SIM uses 09:40 bar as proxy for 09:39:50

log = get_logger(__name__)


def _start_intraday_paper_thread() -> threading.Thread:
    """
    Start the intraday paper engine in a daemon thread.

    Same engine as live Phase A:
      - Reads portfolio_plan from live_state.json
      - Watches quotes (ltp/ohlc) and sim_clock
      - Updates positions + P&L + risk into live_state.json
    """
    def _run():
        try:
            log.info("SIM+PAPER: starting intraday paper loop...")
            run_intraday_paper_loop()
            log.info("SIM+PAPER: intraday paper loop finished")
        except Exception:
            log.exception("SIM+PAPER: intraday paper loop crashed")

    t = threading.Thread(target=_run, name="sim-intraday-paper", daemon=True)
    t.start()
    return t

def run_sim_with_paper(sim_day: str, speed: float) -> None:
    """
    Full SIM for a single trading day, including P&L, with live-like timing:

      - 09:15–<09:40: only quotes stream, no plan, no P&L.
      - At first bar with time >= T_PLAN (09:40 in TM5):
          * build portfolio_plan via arm_portfolio_for_day
          * start intraday paper engine (positions + P&L)
      - Continue streaming quotes until end of day.
    """
    log.info("SIM+PAPER: starting for day=%s, speed=%sx", sim_day, speed)

    # 0) Base SIM shell in live_state.json
    _ensure_state_base(sim_day)

    # 1) Load TM5 intraday frames for this day
    frames = _load_intraday_for_day(sim_day)
    if not frames:
        log.error("SIM+PAPER: no intraday data for any symbols on %s", sim_day)
        return

    # 2) Build union time axis across all symbols
    all_times: List[datetime] = []
    for df in frames.values():
        all_times.extend(df["DateTime"].tolist())

    times_sorted = sorted(set(all_times))
    log.info(
        "SIM+PAPER: starting intraday replay for %s, %s bars, speed=%sx",
        sim_day,
        len(times_sorted),
        speed,
    )

    planned = False
    paper_started = False
    paper_thread: threading.Thread | None = None

    prev_ts: datetime | None = None
    for ts in times_sorted:
        # Simulated delay between bars
        if prev_ts is None:
            delay = 0.0
        else:
            delta_secs = (ts - prev_ts).total_seconds()
            delay = max(0.0, delta_secs / max(speed, 1e-6))
        prev_ts = ts

        # When we hit the 09:40 bar (proxy for 09:39:50), build plan once
        if (not planned) and ts.time() >= T_PLAN:
            log.info(
                "SIM+PAPER: time %s >= plan cut %s; building portfolio plan",
                ts.time(),
                T_PLAN,
            )
            arm_portfolio_for_day(day=sim_day, risk_rs=None, wait_for_time=False)
            planned = True

            # Start intraday paper engine AFTER plan exists
            paper_thread = _start_intraday_paper_thread()
            paper_started = True

        # Sleep for compressed real-time effect
        if delay > 0:
            time.sleep(delay)

        # Fan out this timestamp across all symbols → quotes
        for sym, df_day in frames.items():
            row = df_day[df_day["DateTime"] == ts]
            if row.empty:
                continue
            bar = row.iloc[0]
            _write_bar(sym, bar, sim_day)

    log.info("SIM+PAPER: finished streaming intraday for %s", sim_day)

    # Give intraday paper a moment to settle final writes (optional)
    if paper_started and paper_thread is not None:
        try:
            paper_thread.join(timeout=5.0)
        except Exception:
            pass

    log.info("SIM+PAPER: done for day=%s", sim_day)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay TM5 intraday for a day AND run intraday paper (P&L SIM)."
    )
    parser.add_argument(
        "--day",
        type=str,
        required=True,
        help="Trading day YYYY-MM-DD to simulate",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=60.0,
        help="Speed multiplier vs real-time (e.g. 60 = 1 hour in 1 minute)",
    )
    args = parser.parse_args()

    run_sim_with_paper(args.day, args.speed)


if __name__ == "__main__":
    main()
