# apps/runtime/run_phase_a.py
#
# Phase A runner (paper-only):
# - Starts live 5-minute aggregator (agg5) which:
#     * consumes Kite ticks
#     * appends to data/intraday/{sym}_5minute.csv
#     * updates data/state/live_state.json with latest quotes
# - Arms the full 10-symbol portfolio at ~09:40 using the same logic
#   as GET /api/state and POST /api/plan/arm_day.
#
# This script does NOT place any real orders. It is purely for:
#   - live data plumbing
#   - automatic daily planning
#
# Execution (paper trades) and risk engine will be wired on top of this.

from __future__ import annotations

import argparse
import threading
import time
from datetime import date
from typing import Optional, Sequence

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger
from probedge.realtime.agg5 import run_agg
from apps.runtime.daily_timeline import arm_portfolio_for_day

log = get_logger(__name__)


def _start_agg_thread(symbols: Sequence[str]) -> threading.Thread:
    """Start the synchronous agg5 loop in a daemon thread."""
    def _run():
        try:
            run_agg(symbols)
        except Exception:
            log.exception("agg5 loop crashed")

    t = threading.Thread(target=_run, name="agg5-thread", daemon=True)
    t.start()
    return t


def _start_planner_thread(day: str, risk_rs: Optional[int], wait_for_time: bool) -> threading.Thread:
    """Start the 09:40 planner in a daemon thread."""
    def _run():
        try:
            arm_portfolio_for_day(day=day, risk_rs=risk_rs, wait_for_time=wait_for_time)
        except Exception:
            log.exception("arm_portfolio_for_day failed")

    t = threading.Thread(target=_run, name="planner-thread", daemon=True)
    t.start()
    return t


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase A runner: live agg5 + auto 09:40 portfolio planning (paper-only).",
    )
    parser.add_argument(
        "--day",
        type=str,
        default=None,
        help="Trading day in YYYY-MM-DD (default: today). Used for planning.",
    )
    parser.add_argument(
        "--risk",
        type=int,
        default=None,
        help="Override daily risk in rupees (default: SETTINGS / _effective_daily_risk_rs).",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="If set, do NOT wait until 09:40; build plan immediately.",
    )
    args = parser.parse_args()

    day = args.day or date.today().isoformat()
    risk_rs: Optional[int] = args.risk

    symbols = SETTINGS.symbols
    log.info("Phase A (paper) starting for day=%s symbols=%s", day, symbols)

    # 1) Start live 5-minute aggregator (blocking loop in a thread)
    agg_thread = _start_agg_thread(symbols)
    log.info("agg5 thread started: %s", agg_thread.name)

    # 2) Start portfolio planner (09:40 lock by default)
    wait_for_time = not args.no_wait
    planner_thread = _start_planner_thread(day=day, risk_rs=risk_rs, wait_for_time=wait_for_time)
    log.info(
        "planner thread started: %s (day=%s, risk_rs=%s, wait_for_time=%s)",
        planner_thread.name,
        day,
        risk_rs,
        wait_for_time,
    )

    # 3) Main thread just stays alive until Ctrl+C
    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Phase A runner interrupted by user; shutting down.")
        # Threads are daemons; process exit will stop them.


if __name__ == "__main__":
    main()
