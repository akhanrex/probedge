# apps/runtime/run_phase_a.py
#
# Phase A runner (paper-only, no real orders):
# - Live mode (default):
#     * Starts live 5-minute aggregator (agg5) which:
#         - consumes Kite ticks
#         - appends to data/intraday/{sym}_5minute.csv
#         - updates data/state/live_state.json with latest quotes
#     * Arms the full 10-symbol portfolio at ~09:40 using the same logic
#       as the API state/plan endpoints.
#     * After market close (~15:10 IST), automatically:
#         - runs paper execution from journal (simulated fills)
#         - rolls fills into daily P&L summary.
#
# - Backtest/past-day mode:
#     * With --skip-agg, --no-wait-plan, --eod-no-wait it will:
#         - build the plan immediately for --day
#         - run paper exec + daily P&L immediately
#
# This script does NOT talk to the live broker OMS. It is the
# "everything in paper" day runner: data + plan + paper exec + P&L.

from __future__ import annotations

import argparse
import threading
import time
from datetime import date, datetime, time as dtime
from typing import Optional, Sequence

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger
from probedge.realtime.agg5 import run_agg
from apps.runtime.intraday_paper import run_intraday_paper_loop
from apps.runtime.daily_timeline import arm_portfolio_for_day
from apps.runtime.paper_exec_from_journal import run_paper_exec_for_day
from apps.runtime.fills_to_daily import main as fills_to_daily_main

log = get_logger(__name__)


# -------- helpers --------

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


def _wait_until_today(target: dtime) -> None:
    """Block until today's wall-clock >= target (IST local time)."""
    while True:
        now = datetime.now()
        if now.time() >= target:
            return
        remaining = (
            datetime.combine(now.date(), target) - now
        ).total_seconds()
        sleep_for = max(5.0, min(remaining, 60.0))
        time.sleep(sleep_for)


def _start_eod_paper_thread(day: str, wait_for_close: bool) -> threading.Thread:
    """Start EOD paper execution + daily P&L in a daemon thread."""
    def _run():
        try:
            if wait_for_close:
                target = dtime(hour=15, minute=10)
                log.info("EOD thread waiting until %s for day=%s", target, day)
                _wait_until_today(target)
            else:
                log.info("EOD thread running immediately for day=%s (--eod-no-wait)", day)

            log.info("EOD paper exec starting for day=%s", day)
            run_paper_exec_for_day(day)

            log.info("EOD daily P&L aggregation starting")
            fills_to_daily_main()
            log.info("EOD paper exec + daily P&L completed for day=%s", day)
        except Exception:
            log.exception("EOD paper exec pipeline failed")

    t = threading.Thread(target=_run, name="eod-paper-thread", daemon=True)
    t.start()
    return t

def _start_intraday_paper_thread(
    planner_thread: Optional[threading.Thread],
) -> threading.Thread:
    """
    Start the intraday paper engine in a daemon thread.

    It waits for planner_thread to finish (so portfolio_plan is present
    in live_state.json), then runs run_intraday_paper_loop().
    """
    def _run():
        try:
            if planner_thread is not None:
                log.info("intraday-paper: waiting for planner to finish...")
                planner_thread.join()
                log.info("intraday-paper: planner finished; starting loop")

            run_intraday_paper_loop()
            log.info("intraday-paper: loop finished")
        except Exception:
            log.exception("intraday paper loop crashed")

    t = threading.Thread(target=_run, name="intraday-paper-thread", daemon=True)
    t.start()
    return t

# -------- entrypoint --------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Phase A runner: live agg5 + auto 09:40 portfolio planning + "
            "EOD paper execution and daily P&L (NO real orders)."
        ),
    )
    parser.add_argument(
        "--day",
        type=str,
        default=None,
        help="Trading day in YYYY-MM-DD (default: today). Used for planning and paper exec.",
    )
    parser.add_argument(
        "--risk",
        type=int,
        default=None,
        help="Override daily risk in rupees (default: SETTINGS / _effective_daily_risk_rs).",
    )
    parser.add_argument(
        "--no-wait-plan",
        action="store_true",
        help="If set, do NOT wait until 09:40; build plan immediately.",
    )
    parser.add_argument(
        "--eod-no-wait",
        action="store_true",
        help="If set, do NOT wait until 15:10; run paper exec + daily P&L immediately.",
    )
    parser.add_argument(
        "--skip-agg",
        action="store_true",
        help="If set, do NOT start live agg5 (use only for past-day / TM5 already built).",
    )
    args = parser.parse_args()

    day = args.day or date.today().isoformat()
    risk_rs: Optional[int] = args.risk

    symbols = SETTINGS.symbols
    log.info("Phase A (paper) starting for day=%s symbols=%s", day, symbols)

    # 1) Start live 5-minute aggregator unless skipped
    if args.skip_agg:
        log.info("Skipping agg5 (--skip-agg set). Assuming TM5 already present for day=%s", day)
        agg_thread = None
    else:
        agg_thread = _start_agg_thread(symbols)
        log.info("agg5 thread started: %s", agg_thread.name)

    # 2) Start portfolio planner
    wait_for_time = not args.no_wait_plan
    planner_thread = _start_planner_thread(day=day, risk_rs=risk_rs, wait_for_time=wait_for_time)
    log.info(
        "planner thread started: %s (day=%s, risk_rs=%s, wait_for_time=%s)",
        planner_thread.name,
        day,
        risk_rs,
        wait_for_time,
    )

    # 2.5) Start intraday paper engine (live intraday P&L + risk)
    intraday_thread = None
    # Only makes sense when we are behaving like a "real" day:
    # - we are not doing instant EOD
    # (we still allow --skip-agg; engine will just see no LTPs)
    if not args.eod_no_wait:
        intraday_thread = _start_intraday_paper_thread(planner_thread)
        log.info(
            "intraday paper thread started: %s (day=%s)",
            intraday_thread.name if intraday_thread else None,
            day,
        )

    # 3) Start EOD paper execution + daily P&L
    eod_thread = _start_eod_paper_thread(day=day, wait_for_close=not args.eod_no_wait)
    log.info(
        "EOD paper thread started: %s (day=%s, wait_for_close=%s)",
        eod_thread.name,
        day,
        not args.eod_no_wait,
    )


    # 4) Main thread just stays alive until Ctrl+C
    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        log.info("Phase A runner interrupted by user; shutting down.")


if __name__ == "__main__":
    main()
