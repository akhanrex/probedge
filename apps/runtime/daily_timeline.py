# apps/runtime/daily_timeline.py

from __future__ import annotations
from probedge.journal.writer import append_portfolio_plan
import argparse
from datetime import date, datetime, time as dtime
from typing import Optional

from probedge.infra.logger import get_logger
from probedge.storage.atomic_json import AtomicJSON
from probedge.infra.settings import SETTINGS
from probedge.decision.portfolio_planner import (
    build_portfolio_state_for_day,
)

log = get_logger(__name__)

STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)

T_PLAN = dtime(9, 40)  # 09:40 plan lock time (IST)


def wait_until(target: dtime) -> None:
    """
    Simple blocking wait until today's time >= target.
    For now: naive system-time check, good enough on your Mac.
    """
    import time as _time

    while True:
        now = datetime.now().time()
        if now >= target:
            return
        # sleep in small chunks to not spin CPU
        _time.sleep(5)


def arm_portfolio_for_day(
    day: str,
    risk_rs: int,
    wait_for_time: bool = False,
):
    """
    Build the full 10-symbol parity plan for a specific trading day and
    persist it into live_state.json + journal.

    - Uses the same logic as GET /api/state.
    - 'day' is YYYY-MM-DD in backtest / paper mode.
    """
    log.info(
        "Arming full portfolio for day=%s, daily_risk=%s, wait=%s",
        day,
        risk_rs,
        wait_for_time,
    )

    # For now we honour the CLI risk directly
    daily_risk_rs = int(risk_rs)

    # 1) Build raw plans (tags + pick + entry/stop/targets) for each symbol
    raw_plans = _build_raw_plans_for_day(day)

    # 2) Apply portfolio parity split (equal risk per active trade)
    portfolio_plan = _apply_portfolio_split(raw_plans, daily_risk_rs)

    # Force portfolio date to match the requested day
    portfolio_plan["date"] = day

    # 3) Persist into live_state.json (under 'portfolio_plan')
    _write_portfolio_plan_to_state(portfolio_plan)

    # 4) Log a concise summary
    log.info(
        "Portfolio plan built for day %s: daily_risk=%s, active_trades=%s, total_planned=%s",
        day,
        portfolio_plan["daily_risk_rs"],
        portfolio_plan["active_trades"],
        portfolio_plan.get("total_planned_risk_rs"),
    )

    # 5) Append to journal.csv (one row per active planned trade)
    try:
        written = append_portfolio_plan(portfolio_plan)
        log.info(
            "Journaled %d planned trades to %s for day=%s",
            written,
            SETTINGS.paths.journal,
            day,
        )
    except Exception:
        log.exception("Failed to append portfolio_plan to journal")

    return portfolio_plan


def main():
    parser = argparse.ArgumentParser(description="Probedge daily portfolio planner")
    parser.add_argument(
        "--day",
        type=str,
        default=None,
        help="Trading day YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--risk",
        type=int,
        default=None,
        help="Override daily risk in rupees (default: SETTINGS)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="If set, wait until 09:40 before building plan",
    )
    args = parser.parse_args()

    arm_portfolio_for_day(
        day=args.day,
        risk_rs=args.risk,
        wait_for_time=args.wait,
    )


if __name__ == "__main__":
    main()
