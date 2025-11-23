# apps/runtime/daily_timeline.py

from __future__ import annotations

import argparse
from datetime import date, datetime, time as dtime
from typing import Optional

from probedge.infra.logger import get_logger
from probedge.storage.atomic_json import AtomicJSON
from probedge.infra.settings import SETTINGS
from probedge.journal.writer import append_portfolio_plan

# Reuse the same helpers as the API state/plan endpoints
from apps.api.routes.state import (
    _build_raw_plans_for_day,
    _apply_portfolio_split,
    _write_portfolio_plan_to_state,
    _effective_daily_risk_rs,
)

log = get_logger(__name__)

STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)

# 09:40 plan lock time (IST)
T_PLAN = dtime(9, 40)


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
        _time.sleep(5)


def arm_portfolio_for_day(
    day: Optional[str],
    risk_rs: Optional[int],
    wait_for_time: bool = False,
):
    """
    Build the full 10-symbol parity plan for a specific trading day and
    persist it into live_state.json + journal.

    - Uses the SAME logic as GET /api/state and POST /api/plan/arm_day.
    - 'day' is YYYY-MM-DD in backtest / paper mode.
    """
    # 0) Resolve day string
    if day is None:
        day = date.today().isoformat()

    # 1) Optionally wait until T_PLAN (for real intraday run)
    if wait_for_time:
        log.info("Waiting until %s before arming portfolio for %s", T_PLAN, day)
        wait_until(T_PLAN)

    # 2) Decide daily risk: CLI override > config
    if risk_rs is not None:
        daily_risk_rs = int(risk_rs)
    else:
        daily_risk_rs = _effective_daily_risk_rs()

    log.info(
        "Arming full portfolio for day=%s, daily_risk=%s, wait=%s",
        day,
        daily_risk_rs,
        wait_for_time,
    )

    # 3) Build raw per-symbol plans (tags + pick + entry/stop/targets)
    raw_plans = _build_raw_plans_for_day(day)

    # 4) Apply portfolio parity split (equal risk per active trade)
    portfolio_plan = _apply_portfolio_split(raw_plans, daily_risk_rs)

    # Force portfolio date to the requested day
    portfolio_plan["date"] = day

    # 5) Persist into live_state.json under 'portfolio_plan'
    _write_portfolio_plan_to_state(portfolio_plan)

    # 6) Log summary
    log.info(
        "Portfolio plan built for day %s: daily_risk=%s, active_trades=%s, total_planned=%s",
        day,
        portfolio_plan["daily_risk_rs"],
        portfolio_plan["active_trades"],
        portfolio_plan.get("total_planned_risk_rs"),
    )

    # 7) Append to journal.csv (one row per active planned trade)
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
        help="Override daily risk in rupees (default: SETTINGS / _effective_daily_risk_rs)",
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
