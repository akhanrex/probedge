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
    day: Optional[str] = None,
    risk_rs: Optional[int] = None,
    wait_for_time: bool = False,
) -> dict:
    """
    Build and persist a full 10-symbol portfolio plan for a given day.

    - day: 'YYYY-MM-DD' or None (=today)
    - risk_rs: override daily risk (None = default from SETTINGS)
    - wait_for_time: if True, block until 09:40 before computing plan
    """
    if wait_for_time:
        log.info("Waiting until %s before building portfolio plan...", T_PLAN)
        wait_until(T_PLAN)

    if day is None:
        d: Optional[date] = None
    else:
        d = date.fromisoformat(day)

    plan = build_portfolio_state_for_day(day=d, explicit_risk_rs=risk_rs)

    state = aj.read(default={})
    state["portfolio_plan"] = plan
    aj.write(state)

    log.info(
        "Portfolio plan built for day %s: daily_risk=%s, active_trades=%s, total_planned=%s",
        day,
        portfolio_plan["daily_risk_rs"],
        portfolio_plan["active_trades"],
        portfolio_plan["total_planned_risk_rs"],
    )

    # --- journal the portfolio plan ---
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
