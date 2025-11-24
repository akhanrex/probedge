# apps/runtime/run_range_from_tm5.py

from __future__ import annotations

import subprocess
from datetime import date, timedelta

from probedge.infra.logger import get_logger

log = get_logger(__name__)


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    # ---- EDIT THESE ----
    start_day = date(2025, 8, 1)
    end_day   = date(2025, 9, 30)
    daily_risk = 10000
    # --------------------

    for d in daterange(start_day, end_day):
        if d.weekday() >= 5:
            continue  # skip Sat/Sun
        day_str = d.isoformat()
        log.info("=== Simulating day %s ===", day_str)

        # 1) Build plan from TM5
        cmd_plan = [
            "python",
            "-m",
            "apps.runtime.daily_timeline",
            "--day",
            day_str,
            "--risk",
            str(daily_risk),
        ]
        subprocess.run(cmd_plan, check=True)

        # 2) Execute paper trades from journal (fills)
        cmd_exec = [
            "python",
            "-m",
            "apps.runtime.paper_exec_from_journal",
            "--day",
            day_str,
        ]
        subprocess.run(cmd_exec, check=True)


if __name__ == "__main__":
    main()
