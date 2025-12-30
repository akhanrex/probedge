# apps/runtime/paper_exec_from_journal.py

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
import pandas as pd

from probedge.infra.logger import get_logger
from probedge.infra.settings import SETTINGS
from probedge.backtest.exec_adapter import simulate_trade_colab_style


def _dt_to_iso(val: object) -> str:
    """Convert numpy.datetime64 / Timestamp / None to ISO string safely."""
    if val is None:
        return ""
    try:
        return pd.to_datetime(val).isoformat()
    except Exception:
        # Fallback: plain string
        return str(val)


log = get_logger(__name__)


def run_paper_exec_for_day(day_str: str):
    day = pd.to_datetime(day_str).normalize()

    journal_path = Path("data/journal/journal.csv")
    fills_path   = Path("data/journal/fills.csv")

    journal = pd.read_csv(journal_path)
    journal["day"] = pd.to_datetime(journal["day"]).dt.normalize()

    # Filter for this day and paper mode
    day_trades = journal[(journal["day"] == day) & (journal["mode"] == "paper")].copy()

    if day_trades.empty:
        log.info("No trades in journal for day=%s", day_str)
        return

    # *** NEW: drop exact duplicates so re-arming doesn't multiply fills ***
    day_trades = day_trades.drop_duplicates(
        subset=[
            "day", "mode", "symbol", "side",
            "qty", "entry", "stop", "target1", "target2"
        ]
    ).reset_index(drop=True)

    fills_rows = []

    for _, trade in day_trades.iterrows():
        (
            pnl_r1,
            pnl_r2,
            r1_result,
            r2_result,
            touches,
        ) = simulate_trade_colab_style(trade)

        fills_rows.append(
            {
                "day": trade["day"].date().isoformat(),
                "mode": trade["mode"],
                "symbol": trade["symbol"],
                "side": trade["side"],
                "qty": int(trade["qty"]),
                "entry": float(trade["entry"]),
                "stop": float(trade["stop"]),
                "target1": float(trade["target1"]),
                "target2": float(trade["target2"]),
                "pnl_r1": pnl_r1,
                "pnl_r2": pnl_r2,
                "r1_result": r1_result,
                "r2_result": r2_result,
                "hit_stop_time": (
                    pd.to_datetime(touches["stop"]).isoformat()
                    if touches["stop"] is not None
                    else ""
                ),
                "hit_t1_time": (
                    pd.to_datetime(touches["t1"]).isoformat()
                    if touches["t1"] is not None
                    else ""
                ),
                "hit_t2_time": (
                    pd.to_datetime(touches["t2"]).isoformat()
                    if touches["t2"] is not None
                    else ""
                ),
                "created_at": pd.Timestamp.now().isoformat(timespec="seconds"),
            }
        )

    # Append to fills.csv (create if needed)
    fills_df = pd.DataFrame(fills_rows)
    header = not os.path.exists(fills_path)
    fills_df.to_csv(fills_path, mode="a", header=header, index=False)

    log.info(
        "Appended %d fill rows to %s for day=%s",
        len(fills_df), fills_path, day_str
    )


def main():
    parser = argparse.ArgumentParser(
        description="Replay journal trades in paper mode using Colab-style simulator."
    )
    parser.add_argument(
        "--day",
        type=str,
        required=True,
        help="Trading day YYYY-MM-DD",
    )
    args = parser.parse_args()

    run_paper_exec_for_day(args.day)


if __name__ == "__main__":
    main()
