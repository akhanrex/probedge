# apps/runtime/paper_exec_from_journal.py

from __future__ import annotations

import argparse
import os
from datetime import datetime

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


def run_paper_exec_for_day(day_str: str) -> None:
    """
    Replay all planned trades from journal.csv for a given day using the
    SAME Colab-style R1/R2 resolver, and write results to data/journal/fills.csv.
    """
    log.info("Paper exec from journal for day=%s", day_str)

    journal_path = SETTINGS.paths.journal or "data/journal/journal.csv"
    fills_path = "data/journal/fills.csv"

    if not os.path.exists(journal_path):
        raise FileNotFoundError(f"Journal not found at {journal_path}")

    journal = pd.read_csv(journal_path)

    if "day" not in journal.columns:
        raise RuntimeError("journal.csv missing 'day' column")

    # Filter trades for the requested day
    mask = journal["day"].astype(str) == day_str
    day_trades = journal.loc[mask].copy()

    # Only simulate real trades (qty > 0)
    if "qty" in day_trades.columns:
        day_trades = day_trades[day_trades["qty"] > 0]

    if day_trades.empty:
        log.warning("No trades for day=%s in journal", day_str)
        return

    fills_rows = []

    for _, trade in day_trades.iterrows():
        # Colab-style sim: returns (pnl_r1, pnl_r2, r1, r2, touches)
        pnl_r1, pnl_r2, r1, r2, touches = simulate_trade_colab_style(trade)

        fills_rows.append(
            {
                "day": trade["day"],
                "mode": trade.get("mode", ""),
                "symbol": trade["symbol"],
                "side": trade["side"],
                "qty": trade["qty"],
                "entry": trade["entry"],
                "stop": trade["stop"],
                "target1": trade["target1"],
                "target2": trade["target2"],
                "pnl_r1": pnl_r1,
                "pnl_r2": pnl_r2,
                "r1_result": r1,
                "r2_result": r2,
                "hit_stop_time": _dt_to_iso(touches["stop"]),
                "hit_t1_time": _dt_to_iso(touches["t1"]),
                "hit_t2_time": _dt_to_iso(touches["t2"]),
                "created_at": datetime.now().isoformat(timespec="seconds"),
            }
        )

    new_fills = pd.DataFrame(fills_rows)

    # Append to existing fills.csv (if any)
    if os.path.exists(fills_path):
        existing = pd.read_csv(fills_path)
        fills = pd.concat([existing, new_fills], ignore_index=True)
    else:
        os.makedirs(os.path.dirname(fills_path), exist_ok=True)
        fills = new_fills

    fills.to_csv(fills_path, index=False)
    log.info(
        "Appended %d fill rows to %s for day=%s",
        len(new_fills),
        fills_path,
        day_str,
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
