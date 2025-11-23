# apps/runtime/paper_exec_from_journal.py

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd

from probedge.infra.logger import get_logger
from probedge.infra.settings import SETTINGS
from probedge.journal.fills import append_fills
from probedge.backtest.exec_adapter import simulate_trade_colab_style

log = get_logger(__name__)


def _load_journal_for_day(day_str: str) -> pd.DataFrame:
    journal_path = Path(SETTINGS.paths.journal or "data/journal/journal.csv")
    if not journal_path.exists():
        raise FileNotFoundError(f"Journal not found at {journal_path}")

    df = pd.read_csv(journal_path)
    if "day" not in df.columns:
        raise RuntimeError("journal.csv is missing 'day' column")

    df["day"] = df["day"].astype(str)
    day_df = df[df["day"] == day_str].copy()

    if day_df.empty:
        log.warning("No journal rows found for day=%s", day_str)

    return day_df


def _load_intraday(sym: str) -> pd.DataFrame:
    tmpl = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    path = Path(tmpl.format(sym=sym))
    if not path.exists():
        raise FileNotFoundError(f"Intraday file not found for {sym}: {path}")

    df = pd.read_csv(path)
    return df


def run_paper_exec_for_day(day: str):
    log.info("Paper exec from journal for day=%s", day)

    journal_df = _load_journal_for_day(day)
    if journal_df.empty:
        log.warning("No planned trades to execute for day=%s", day)
        return

    fills_rows: List[Dict[str, object]] = []
    total_planned = 0.0
    total_pnl = 0.0

    # Group by symbol to reuse intraday load
    for symbol, sym_df in journal_df.groupby("symbol"):
        intraday = _load_intraday(symbol)

        for _, row in sym_df.iterrows():
            trade = row.to_dict()
            planned_risk = float(trade["planned_risk_rs"])
            total_planned += planned_risk

            (
                pnl_rs,
                pnl_r,
                exit_reason,
                entry_ts,
                exit_ts,
                exit_price,
            ) = simulate_trade_colab_style(trade, intraday)

            total_pnl += pnl_rs

            fills_rows.append(
                {
                    "day": trade["day"],
                    "mode": trade["mode"],
                    "symbol": trade["symbol"],
                    "side": trade["side"],
                    "qty": int(trade["qty"]),
                    "entry": float(trade["entry"]),
                    "stop": float(trade["stop"]),
                    "target1": float(trade["target1"]),
                    "target2": float(trade["target2"]),
                    "entry_ts": entry_ts.isoformat() if entry_ts else "",
                    "exit_ts": exit_ts.isoformat() if isinstance(exit_ts, datetime) else "",
                    "exit_price": exit_price,
                    "exit_reason": exit_reason,
                    "pnl_rs": pnl_rs,
                    "pnl_r": pnl_r,
                    "planned_risk_rs": planned_risk,
                    "daily_risk_rs": float(trade["daily_risk_rs"]),
                    "strategy": trade.get("strategy", ""),
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                }
            )

    written = append_fills(fills_rows)

    log.info(
        "Paper exec complete for day=%s: trades=%d, planned_risk=%.2f, pnl=%.2f, R=%.3f",
        day,
        written,
        total_planned,
        total_pnl,
        (total_pnl / total_planned) if total_planned else 0.0,
    )


def main():
    parser = argparse.ArgumentParser(description="Paper execution from journal (Colab parity)")
    parser.add_argument(
        "--day",
        type=str,
        required=True,
        help="Trading day YYYY-MM-DD (must exist in journal.csv)",
    )
    args = parser.parse_args()
    run_paper_exec_for_day(args.day)


if __name__ == "__main__":
    main()
