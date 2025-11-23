# apps/runtime/paper_exec_from_journal.py

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from probedge.infra.logger import get_logger
from probedge.infra.settings import SETTINGS
from probedge.backtest.exec_adapter import simulate_trade_colab_style

log = get_logger(__name__)


def _load_intraday(sym: str) -> pd.DataFrame:
    """
    Load intraday TM5 CSV for a symbol using SETTINGS.paths.intraday
    fallback to data/intraday/{sym}_5minute.csv
    """
    tmpl = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    path = Path(tmpl.format(sym=sym))
    if not path.exists():
        raise FileNotFoundError(f"Intraday file not found for {sym}: {path}")
    return pd.read_csv(path)


def run_paper_exec_for_day(day_str: str) -> None:
    """
    For a given day:
      - read journal.csv
      - simulate fills using Colab-style logic
      - append rows to data/journal/fills.csv
    """
    journal_path = Path(SETTINGS.paths.journal or "data/journal/journal.csv")
    if not journal_path.exists():
        raise RuntimeError(f"Journal not found at {journal_path}")

    log.info("Paper exec from journal for day=%s", day_str)
    j = pd.read_csv(journal_path)

    if "day" not in j.columns:
        raise RuntimeError("journal.csv missing 'day' column")

    j_day = j[j["day"].astype(str) == str(day_str)].copy()
    if j_day.empty:
        log.info("No journal rows for day=%s", day_str)
        return

    fills: List[Dict[str, Any]] = []

    # cache intraday per symbol
    intraday_cache: Dict[str, pd.DataFrame] = {}

    for _, row in j_day.iterrows():
        sym = str(row["symbol"]).upper()
        side = str(row["side"]).upper()
        qty = int(row["qty"])

        # safety: skip nonsense
        if qty <= 0:
            continue

        if sym not in intraday_cache:
            intraday_cache[sym] = _load_intraday(sym)

        intraday = intraday_cache[sym]

        trade = {
            "day": day_str,
            "symbol": sym,
            "side": side,
            "qty": qty,
            "entry": float(row["entry"]),
            "stop": float(row["stop"]),
            "target1": float(row["target1"]),
            "target2": float(row["target2"]),
            "planned_risk_rs": float(row.get("planned_risk_rs", 0.0) or 0.0),
        }

        (
            pnl_rs,
            pnl_r,
            exit_reason,
            entry_ts,
            exit_ts,
            exit_price,
        ) = simulate_trade_colab_style(trade, intraday)

        fills.append(
            {
                "day": day_str,
                "mode": row.get("mode", "paper"),
                "symbol": sym,
                "side": side,
                "qty": qty,
                "entry": float(row["entry"]),
                "stop": float(row["stop"]),
                "target1": float(row["target1"]),
                "target2": float(row["target2"]),
                "entry_time": entry_ts,
                "exit_time": exit_ts,
                "exit_price": exit_price,
                "pnl_rs": pnl_rs,
                "pnl_r": pnl_r,
                "exit_reason": exit_reason,
                "daily_risk_rs": float(row.get("daily_risk_rs", 0.0) or 0.0),
                "planned_risk_rs": float(row.get("planned_risk_rs", 0.0) or 0.0),
                "confidence_pct": row.get("confidence_pct", None),
                "tag_OT": row.get("tag_OT", None),
                "tag_OL": row.get("tag_OL", None),
                "tag_PDC": row.get("tag_PDC", None),
                "parity_mode": row.get("parity_mode", None),
                "strategy": row.get("strategy", None),
                "journal_created_at": row.get("created_at", None),
            }
        )

    if not fills:
        log.info("No valid fills generated for day=%s", day_str)
        return

    fills_df = pd.DataFrame(fills)
    fills_path = Path("data/journal/fills.csv")

    mode = "a" if fills_path.exists() else "w"
    header = not fills_path.exists()
    fills_df.to_csv(fills_path, index=False, mode=mode, header=header)

    log.info(
        "Wrote %d fills to %s for day=%s",
        len(fills_df),
        fills_path,
        day_str,
    )


def main():
    parser = argparse.ArgumentParser(description="Probedge paper exec from journal")
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
