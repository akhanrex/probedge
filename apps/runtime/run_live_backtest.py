# apps/runtime/run_live_backtest.py
#
# Multi-day backtest using the SAME Phase A planner + paper execution
# that the live system uses.
#
# For each trading day D in the intraday data:
#   1) arm_portfolio_for_day(day=D, wait_for_time=False)
#   2) run_paper_exec_for_day(D)
# After all days, we run fills_to_daily to build daily P&L.
#
# IMPORTANT:
# - This assumes DATA_DIR points to a "backtest" data tree
#   (intraday + masters already built).
# - It does NOT rebuild masters or intraday.
# - It does NOT talk to Kite / ticks / agg5.

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import List, Optional, Set
from typing import Sequence

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger
from apps.runtime.daily_timeline import arm_portfolio_for_day
from apps.runtime.paper_exec_from_journal import run_paper_exec_for_day
from apps.runtime.fills_to_daily import main as fills_to_daily_main
from probedge.backtest.exec_adapter import _read_tm5
from probedge.storage.resolver import ALIASES as SYMBOL_ALIASES


log = get_logger(__name__)


def _intraday_dir() -> Path:
    """
    Resolve the intraday directory from SETTINGS.data_dir.

    In your repo layout:
      - DATA_DIR points to the repo root (from .env)
      - intraday files live under: DATA_DIR / "data/intraday"

    We also check DATA_DIR / "intraday" as a fallback, just in case.
    """
    root = Path(SETTINGS.data_dir)

    candidates = [
        root / "data" / "intraday",  # normal live layout
        root / "intraday",           # fallback (e.g. backtest-only layout)
    ]

    for p in candidates:
        if p.exists() and p.is_dir():
            return p

    # Fall back to the first candidate but log a warning
    log.warning(
        "Could not find intraday dir among %s; defaulting to %s",
        [str(c) for c in candidates],
        candidates[0],
    )
    return candidates[0]

def _storage_symbol(sym: str) -> str:
    s = str(sym).upper()
    return SYMBOL_ALIASES.get(s, s)


def _intraday_path(sym: str) -> Path:
    intraday_dir = _intraday_dir()
    storage_sym = _storage_symbol(sym)
    return intraday_dir / f"{storage_sym}_5minute.csv"

def _load_trading_calendar(
    symbols: Sequence[str],
    start: Optional[date],
    end: Optional[date],
) -> list[date]:
    """
    Build union of trading days across intraday TM5 files, using the same
    reader + alias mapping as live planner.

    If start/end are None, they are inferred from the data.
    """
    days = set()

    for sym in symbols:
        path = _intraday_path(sym)
        if not path.exists():
            log.warning("Intraday file missing for %s: %s", sym, path)
            continue

        try:
            df = _read_tm5(str(path))
        except Exception:
            log.exception("Failed to read intraday file for %s: %s", sym, path)
            continue


        if "Date" in df.columns:
            dseries = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
        else:
            # very defensive, but read_tm5_csv should always give Date
            dseries = pd.to_datetime(df["DateTime"], errors="coerce").dt.normalize()

        days.update(dseries.dropna().unique())

    if not days:
        raise RuntimeError("No trading days found in intraday data for any symbol")

    ordered_all = sorted(pd.Timestamp(d).date() for d in days)

    # Derive effective window
    start_eff = start or ordered_all[0]
    end_eff = end or ordered_all[-1]
    if start_eff > end_eff:
        raise RuntimeError(f"Backtest start date {start_eff} is after end date {end_eff}")

    ordered = [d for d in ordered_all if start_eff <= d <= end_eff]

    log.info(
        "Backtest trading calendar resolved: %d days from %s to %s",
        len(ordered),
        ordered[0],
        ordered[-1],
    )
    return ordered



def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Multi-day backtest using live Phase A planner + paper execution "
            "(no live ticks, no Kite)."
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date YYYY-MM-DD (default: first date available in intraday files)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date YYYY-MM-DD (default: last date available in intraday files)",
    )
    parser.add_argument(
        "--risk",
        type=int,
        default=None,
        help="Override DAILY portfolio risk in rupees "
             "(default: SETTINGS / _effective_daily_risk_rs).",
    )

    args = parser.parse_args()

    symbols = list(SETTINGS.symbols)
    if not symbols:
        raise RuntimeError("SETTINGS.symbols is empty; nothing to backtest")

    start_date: Optional[date] = (
        date.fromisoformat(args.start) if args.start else None
    )
    end_date: Optional[date] = (
        date.fromisoformat(args.end) if args.end else None
    )

    days = _load_trading_calendar(symbols, start=start_date, end=end_date)

    log.info(
        "Backtest starting: %d trading days from %s to %s | symbols=%s | risk_override=%s",
        len(days),
        days[0],
        days[-1],
        symbols,
        args.risk,
    )

    for idx, d in enumerate(days, start=1):
        day_str = d.isoformat()
        log.info("=== Backtest day %d/%d: %s ===", idx, len(days), day_str)

        # 1) Plan with the live planner (no waiting for clock)
        try:
            arm_portfolio_for_day(
                day=day_str,
                risk_rs=args.risk,
                wait_for_time=False,
            )
        except Exception:
            log.exception("arm_portfolio_for_day failed for %s; skipping executions", day_str)
            continue

        # 2) Paper execution for that day, using the same runtime as live EOD
        try:
            run_paper_exec_for_day(day_str)
        except Exception:
            log.exception("run_paper_exec_for_day failed for %s", day_str)
            continue

    # 3) After all days have fills, roll them up into daily P&L ONCE
    try:
        fills_to_daily_main()
    except Exception:
        log.exception("fills_to_daily_main failed at end of backtest")

    log.info("Backtest completed across %d trading days.", len(days))


if __name__ == "__main__":
    main()

