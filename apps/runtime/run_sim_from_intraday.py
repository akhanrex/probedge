from __future__ import annotations

import argparse
import time
from datetime import datetime
from typing import Dict, List

import pandas as pd

from probedge.infra.logger import get_logger
from probedge.infra.settings import SETTINGS
from probedge.infra.loaders import read_tm5_csv, by_day_map
from probedge.storage.atomic_json import AtomicJSON
from probedge.storage.resolver import intraday_path
from apps.runtime.daily_timeline import arm_portfolio_for_day
from pathlib import Path

log = get_logger(__name__)

# Use the SAME state file as API/agg5 (SETTINGS.paths.state → usually data/state/live_state.json)
STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
Path(STATE_PATH).parent.mkdir(parents=True, exist_ok=True)
aj = AtomicJSON(STATE_PATH)


def _ensure_state_base(sim_day: str) -> None:
    """
    Reset live_state.json for a fresh SIM day:

    - clear any old plan / positions / P&L / risk meta
    - keep symbols dict (quotes will refill as we stream)
    - mark sim metadata shell (sim_day, sim_clock=None)
    """
    state = aj.read()
    if not isinstance(state, dict):
        state = {}

    # Wipe plan + P&L related keys from any previous run
    for k in (
        "portfolio_plan",
        "positions",
        "pnl",
        "risk",
        "batch_agent",
        "daily_risk_rs",
        "active_trades",
        "risk_per_trade_rs",
        "total_planned_risk_rs",
        "portfolio_date",
    ):
        state.pop(k, None)

    # Clean per-symbol plan/position/pnl (if any leftover)
    symbols = state.get("symbols") or {}
    for sym, s in symbols.items():
        if not isinstance(s, dict):
            continue
        s.pop("plan", None)
        s.pop("position", None)
        s.pop("pnl_today", None)
    state["symbols"] = symbols

    # Top-level date for UI header
    state["date"] = sim_day
    # SIM metadata
    state["sim"] = True
    state["sim_day"] = sim_day
    state["sim_clock"] = None

    aj.write(state)


def _write_bar(sym: str, bar: pd.Series, sim_day: str) -> None:
    """
    Update live_state.json with the latest quote for a single symbol/bar.
    Shape matches live agg5: ltp + ohlc + volume.
    """
    state = aj.read()
    if not isinstance(state, dict):
        state = {}

    state.setdefault("symbols", {})
    sym_state = state["symbols"].setdefault(sym, {})

    dt = pd.to_datetime(bar["DateTime"])

    px_close = float(bar["Close"])
    px_open  = float(bar["Open"])
    px_high  = float(bar["High"])
    px_low   = float(bar["Low"])
    vol      = float(bar.get("Volume", float("nan")))

    # Top-level, same as agg5
    sym_state["ltp"] = px_close
    sym_state["ohlc"] = {
        "o": px_open,
        "h": px_high,
        "l": px_low,
        "c": px_close,
    }
    sym_state["volume"] = vol

    # Optional nested quote
    sym_state["quote"] = {
        "ltp": px_close,
        "open": px_open,
        "high": px_high,
        "low": px_low,
        "volume": vol,
        "timestamp": dt.isoformat(),
    }

    # SIM metadata
    state["date"] = sim_day
    state["sim"] = True
    state["sim_day"] = sim_day
    state["sim_clock"] = dt.isoformat()

    aj.write(state)


def _load_intraday_for_day(sim_day: str) -> Dict[str, pd.DataFrame]:
    """
    Load intraday TM5 for all symbols and return per-symbol DF for that sim_day.
    """
    frames: Dict[str, pd.DataFrame] = {}
    day_norm = pd.to_datetime(sim_day).normalize()

    for sym in SETTINGS.symbols:
        p = intraday_path(sym)
        df = read_tm5_csv(str(p))
        by_day = by_day_map(df)
        df_day = by_day.get(day_norm)
        if df_day is None or df_day.empty:
            log.warning("SIM: no intraday for %s on %s", sym, sim_day)
            continue
        frames[sym] = df_day

    return frames


def run_sim(sim_day: str, speed: float) -> None:
    """
    Full SIM for a single trading day:

      1) Arm portfolio plan for `sim_day` (risk split, qty, etc.)
      2) Replay intraday 5-min bars as quotes into live_state.json
      3) Mark sim metadata (sim, sim_day, sim_clock, date)
    """
    log.info("SIM: starting for day=%s, speed=%sx", sim_day, speed)

    # 0) Ensure base state + sim metadata shell
    _ensure_state_base(sim_day)

    # 1) Build and persist full portfolio plan for this day
    #    (same as 09:40 planner; writes into live_state.json under 'portfolio_plan'
    #     and symbols[*].plan, and appends to journal).
    arm_portfolio_for_day(day=sim_day, risk_rs=None, wait_for_time=False)

    # 2) Load intraday frames for this sim day
    frames = _load_intraday_for_day(sim_day)
    if not frames:
        log.error("SIM: no intraday data for any symbols on %s", sim_day)
        return

    # Build global time axis across all symbols (unique DateTime values)
    all_times: List[datetime] = []
    for df in frames.values():
        all_times.extend(df["DateTime"].tolist())

    times_sorted = sorted(set(all_times))
    log.info(
        "SIM: starting intraday replay for %s, %s bars, speed=%sx",
        sim_day,
        len(times_sorted),
        speed,
    )

    prev_ts: datetime | None = None
    for ts in times_sorted:
        if prev_ts is None:
            delay = 0.0
        else:
            delta_secs = (ts - prev_ts).total_seconds()
            delay = max(0.0, delta_secs / max(speed, 1e-6))
        prev_ts = ts

        if delay > 0:
            time.sleep(delay)

        # For each symbol, if it has a bar at this timestamp, write quote
        for sym, df_day in frames.items():
            row = df_day[df_day["DateTime"] == ts]
            if row.empty:
                continue
            bar = row.iloc[0]
            _write_bar(sym, bar, sim_day)

    log.info("SIM: finished streaming intraday for %s", sim_day)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay intraday TM5 as SIM ticks")
    parser.add_argument(
        "--day",
        type=str,
        required=True,
        help="Trading day YYYY-MM-DD to simulate",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=60.0,
        help="Speed multiplier vs real-time (e.g. 60 = 1 hour in 1 minute)",
    )
    args = parser.parse_args()

    run_sim(args.day, args.speed)


if __name__ == "__main__":
    main()
