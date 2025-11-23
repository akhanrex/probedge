# probedge/backtest/exec_adapter.py
#
# Single source of truth for execution behaviour.
# Here you should plug in the SAME logic you use in Colab backtest
# (entry bar, SL/TP priority, EOD exit, etc).

from __future__ import annotations

from datetime import datetime
from typing import Dict, Tuple

import pandas as pd

from probedge.infra.logger import get_logger

log = get_logger(__name__)


def simulate_trade_colab_style(
    trade: Dict[str, object],
    intraday: pd.DataFrame,
) -> Tuple[float, float, str, datetime | None, datetime | None, float]:
    """
    Run the *exact* Colab backtest method for ONE planned trade.

    Parameters
    ----------
    trade : dict
        One row from journal.csv:
        - day, mode, symbol, side (BUY/SELL)
        - qty, entry, stop, target1, target2
        - planned_risk_rs, daily_risk_rs, strategy, created_at, etc.

    intraday : DataFrame
        5-minute bars for that symbol, with at least:
        - datetime column (any name, but you'll standardize below)
        - open, high, low, close

    Returns
    -------
    pnl_rs : float
    pnl_r  : float (R multiple = pnl_rs / planned_risk_rs)
    exit_reason : str (e.g., "TP1", "TP2", "SL", "EOD", "NOFILL", etc.)
    entry_ts : datetime | None
    exit_ts  : datetime | None
    exit_price : float

    NOTE:
    -----
    For now this function is just a wrapper / placeholder.
    You should copy the day-execution logic from your Colab
    backtest and implement it HERE so that both Colab and
    Probedge runtime behave identically.
    """
    # ====== PLACEHOLDER IMPLEMENTATION ======
    # This simple version is only to keep the code runnable.
    # You MUST replace this body with your real Colab logic.

    side = str(trade["side"]).upper()
    qty = int(trade["qty"])
    entry = float(trade["entry"])
    stop = float(trade["stop"])
    t1 = float(trade["target1"])
    t2 = float(trade["target2"])
    planned_risk = float(trade["planned_risk_rs"])

    if intraday.empty or qty <= 0:
        return 0.0, 0.0, "NO_DATA", None, None, entry

    # Standardize columns (assumes you will adapt to your tm5 schema)
    cols_lower = {c.lower(): c for c in intraday.columns}
    dt_col = None
    for cand in ("datetime", "timestamp", "ts", "_dt"):
        if cand in cols_lower:
            dt_col = cols_lower[cand]
            break
    if dt_col is None:
        raise RuntimeError("No datetime column in intraday df for simulate_trade_colab_style")

    intraday = intraday.copy()
    intraday["_dt"] = pd.to_datetime(intraday[dt_col])

    # Naive placeholder: first bar touching entry; SL/TP2/TP1 priority.
    entry_idx = None
    entry_ts = None

    for i, row in intraday.iterrows():
        low = float(row.get("low", row.get("Low", 0)))
        high = float(row.get("high", row.get("High", 0)))
        dt = row["_dt"]
        if low <= entry <= high:
            entry_idx = i
            entry_ts = dt
            break

    if entry_idx is None:
        # Never filled
        exit_ts = intraday["_dt"].iloc[-1]
        exit_price = entry
        pnl_rs = 0.0
        pnl_r = 0.0
        return pnl_rs, pnl_r, "NOFILL", None, exit_ts, exit_price

    exit_price = float(intraday["close"].iloc[-1])
    exit_ts = intraday["_dt"].iloc[-1]
    exit_reason = "EOD"

    for _, row in intraday.iloc[entry_idx:].iterrows():
        high = float(row.get("high", row.get("High", 0)))
        low = float(row.get("low", row.get("Low", 0)))
        dt = row["_dt"]

        if side == "BUY":
            hit_sl = low <= stop
            hit_t2 = high >= t2
            hit_t1 = high >= t1
        else:  # SELL
            hit_sl = high >= stop
            hit_t2 = low <= t2
            hit_t1 = low <= t1

        if hit_sl:
            exit_price = stop
            exit_ts = dt
            exit_reason = "SL"
            break
        if hit_t2:
            exit_price = t2
            exit_ts = dt
            exit_reason = "TP2"
            break
        if hit_t1:
            exit_price = t1
            exit_ts = dt
            exit_reason = "TP1"
            break

    if side == "BUY":
        pnl_rs = (exit_price - entry) * qty
    else:
        pnl_rs = (entry - exit_price) * qty

    pnl_r = pnl_rs / planned_risk if planned_risk != 0 else 0.0

    return pnl_rs, pnl_r, exit_reason, entry_ts, exit_ts, exit_price
