# probedge/backtest/exec_adapter.py

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.storage.readers import read_tm5_csv  # <-- reuse the proven reader


# Match Colab backtest window: 09:40 → 15:05
T0_M = 9 * 60 + 40  # 09:40
T1_M = 15 * 60 + 5  # 15:05


def _slice_window_fast(df_day: pd.DataFrame, m0: int, m1: int) -> pd.DataFrame:
    """Slice a single-day TM5 frame between minute offsets [m0, m1]."""
    if df_day is None or df_day.empty:
        return pd.DataFrame()
    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    return df_day.loc[m, ["DateTime", "Open", "High", "Low", "Close", "Date"]]


def _earliest_touch_times(
    win: pd.DataFrame, long: bool, stop: float, t1: float, t2: float
) -> Dict[str, pd.Timestamp | None]:
    """
    Earliest times price touches stop, T1, T2 in 09:40→15:05.

    This is the same logic as in your Colab batch backtest.
    """
    if win is None or win.empty:
        return {"stop": None, "t1": None, "t2": None}

    hi = win["High"].to_numpy(dtype=float)
    lo = win["Low"].to_numpy(dtype=float)
    ts = win["DateTime"].to_numpy()

    if long:
        cond_stop = lo <= stop
        cond_t1 = hi >= t1
        cond_t2 = hi >= t2
    else:
        cond_stop = hi >= stop
        cond_t1 = lo <= t1
        cond_t2 = lo <= t2

    i_stop = np.argmax(cond_stop) if np.any(cond_stop) else -1
    i_t1 = np.argmax(cond_t1) if np.any(cond_t1) else -1
    i_t2 = np.argmax(cond_t2) if np.any(cond_t2) else -1

    return {
        "stop": ts[i_stop] if i_stop >= 0 else None,
        "t1": ts[i_t1] if i_t1 >= 0 else None,
        "t2": ts[i_t2] if i_t2 >= 0 else None,
    }


def _load_tm5_for_symbol(symbol: str) -> pd.DataFrame:
    """
    Load TM5 intraday file for a symbol using the SAME reader as plan_core:
    probedge.storage.readers.read_tm5_csv.

    This guarantees we handle whatever datetime layout your CSVs actually have.
    """
    path_tpl = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    path = path_tpl.format(sym=symbol)

    df = read_tm5_csv(path)  # <-- canonical reader

    # Ensure the columns we need for Colab-style backtest exist
    if "DateTime" not in df.columns:
        raise RuntimeError(
            f"read_tm5_csv returned no DateTime column for {symbol} ({path})"
        )

    if "Date" not in df.columns:
        df["Date"] = df["DateTime"].dt.normalize()

    if "_mins" not in df.columns:
        df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute

    return df


def simulate_trade_colab_style(trade_row, intraday_raw=None):
    """
    Simulate a single trade using the SAME R1/R2 resolution logic
    as your Colab backtest code.

    Returns:
        (pnl_r1, pnl_r2, r1_result, r2_result, hit_times_dict)
    """
    # --- 1) Extract journal fields (from data/journal/journal.csv) ---
    symbol = str(trade_row["symbol"])
    day = pd.to_datetime(trade_row["day"]).normalize()

    side = str(trade_row["side"]).upper()
    long_side = side in ("BUY", "BULL")

    qty = int(trade_row["qty"])
    entry = float(trade_row["entry"])
    stop = float(trade_row["stop"])
    t1 = float(trade_row["target1"])
    t2 = float(trade_row["target2"])

    # --- 2) Load TM5 for that symbol+day using the canonical reader ---
    tm5 = _load_tm5_for_symbol(symbol)

    # Filter to this trading day
    day_df = tm5[tm5["Date"] == day].copy()
    if day_df.empty:
        raise RuntimeError(f"No intraday TM5 data for {symbol} on {day.date()}")

    # Slice 09:40→15:05 window (matches Colab backtest)
    w09 = _slice_window_fast(day_df, T0_M, T1_M)
    if w09.empty:
        raise RuntimeError(f"No 09:40–15:05 window for {symbol} on {day.date()}")

    # --- 3) Determine earliest touches of
