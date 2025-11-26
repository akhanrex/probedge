# probedge/backtest/exec_adapter.py

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from probedge.infra.settings import SETTINGS



# Match Colab backtest window: 09:40 → 15:05
T0_M = 9 * 60 + 40  # 09:40
T1_M = 15 * 60 + 5  # 15:05


def _read_tm5(path: str) -> pd.DataFrame:
    """
    Robust 5-min reader copied from Colab batch backtest.
    Handles BOM, weird column names, and builds DateTime + OHLCV.
    """
    df = pd.read_csv(path)
    # Clean column names (strip BOM, spaces)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]

    lc2orig = {c.lower(): c for c in df.columns}

    # 1) Try common datetime columns
    dt = None
    for key in ("datetime", "date_time", "timestamp", "date"):
        if key in lc2orig:
            dt = pd.to_datetime(df[lc2orig[key]], errors="coerce")
            break

    # 2) Try separate date + time columns
    if dt is None and ("date" in lc2orig and "time" in lc2orig):
        dt = pd.to_datetime(
            df[lc2orig["date"]].astype(str) + " " + df[lc2orig["time"]].astype(str),
            errors="coerce",
        )

    # 3) Try year/month/day/hour/minute style columns
    parts = ["year", "month", "day", "hour", "minute"]
    if dt is None and all(p in lc2orig for p in parts):
        dt = pd.to_datetime(
            dict(
                year=df[lc2orig["year"]],
                month=df[lc2orig["month"]],
                day=df[lc2orig["day"]],
                hour=df[lc2orig["hour"]],
                minute=df[lc2orig["minute"]],
            ),
            errors="coerce",
        )

    if dt is None:
        raise RuntimeError(f"No recognizable datetime columns in intraday file: {path}")

    # Canonical DateTime column
    for col in list(df.columns):
        if col != "DateTime" and col.lower() in ("datetime", "date_time", "timestamp"):
            df.drop(columns=col, inplace=True, errors="ignore")

    if "DateTime" in df.columns:
        df["DateTime"] = dt
    else:
        df.insert(0, "DateTime", dt)

    # Map OHLCV
    def pick(*aliases):
        for a in aliases:
            if a in lc2orig:
                return lc2orig[a]
        for c in df.columns:
            if c.lower() in aliases:
                return c
        return None

    col_map = {
        "Open": pick("open", "o"),
        "High": pick("high", "h"),
        "Low": pick("low", "l"),
        "Close": pick("close", "c"),
        "Volume": pick("volume", "vol", "qty", "quantity"),
    }

    for k, v in col_map.items():
        if v and v != k:
            df.rename(columns={v: k}, inplace=True)

    # After renames, drop any duplicate columns by name (keep first occurrence)
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]


    # Ensure numeric OHLCV
    # Ensure numeric OHLCV (defensive against weird shapes)
    for k in ("Open", "High", "Low", "Close", "Volume"):
        if k in df.columns:
            col = df[k]

            # If, for any reason, we ended up with a 2D slice, squeeze it
            if isinstance(col, pd.DataFrame):
                # take first column if duplicated somehow
                col = col.iloc[:, 0]

            # squeeze 1D and coerce to numeric
            df[k] = pd.to_numeric(col.squeeze(), errors="coerce")


    # Drop junk, sort, and add Date / _mins
    df = (
        df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
        .sort_values("DateTime")
        .reset_index(drop=True)
    )

    # IST-naive: treat as wall-clock, no tz conversion
    df["Date"] = df["DateTime"].dt.normalize()
    df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute
    return df


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

    def _as_1d(col):
        # handle accidental 2D frames or weird shapes
        if isinstance(col, pd.DataFrame):
            col = col.iloc[:, 0]
        return pd.to_numeric(col.squeeze(), errors="coerce").to_numpy(dtype=float)

    hi = _as_1d(win["High"])
    lo = _as_1d(win["Low"])
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


from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger

log = get_logger(__name__)

def _load_tm5_for_symbol(symbol: str) -> pd.DataFrame:
    """
    Load intraday 5-min data for a symbol using the same path pattern
    as the rest of the app, then parse it with the Colab _read_tm5.
    """
    pattern = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    path = pattern.format(sym=symbol)
    log.info("[exec_adapter] loading intraday tm5 for %s from %s", symbol, path)
    return _read_tm5(path)

def simulate_trade_colab_style(trade_row, intraday_raw=None):
    """
    Simulate a single trade using the SAME R1/R2 resolution logic
    as your Colab backtest code.

    Returns:
        (pnl_r1, pnl_r2, r1_result, r2_result, hit_times_dict)
    """
    # --- 1) Extract journal fields (from data/journal/journal.csv) ---
    symbol = str(trade_row["symbol"])
    day_ts = pd.to_datetime(trade_row["day"])
    day_date = day_ts.date()


    side = str(trade_row["side"]).upper()
    long_side = side in ("BUY", "BULL")

    qty = int(trade_row["qty"])
    entry = float(trade_row["entry"])
    stop = float(trade_row["stop"])
    t1 = float(trade_row["target1"])
    t2 = float(trade_row["target2"])

    # --- 2) Load TM5 for that symbol+day using the canonical reader ---
    tm5 = _load_tm5_for_symbol(symbol)

    # Ensure we have a Date column to filter on
    if "Date" not in tm5.columns and "DateTime" in tm5.columns:
        tm5["Date"] = pd.to_datetime(tm5["DateTime"])
    elif "Date" in tm5.columns:
        tm5["Date"] = pd.to_datetime(tm5["Date"])
    else:
        raise RuntimeError(
            f"TM5 for {symbol} missing Date/DateTime columns; columns={tm5.columns.tolist()}"
        )

    # Filter to this trading day by DATE (ignore timezone)
    df_dates = tm5["Date"].dt.date
    day_df = tm5[df_dates == day_date].copy()

    if day_df.empty:
        raise RuntimeError(f"No intraday TM5 data for {symbol} on {day_date}")


    # Slice 09:40→15:05 window (matches Colab backtest)
    w09 = _slice_window_fast(day_df, T0_M, T1_M)
    if w09.empty:
        raise RuntimeError(f"No 09:40–15:05 window for {symbol} on {day.date()}")

    # --- 3) Determine earliest touches of stop/T1/T2 ---
    touches = _earliest_touch_times(w09, long_side, stop, t1, t2)
    ts_stop, ts_t1, ts_t2 = touches["stop"], touches["t1"], touches["t2"]

    # Risk per share from entry/stop (same as batch)
    risk_per_share = (entry - stop) if long_side else (stop - entry)
    if not np.isfinite(risk_per_share) or risk_per_share <= 0:
        raise RuntimeError(f"Bad risk_per_share for {symbol} on {day.date()}")

    # --- 4) R1 PnL (1R target) ---
    if ts_t1 is not None and (ts_stop is None or ts_t1 <= ts_stop):
        # T1 hit before (or same bar as) stop
        pnl_r1 = qty * (t1 - entry) if long_side else qty * (entry - t1)
        r1 = "WIN"
    elif ts_stop is not None and (ts_t1 is None or ts_stop < ts_t1):
        # Stop hit first
        pnl_r1 = -qty * risk_per_share
        r1 = "LOSS"
    else:
        # Neither T1 nor stop hit → exit at 15:05 close
        close_col = w09["Close"]
        if isinstance(close_col, pd.DataFrame):
            close_col = close_col.iloc[:, 0]
        exit_px = float(close_col.iloc[-1])

        pnl_r1 = qty * (exit_px - entry) if long_side else qty * (entry - exit_px)
        r1 = "EOD"


    # --- 5) R2 PnL (2R target) ---
    if ts_t2 is not None and (ts_stop is None or ts_t2 <= ts_stop):
        pnl_r2 = qty * (t2 - entry) if long_side else qty * (entry - t2)
        r2 = "WIN"
    elif ts_stop is not None and (ts_t2 is None or ts_stop < ts_t2):
        pnl_r2 = -qty * risk_per_share
        r2 = "LOSS"
    else:
    else:
        close_col = w09["Close"]
        if isinstance(close_col, pd.DataFrame):
            close_col = close_col.iloc[:, 0]
        exit_px = float(close_col.iloc[-1])

        pnl_r2 = qty * (exit_px - entry) if long_side else qty * (entry - exit_px)
        r2 = "EOD"


    return pnl_r1, pnl_r2, r1, r2, touches
