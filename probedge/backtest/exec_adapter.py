# probedge/backtest/exec_adapter.py

from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, Tuple

import numpy as np
import pandas as pd

# 09:40 → 15:05, same as batch code
T0_M = 9 * 60 + 40
T1_M = 15 * 60 + 5


def _canonicalize_intraday_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Take a raw intraday df (from data/intraday/{sym}_5minute.csv) and
    make it look like the batch _read_tm5 output:

    - Cleaned columns
    - DateTime column (built from datetime / timestamp OR date + time)
    - Open / High / Low / Close normalized
    - Date (normalized)
    - _mins (HH*60 + MM)
    """
    df = df.copy()

    # clean columns
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    lc2orig = {c.lower(): c for c in df.columns}

    # build DateTime
    dt = None

    # 1) direct datetime-like column
    for key in ("datetime", "date_time", "timestamp"):
        if key in lc2orig:
            dt = pd.to_datetime(df[lc2orig[key]], errors="coerce")
            break

    # 2) separate date + time
    if dt is None and ("date" in lc2orig and "time" in lc2orig):
        dt = pd.to_datetime(
            df[lc2orig["date"]].astype(str) + " " + df[lc2orig["time"]].astype(str),
            errors="coerce",
        )

    # 3) fallback if there is a "DateTime" column but not lowercased
    if dt is None and "DateTime" in df.columns:
        dt = pd.to_datetime(df["DateTime"], errors="coerce")

    if dt is None:
        raise RuntimeError("No recognizable datetime columns in intraday df")

    # inject DateTime col
    if "DateTime" in df.columns:
        df["DateTime"] = dt
    else:
        df.insert(0, "DateTime", dt)

    # normalize OHLC column names
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
    }

    for k, v in col_map.items():
        if v and v != k:
            df.rename(columns={v: k}, inplace=True)

    for k in ("Open", "High", "Low", "Close"):
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce")

    # drop bad rows, sort by time
    df = (
        df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
        .sort_values("DateTime")
        .reset_index(drop=True)
    )

    # add Date + _mins like batch _read_tm5
    df["Date"] = df["DateTime"].dt.normalize()
    df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute
    return df


def _slice_window_fast(df_day: pd.DataFrame, m0: int, m1: int) -> pd.DataFrame:
    """
    Same as batch _slice_window_fast:
    returns rows with _mins in [m0, m1], keeping main columns.
    """
    if df_day is None or df_day.empty:
        return pd.DataFrame()

    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    cols = [
        c
        for c in ["DateTime", "Open", "High", "Low", "Close", "Date", "_mins"]
        if c in df_day.columns
    ]
    return df_day.loc[m, cols]


def _earliest_touch_times(
    win: pd.DataFrame, long: bool, stop: float, t1: float, t2: float
) -> Dict[str, datetime | None]:
    """
    Identical logic to batch _earliest_touch_times:
    earliest bar where price touches stop, T1, T2.
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


def simulate_trade_colab_style(
    trade: Dict[str, Any], intraday_raw: pd.DataFrame
) -> Tuple[float, float, str, datetime | None, datetime | None, float]:
    """
    Core adapter:

    Inputs:
      trade: dict with keys
        - day (YYYY-MM-DD)
        - symbol
        - side ('BUY'/'SELL')
        - qty
        - entry
        - stop
        - target1
        - target2
        - planned_risk_rs (optional, from journal)
      intraday_raw: df loaded from data/intraday/{sym}_5minute.csv

    Behavior:
      - Canonicalize intraday df like _read_tm5
      - Slice 09:40→15:05 window for that day
      - Use batch _earliest_touch_times
      - Resolve exit using R2 logic: TP2 vs SL vs EOD

    Returns:
      (pnl_rs, pnl_r, exit_reason, entry_ts, exit_ts, exit_price)
    """
    df = _canonicalize_intraday_df(intraday_raw)

    day = pd.to_datetime(trade["day"]).normalize()
    df_day = df[df["Date"] == day]
    if df_day.empty:
        return 0.0, 0.0, "NO_DATA", None, None, float(trade.get("entry", 0.0))

    w09 = _slice_window_fast(df_day, T0_M, T1_M)
    if w09.empty:
        return 0.0, 0.0, "NO_SESSION", None, None, float(trade.get("entry", 0.0))

    side = str(trade["side"]).upper()
    long_side = side in ("BUY", "LONG", "BULL")

    qty = int(trade["qty"])
    entry = float(trade["entry"])
    stop = float(trade["stop"])
    t1 = float(trade["target1"])
    t2 = float(trade["target2"])
    planned_risk = float(trade.get("planned_risk_rs", 0.0) or 0.0)

    # same sign logic as batch: risk per share is stop - entry on shorts, entry - stop on longs
    risk_per_share = (entry - stop) if long_side else (stop - entry)
    if not np.isfinite(risk_per_share) or risk_per_share <= 0:
        return 0.0, 0.0, "BAD_RISK", None, None, entry

    if planned_risk == 0.0:
        planned_risk = abs(qty * risk_per_share)

    touches = _earliest_touch_times(w09, long_side, stop, t1, t2)
    ts_stop, ts_t1, ts_t2 = touches["stop"], touches["t1"], touches["t2"]

    # === R2-style resolution (our live contract) ===
    # TP2 first, else SL, else EOD
    if ts_t2 is not None and (ts_stop is None or ts_t2 <= ts_stop):
        exit_price = t2
        exit_ts = ts_t2
        exit_reason = "TP2"
    elif ts_stop is not None and (ts_t2 is None or ts_stop < ts_t2):
        exit_price = stop
        exit_ts = ts_stop
        exit_reason = "SL"
    else:
        exit_price = float(w09["Close"].iloc[-1])
        exit_ts = w09["DateTime"].iloc[-1]
        exit_reason = "EOD"

    entry_ts = w09["DateTime"].iloc[0]

    if long_side:
        pnl_rs = (exit_price - entry) * qty
    else:
        pnl_rs = (entry - exit_price) * qty

    pnl_r = pnl_rs / planned_risk if planned_risk else 0.0

    return pnl_rs, pnl_r, exit_reason, entry_ts, exit_ts, exit_price
