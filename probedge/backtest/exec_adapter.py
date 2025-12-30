
from __future__ import annotations

from typing import Dict
from pathlib import Path
from datetime import time as dtime
import numpy as np
import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger
from probedge.storage.resolver import ALIASES as SYMBOL_ALIASES


def _intraday_root() -> Path:
    """
    Resolve intraday dir from DATA_DIR, supporting both:

      - <DATA_DIR>/data/intraday   (normal live layout)
      - <DATA_DIR>/intraday        (backtest layout)
    """
    root = Path(SETTINGS.data_dir)
    candidates = [
        root / "data" / "intraday",
        root / "intraday",
    ]
    for p in candidates:
        if p.exists() and p.is_dir():
            return p
    # fallback: first candidate, even if it doesn't exist yet
    return candidates[0]


def _storage_symbol(sym: str) -> str:
    s = str(sym).upper()
    # e.g. "TATAMOTORS" -> "TMPV" from ALIASES
    return SYMBOL_ALIASES.get(s, s)


def _tm5_path_for_symbol(sym: str) -> Path:
    storage_sym = _storage_symbol(sym)
    return _intraday_root() / f"{storage_sym}_5minute.csv"


def _load_tm5_for_day(symbol: str, day_date: pd.Timestamp) -> pd.DataFrame:
    """
    Load 5-min intraday for a symbol and slice to the specific day.
    Uses the same reader + alias mapping as live planner.
    """
    path = _tm5_path_for_symbol(symbol)
    log.info(
        "[exec_adapter] loading intraday tm5 for %s (storage=%s) from %s",
        symbol,
        _storage_symbol(symbol),
        path,
    )

    df_all = _read_tm5(str(path))


    dseries = pd.to_datetime(df_all["Date"], errors="coerce").dt.normalize()
    mask = dseries.eq(pd.to_datetime(day_date).normalize())
    df_day = df_all.loc[mask].copy()
    return df_day



log = get_logger(__name__)

# Match Colab backtest window: 09:40 → 15:05 (IST wall-clock)
T0_M = 9 * 60 + 40  # 09:40
T1_M = 15 * 60 + 5  # 15:05


def _read_tm5(path: str) -> pd.DataFrame:
    """
    Robust 5-minute reader, aligned with the new minute_to_tm5 output.

    Expected format (from apps.runtime.minute_to_tm5):
        Date,Open,High,Low,Close,Volume,DateTime,_mins

    - Keeps IST wall-clock (drops timezone info, no UTC shift)
    - Rebuilds Date and _mins from DateTime
    """
    log.info("[exec_adapter] reading TM5 from %s", path)
    df = pd.read_csv(path)

    # Clean column names
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]

    # === Fast path: new TM5 format we generate ourselves ===
    required = {"Date", "DateTime", "Open", "High", "Low", "Close"}
    if required.issubset(df.columns):
        # Parse DateTime (keep wall-clock, drop tz)
        dt = pd.to_datetime(df["DateTime"], errors="coerce")
        if getattr(dt.dtype, "tz", None) is not None:
            # Drop timezone but KEEP local time (09:15 stays 09:15)
            dt = dt.dt.tz_localize(None)
        df["DateTime"] = dt

        # Parse Date, but we will recompute it from DateTime anyway
        d = pd.to_datetime(df["Date"], errors="coerce")
        if getattr(d.dtype, "tz", None) is not None:
            d = d.dt.tz_localize(None)
        df["Date"] = d

        # Ensure numeric OHLCV
        for k in ("Open", "High", "Low", "Close", "Volume"):
            if k in df.columns:
                df[k] = pd.to_numeric(df[k], errors="coerce")

        # Drop bad rows & sort
        df = (
            df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
            .sort_values("DateTime")
            .reset_index(drop=True)
        )

        # Canonical Date (naive) and minute-of-day
        df["Date"] = df["DateTime"].dt.normalize()
        df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute

        return df

    # === Fallback path (older / split date+time formats, if any) ===
    lc2orig = {c.lower(): c for c in df.columns}

    dt = None

    # 1) Preferred: separate Date + Time columns → combine them
    if "date" in lc2orig and "time" in lc2orig:
        date_col = lc2orig["date"]
        time_col = lc2orig["time"]
        combined = (
            df[date_col].astype(str).str.strip()
            + " "
            + df[time_col].astype(str).str.strip()
        )
        dt = pd.to_datetime(combined, errors="coerce")

    # 2) Else try an existing datetime-like column
    if dt is None:
        for key in ("datetime", "date_time", "timestamp"):
            if key in lc2orig:
                dt = pd.to_datetime(df[lc2orig[key]], errors="coerce")
                break

    # 3) Fallback: treat "date" as full datetime
    if dt is None and "date" in lc2orig:
        dt = pd.to_datetime(df[lc2orig["date"]], errors="coerce")

    if dt is None:
        raise RuntimeError(f"No recognizable datetime columns in intraday file: {path}")

    # Drop timezone but keep local wall-clock
    if getattr(dt.dtype, "tz", None) is not None:
        dt = dt.dt.tz_localize(None)

    # Insert / overwrite DateTime
    if "DateTime" in df.columns:
        df["DateTime"] = dt
    else:
        df.insert(0, "DateTime", dt)

    # Map OHLCV with some aliases
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

    for k in ("Open", "High", "Low", "Close", "Volume"):
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce")

    df = (
        df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
        .sort_values("DateTime")
        .reset_index(drop=True)
    )

    # Canonical Date + minute-of-day
    df["Date"] = df["DateTime"].dt.normalize()
    df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute

    return df


def _slice_window_fast(df_day: pd.DataFrame, m0: int, m1: int) -> pd.DataFrame:
    """Slice a single-day TM5 frame between minute offsets [m0, m1]."""
    if df_day is None or df_day.empty:
        return pd.DataFrame()

    # Safety: rebuild _mins if missing or all NaN
    if "_mins" not in df_day.columns or df_day["_mins"].isna().all():
        df_day = df_day.copy()
        dt = pd.to_datetime(df_day["DateTime"], errors="coerce")
        df_day["_mins"] = dt.dt.hour * 60 + dt.dt.minute

    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    return df_day.loc[m, ["DateTime", "Open", "High", "Low", "Close", "Date"]]



def _earliest_touch_times(
    win: pd.DataFrame, long: bool, stop: float, t1: float, t2: float
) -> Dict[str, pd.Timestamp | None]:
    """
    Earliest times price touches stop, T1, T2 in 09:40→15:05.
    Mirrors the Colab batch backtest logic.
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
    Load intraday 5-min data for a symbol using the same path pattern
    as the rest of the app, then parse it with our TM5 reader.
    """
    pattern = SETTINGS.paths.intraday or "data/intraday/{sym}_5minute.csv"
    path = pattern.format(sym=symbol)
    log.info("[exec_adapter] loading intraday tm5 for %s from %s", symbol, path)
    return _read_tm5(path)


def simulate_trade_colab_style(trade_row, intraday_raw=None):
    """
    Simulate a single trade using the SAME R1/R2 resolution logic
    as the Colab batch backtest, but reading TM5 via the live
    storage resolver + read_tm5_csv (with symbol aliases).

    Returns:
        (pnl_r1, pnl_r2, r1_result, r2_result, hit_times_dict)
    """
    # --- 1) Extract journal fields ---
    symbol = str(trade_row["symbol"])
    day = pd.to_datetime(trade_row["day"]).normalize()
    day_date = day.date()

    side = str(trade_row["side"]).upper()
    long_side = side in ("BUY", "BULL")

    qty = int(trade_row["qty"])
    entry = float(trade_row["entry"])
    stop = float(trade_row["stop"])
    t1 = float(trade_row["target1"])
    t2 = float(trade_row["target2"])

    # --- 2) Load TM5 slice for that calendar day (live-style) ---
    day_df = _load_tm5_for_day(symbol, pd.Timestamp(day_date))

    if day_df.empty:
        log.error(
            "[exec_adapter] No TM5 rows for %s on %s. Treating as SKIP with 0 PnL.",
            symbol,
            day_date,
        )
        return (
            0.0,  # pnl_r1
            0.0,  # pnl_r2
            "SKIP",  # r1_result
            "SKIP",  # r2_result
            {"stop": None, "t1": None, "t2": None},
        )


    # --- 3) Slice 09:40→15:05 window (Colab backtest window) ---
    w09 = _slice_window_fast(day_df, T0_M, T1_M)
    if w09.empty:
        # Soft-fail: treat as "no tradable window" instead of crashing the day
        # so other symbols still execute and backtest keeps going cleanly.
        log.error(
            "[exec_adapter] Empty 09:40–15:05 window for %s on %s (rows=%d). "
            "Treating as SKIP with 0 PnL.",
            symbol,
            day_date,
            len(day_df),
        )
        return (
            0.0,  # pnl_r1
            0.0,  # pnl_r2
            "SKIP",  # r1_result
            "SKIP",  # r2_result
            {"stop": None, "t1": None, "t2": None},
        )


    # --- 4) Determine earliest touches of stop/T1/T2 ---
    touches = _earliest_touch_times(w09, long_side, stop, t1, t2)
    ts_stop, ts_t1, ts_t2 = touches["stop"], touches["t1"], touches["t2"]

    # Risk per share from entry/stop
    risk_per_share = (entry - stop) if long_side else (stop - entry)
    if not np.isfinite(risk_per_share) or risk_per_share <= 0:
        raise RuntimeError(f"Bad risk_per_share for {symbol} on {day_date}")

    # --- 5) R1 PnL (1R target) ---
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
        exit_px = float(w09["Close"].iloc[-1])
        pnl_r1 = qty * (exit_px - entry) if long_side else qty * (entry - exit_px)
        r1 = "EOD"

    # --- 6) R2 PnL (2R target) ---
    if ts_t2 is not None and (ts_stop is None or ts_t2 <= ts_stop):
        pnl_r2 = qty * (t2 - entry) if long_side else qty * (entry - t2)
        r2 = "WIN"
    elif ts_stop is not None and (ts_t2 is None or ts_stop < ts_t2):
        pnl_r2 = -qty * risk_per_share
        r2 = "LOSS"
    else:
        exit_px = float(w09["Close"].iloc[-1])
        pnl_r2 = qty * (exit_px - entry) if long_side else qty * (entry - exit_px)
        r2 = "EOD"

    return pnl_r1, pnl_r2, r1, r2, touches

