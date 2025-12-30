# apps/runtime/rebuild_masters_from_intraday.py
#
# Rebuild last N days of master tags from clean 5-minute intraday,
# using the robust classifiers in probedge.core.classifiers.
#
# - Keeps old history before cutoff_date.
# - Recomputes daily tags for [cutoff_date .. last intraday day].
# - Writes back to data/masters/{SYM}_5MINUTE_MASTER.csv
#
# Safe aliasing:
#   logical "TATAMOTORS" -> files "TMPV_5minute.csv" / "TMPV_5MINUTE_MASTER.csv"

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.core.classifiers import (
    prev_trading_day_ohlc,
    compute_prevdaycontext_robust,
    compute_openingtrend_robust,
    compute_result_0940_1505,
    compute_openlocation_from_df,
    compute_first_candletype,
    compute_rangestatus,
)

# How many days to rebuild
DAYS_BACK = 180

# Logical -> file symbol mapping
ALIASES: Dict[str, str] = {
    "TATAMOTORS": "TMPV",
}


def resolve_file_symbol(sym: str) -> str:
    """Map logical symbol to actual file symbol."""
    return ALIASES.get(sym, sym)


def load_intraday(sym: str) -> pd.DataFrame:
    """
    Load intraday 5-minute data for a symbol and return a DataFrame with:
      DateTime, Open, High, Low, Close, Volume
    built from data/intraday/{SYM}_5minute.csv (cleaned format).
    """
    file_sym = resolve_file_symbol(sym)
    intraday_path = SETTINGS.data_dir / "data" / "intraday" / f"{file_sym}_5minute.csv"
    if not intraday_path.exists():
        raise FileNotFoundError(f"Missing intraday file for {sym}: {intraday_path}")

    df = pd.read_csv(intraday_path)

    # Expect headers: date,time,open,high,low,close,volume
    if not {"date", "time", "open", "high", "low", "close"}.issubset(df.columns):
        raise ValueError(f"Unexpected columns in {intraday_path}: {df.columns.tolist()}")

    dt = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")

    out = pd.DataFrame(
        {
            "DateTime": dt,
            "Open": df["open"].astype(float),
            "High": df["high"].astype(float),
            "Low": df["low"].astype(float),
            "Close": df["close"].astype(float),
            "Volume": df.get("volume", 0).astype(float),
        }
    )
    out = out.dropna(subset=["DateTime"])
    out = out.sort_values("DateTime").reset_index(drop=True)
    out["Day"] = out["DateTime"].dt.normalize()
    return out


def build_daily_tags(sym: str, df: pd.DataFrame, cutoff_day: pd.Timestamp) -> pd.DataFrame:
    """
    From full intraday df (DateTime, Open, High, Low, Close, Volume, Day),
    build daily tag rows for all days >= cutoff_day.

    Output columns:
      Date,OpeningTrend,Result,OpenLocation,FirstCandleType,
      RangeStatus,PrevDayContext,DayHigh,DayLow,Open,High,Low,Close
    """
    days = sorted(d for d in df["Day"].dropna().unique())
    if not days:
        return pd.DataFrame()

    rows: List[dict] = []

    for day in days:
        if day < cutoff_day:
            continue

        day_df = df[df["Day"].eq(day)].copy()
        if day_df.empty:
            continue

        # Prev-day OHLC from the same intraday df
        prev_ohlc = prev_trading_day_ohlc(df, day)

        # OpeningTrend (09:15–09:40)
        opening_trend = compute_openingtrend_robust(day_df)

        # Result (09:40–15:05)
        result_tag, _ = compute_result_0940_1505(day_df)

        # OpenLocation + FirstCandleType + RangeStatus need prev_ohlc
        if prev_ohlc:
            open_location = compute_openlocation_from_df(day_df, prev_ohlc)
            first_candle = compute_first_candletype(day_df, prev_ohlc)
            range_status = compute_rangestatus(day_df, open_location, prev_ohlc)
            prev_ctx = compute_prevdaycontext_robust(
                prev_ohlc["open"],
                prev_ohlc["high"],
                prev_ohlc["low"],
                prev_ohlc["close"],
            )
        else:
            open_location = ""
            first_candle = ""
            range_status = ""
            prev_ctx = ""

        # Daily OHLC
        day_high = float(day_df["High"].max())
        day_low = float(day_df["Low"].min())
        day_open = float(day_df.sort_values("DateTime")["Open"].iloc[0])
        day_close = float(day_df.sort_values("DateTime")["Close"].iloc[-1])

        rows.append(
            {
                "Date": day.date().isoformat(),
                "OpeningTrend": opening_trend,
                "Result": result_tag,
                "OpenLocation": open_location,
                "FirstCandleType": first_candle,
                "RangeStatus": range_status,
                "PrevDayContext": prev_ctx,
                "DayHigh": day_high,
                "DayLow": day_low,
                "Open": day_open,
                "High": day_high,
                "Low": day_low,
                "Close": day_close,
            }
        )

    return pd.DataFrame(rows)


def rebuild_master_for_symbol(sym: str) -> None:
    file_sym = resolve_file_symbol(sym)
    master_path: Path = SETTINGS.data_dir / "data" / "masters" / f"{file_sym}_5MINUTE_MASTER.csv"

    print(f"[masters] {sym}: rebuilding into {master_path}")

    df_intraday = load_intraday(sym)

    # Compute cutoff day (today - DAYS_BACK)
    cutoff_day = pd.Timestamp(date.today() - timedelta(days=DAYS_BACK))

    new_daily = build_daily_tags(sym, df_intraday, cutoff_day)
    if new_daily.empty:
        print(f"[masters] {sym}: no new daily rows to build (intraday empty or no days >= cutoff)")
        return

    new_daily["Date"] = pd.to_datetime(new_daily["Date"]).dt.date

    if master_path.exists():
        old = pd.read_csv(master_path)
        if "Date" not in old.columns:
            raise ValueError(f"[masters] {sym}: master missing Date column: {master_path}")
        old["Date"] = pd.to_datetime(old["Date"]).dt.date

        # Keep only rows strictly before cutoff_day
        cutoff_date = new_daily["Date"].min()
        old_keep = old[old["Date"] < cutoff_date].copy()
        merged = pd.concat([old_keep, new_daily], ignore_index=True)
    else:
        merged = new_daily

    merged = merged.sort_values("Date").reset_index(drop=True)
    merged["Date"] = merged["Date"].astype(str)

    # Enforce column order
    cols = [
        "Date",
        "OpeningTrend",
        "Result",
        "OpenLocation",
        "FirstCandleType",
        "RangeStatus",
        "PrevDayContext",
        "DayHigh",
        "DayLow",
        "Open",
        "High",
        "Low",
        "Close",
    ]
    merged = merged[cols]

    master_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(master_path, index=False)

    print(f"[masters] {sym}: wrote {len(merged)} rows (kept {len(merged) - len(new_daily)} old, {len(new_daily)} new)")


def main():
    # Use the logical universe from settings (TATAMOTORS, SBIN, RECLTD, ...).
    symbols = list(SETTINGS.symbols)
    print("[masters] Rebuilding masters from intraday for symbols:", symbols)
    for sym in symbols:
        try:
            rebuild_master_for_symbol(sym)
        except Exception as e:
            print(f"[masters] ERROR for {sym}: {e}")


if __name__ == "__main__":
    main()

