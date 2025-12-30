# apps/runtime/rebuild_masters_from_intraday_simple.py

from pathlib import Path
import pandas as pd
from probedge.infra.settings import SETTINGS
from probedge.storage.resolver import ALIASES

DATA_DIR = SETTINGS.data_dir
INTRADAY_DIR = DATA_DIR / "data" / "intraday"
MASTERS_DIR = DATA_DIR / "data" / "masters"

SESSION_START = "09:15:00"
SESSION_END = "15:30:00"  # keep full session; we won't trim here


def build_daily_ohlc(sym: str) -> pd.DataFrame:
    """
    Build a clean daily OHLC from intraday 5-minute file:
    cols: date,time,open,high,low,close,volume
    """
    # logical symbol (e.g. TATAMOTORS) -> physical file symbol (e.g. TMPV)
    physical = ALIASES.get(sym, sym)
    intraday_path = INTRADAY_DIR / f"{physical}_5minute.csv"

    if not intraday_path.exists():
        raise FileNotFoundError(f"Missing intraday file for {sym}: {intraday_path}")

    print(f"[masters] {sym}: reading {intraday_path}")
    df = pd.read_csv(intraday_path)

    # Normalise column names
    cols = {c.lower(): c for c in df.columns}
    # Expect at least these
    for k in ["date", "time", "open", "high", "low", "close", "volume"]:
        if k not in cols:
            raise ValueError(f"{intraday_path} missing column {k}")

    df["date"] = pd.to_datetime(df[cols["date"]]).dt.date
    df["time"] = df[cols["time"]].astype(str)

    # Restrict to the normal trading session window if you want (optional)
    # Here we keep everything, but you could do:
    # df = df[(df["time"] >= SESSION_START) & (df["time"] <= SESSION_END)]

    # Group by date and build OHLC
    grouped = df.groupby("date")
    out = pd.DataFrame({
        "Date": grouped["date"].first(),  # just the date
        "DayHigh": grouped[cols["high"]].max(),
        "DayLow": grouped[cols["low"]].min(),
        "Open": grouped[cols["open"]].first(),
        "High": grouped[cols["high"]].max(),
        "Low": grouped[cols["low"]].min(),
        "Close": grouped[cols["close"]].last(),
    }).reset_index(drop=True)

    # Empty tag columns for now – we will re-tag later with classifier
    out.insert(1, "OpeningTrend", "")
    out.insert(2, "Result", "")
    out.insert(3, "OpenLocation", "")
    out.insert(4, "FirstCandleType", "")
    out.insert(5, "RangeStatus", "")
    out.insert(6, "PrevDayContext", "")

    # Ensure column order
    out = out[
        [
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
    ]

    return out.sort_values("Date").reset_index(drop=True)


def rebuild_master_for_symbol(sym: str):
    master_path = MASTERS_DIR / f"{physical}_5MINUTE_MASTER.csv"
    master_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[masters] {sym}: rebuilding master at {master_path}")
    out = build_daily_ohlc(sym)
    out.to_csv(master_path, index=False)
    print(f"[masters] {sym}: wrote {len(out)} rows")


def main():
    print("[masters] Rebuilding masters from intraday for all symbols...")
    for sym in SETTINGS.symbols:
        rebuild_master_for_symbol(sym)
    print("[masters] Done.")


if __name__ == "__main__":
    main()

