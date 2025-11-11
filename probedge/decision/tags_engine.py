from pathlib import Path
import pandas as pd

def _intraday_path(symbol: str) -> Path:
    return Path("data/intraday") / f"{symbol}_5minute.csv"

def _master_path(symbol: str) -> Path:
    return Path("data/masters") / f"{symbol}_5MINUTE_MASTER.csv"

def _read_intraday(symbol: str) -> pd.DataFrame:
    p = _intraday_path(symbol)
    if not p.exists():
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Date"])

    df = pd.read_csv(p)

    # normalize column names case-insensitively
    cols = {c.lower(): c for c in df.columns}
    def has(*names): return any(n.lower() in cols for n in names)
    def col(*names):  return next(cols[n.lower()] for n in names if n.lower() in cols)

    # ensure DateTime exists
    if not has("DateTime","datetime","timestamp","date"):
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Date"])

    dtcol = col("DateTime","datetime","timestamp","date")
    ts = pd.to_datetime(df[dtcol], errors="coerce", utc=True)
    # If parse produced naive (all-naive), localize to Asia/Kolkata then convert to UTC
    if ts.dt.tz is None:
        ts = pd.to_datetime(df[dtcol], errors="coerce").dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    df["DateTime"] = ts.dt.tz_convert("Asia/Kolkata")  # work in IST for convenience
    df["Date"]     = df["DateTime"].dt.tz_localize(None).dt.normalize()

    # map OHLC with case-insensitive names (keep whatever exists)
    for want in ["Open","High","Low","Close","Volume"]:
        if has(want):
            df[want] = df[col(want)]

    keep = [c for c in ["DateTime","Open","High","Low","Close","Date"] if c in df.columns]
    out = (df[keep]
           .dropna(subset=["Date"])
           .drop_duplicates(["DateTime"], keep="last")
           .sort_values("DateTime")
           .reset_index(drop=True))
    return out

def _read_master(symbol: str) -> pd.DataFrame:
    p = _master_path(symbol)
    if not p.exists():
        return pd.DataFrame(columns=["Date","OpeningTrend","OpenLocation","PrevDayContext","Result"])
    df = pd.read_csv(p)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    for col in ("OpeningTrend","OpenLocation","PrevDayContext","Result"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper().replace({"NAN": ""})
    return (df.dropna(subset=["Date"])
              .drop_duplicates("Date", keep="last")
              .sort_values("Date")
              .reset_index(drop=True))
