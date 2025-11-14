import pandas as pd
import numpy as np

def read_tm5_csv(path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize DateTime + OHLC
    cols = {c.lower(): c for c in df.columns}
    dt = None
    for key in ("datetime","date_time","timestamp","date"):
        if key in cols:
            dt = pd.to_datetime(df[cols[key]], errors="coerce"); break
    if dt is None and ("date" in cols and "time" in cols):
        dt = pd.to_datetime(df[cols["date"]].astype(str) + " " + df[cols["time"]].astype(str), errors="coerce")
    if dt is None:
        raise ValueError("No recognizable datetime")
    if "DateTime" in df.columns:
        df["DateTime"] = dt
    else:
        df.insert(0, "DateTime", dt)
    for k in ("Open","High","Low","Close","Volume"):
        if k in df.columns: df[k] = pd.to_numeric(df[k], errors="coerce")
    df = df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime").reset_index(drop=True)
    df["Date"]  = df["DateTime"].dt.normalize()
    df["_mins"] = df["DateTime"].dt.hour*60 + df["DateTime"].dt.minute
    return df

def by_day_map(df_tm5: pd.DataFrame):
    return {d: g.sort_values("DateTime").reset_index(drop=True) for d, g in df_tm5.groupby("Date")}
