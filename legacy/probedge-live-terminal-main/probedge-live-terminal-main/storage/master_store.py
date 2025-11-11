# storage/master_store.py
import os
import pandas as pd
from typing import List, Optional

BASE = os.getenv("DATA_DIR", "./data")
MASTER_DIR = os.path.join(BASE, "master")
TM5_DIR = os.path.join(BASE, "tm5")  # optional, if you keep 5-min csvs per symbol here

def master_path(sym: str) -> str:
    return os.path.join(MASTER_DIR, f"{sym}_Master.csv")

def read_master_headers(sym: str) -> List[str]:
    """Return the existing header list from <symbol>_Master.csv if present, else []."""
    p = master_path(sym)
    if not os.path.exists(p):
        return []
    try:
        df = pd.read_csv(p, nrows=0)
        return list(df.columns)
    except Exception:
        return []

def read_master(sym: str, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    p = master_path(sym)
    if not os.path.exists(p):
        return pd.DataFrame()
    try:
        return pd.read_csv(p, usecols=usecols)
    except Exception:
        return pd.DataFrame()

def tm5_path(sym: str) -> str:
    # 5-min parquet maintained by tm5min_store
    return os.path.join(TM5_DIR, f"{sym}.parquet")

def read_tm5(sym: str) -> pd.DataFrame:
    p = tm5_path(sym)
    if not os.path.exists(p):
        return pd.DataFrame()
    try:
        df = pd.read_parquet(p)
        # create DateTime from end_ts (seconds) if needed
        if "DateTime" not in df.columns and "end_ts" in df.columns:
            df["DateTime"] = pd.to_datetime(df["end_ts"], unit="s")
        else:
            df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()

# (normalized) — keep using MASTER_DIR/<SYMBOL>_Master.csv defined above

def _try_cols(df: pd.DataFrame, pairs: dict) -> dict:
    """Return only keys that exist in df.columns (case sensitive)."""
    return {k: v for k, v in pairs.items() if k in df.columns}

def update_master_tags(symbol: str, day_norm: pd.Timestamp, tags: dict) -> bool:
    """
    Updates/sets tag columns for a given day in the symbol Master CSV.
    Only modifies columns that already exist in the master file.
    Returns True if file was updated.
    """
    p = master_path(symbol)
    if not os.path.exists(p):
        return False
    df = pd.read_csv(p)
    if "Date" not in df.columns:
        return False
    # normalize Date
    try:
        dcol = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    except Exception:
        return False

    # filter tags to existing columns
    tags = _try_cols(df, tags)
    if not tags:
        return False

    # NEW: ensure tag columns are object dtype so writing "TR"/"BULL" is safe
    for k in tags.keys():
        try:
            df[k] = df[k].astype("object")
        except Exception:
            pass

    # locate the day row(s)
    day_norm = pd.to_datetime(day_norm).normalize()
    mask = dcol.eq(day_norm)
    if not mask.any():
        # if day not present, append a new row with Date and tags (others NaN)
        new_row = {c: None for c in df.columns}
        new_row["Date"] = day_norm.strftime("%Y-%m-%d")
        for k, v in tags.items():
            new_row[k] = v
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        idx = df.index[mask]
        for k, v in tags.items():
            df.loc[idx, k] = v

    df.to_csv(p, index=False)
    return True

def most_recent_day(df_intraday: pd.DataFrame):
    if df_intraday is None or df_intraday.empty:
        return None
    d = pd.to_datetime(df_intraday["DateTime"], errors="coerce").dropna().dt.normalize()
    if d.empty: return None
    return d.iloc[-1]
