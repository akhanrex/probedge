from __future__ import annotations
import pandas as pd
from typing import Optional, List
from pathlib import Path
import os
from apps.infra.settings import SETTINGS

def _first_existing(path_templates: List[str], sym: str) -> Optional[str]:
    for tpl in path_templates:
        p = tpl.format(DATA_DIR=SETTINGS.data_dir, sym=sym)
        if os.path.exists(p):
            return p
    return None

def resolve_tm5_path(sym: str) -> Optional[str]:
    return _first_existing(SETTINGS.paths.intraday_patterns, sym)

def resolve_master_path(sym: str) -> Optional[str]:
    return _first_existing(SETTINGS.paths.master_patterns, sym)

def read_tm5(sym: str) -> pd.DataFrame:
    p = resolve_tm5_path(sym)
    if not p:
        raise FileNotFoundError(f"tm5 not found for {sym} in {SETTINGS.paths.intraday_patterns}")
    df = pd.read_csv(p)
    # Try common datetime column names
    if "DateTime" in df.columns:
        dt = pd.to_datetime(df["DateTime"], errors="coerce")
    elif "Datetime" in df.columns:
        dt = pd.to_datetime(df["Datetime"], errors="coerce")
    elif "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce")
    else:
        # Combine if have Date + Time
        if {"Date","Time"}.issubset(df.columns):
            dt = pd.to_datetime(df["Date"] + " " + df["Time"], errors="coerce")
        else:
            raise ValueError("Could not infer DateTime column in tm5 CSV")
    df["DateTime"] = dt
    # Standardize OHLCV names if present in various cases
    rename_map = {}
    for k in ["Open","High","Low","Close","Volume"]:
        for c in df.columns:
            if c.lower()==k.lower() and c!=k:
                rename_map[c] = k
    if rename_map:
        df = df.rename(columns=rename_map)
    # Ensure numeric
    for k in ("Open","High","Low","Close","Volume"):
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce")
    df = df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime").reset_index(drop=True)
    df["Date"] = df["DateTime"].dt.date
    return df

def read_master(sym: str) -> pd.DataFrame:
    p = resolve_master_path(sym)
    if not p:
        raise FileNotFoundError(f"master not found for {sym} in {SETTINGS.paths.master_patterns}")
    df = pd.read_csv(p)
    # Normalize Date
    date_col = None
    for c in ["Date","DATE","TradeDate","date"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        raise ValueError("Could not find 'Date' column in master")
    df["Date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    return df

def read_journal() -> pd.DataFrame:
    p = SETTINGS.paths.journal_csv.format(DATA_DIR=SETTINGS.data_dir)
    if not os.path.exists(p):
        raise FileNotFoundError(p)
    df = pd.read_csv(p)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    return df

def read_state_json() -> dict:
    p = SETTINGS.paths.state_json.format(DATA_DIR=SETTINGS.data_dir)
    if not os.path.exists(p):
        raise FileNotFoundError(p)
    import json
    with open(p, "r") as f:
        return json.load(f)
