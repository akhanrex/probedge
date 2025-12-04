from __future__ import annotations
import os
import numpy as np
import pandas as pd
from probedge.infra.settings import SETTINGS

ALIASES = {}

def _resolve(sym: str) -> str:
    return ALIASES.get(sym.upper(), sym.upper())

def _path_for(sym: str) -> str:
    sym1 = _resolve(sym)
    return SETTINGS.paths.intraday.format(sym=sym1)

def read(symbol: str) -> pd.DataFrame:
    p = _path_for(symbol)
    if not os.path.exists(p):
        return pd.DataFrame(columns=["date","open","high","low","close","volume"])
    df = pd.read_csv(p)

    # normalize date column
    for c in ("Date","date","timestamp","Datetime","datetime"):
        if c in df.columns:
            df = df.rename(columns={c: "date"})
            break

    # safe stringify timestamps
    try:
        if "date" in df.columns:
            dt = pd.to_datetime(df["date"], errors="coerce")
            df["date"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            df["date"] = df["date"].astype(str)
        except Exception:
            pass

    # sanitize for JSON
    df = df.replace([np.inf, -np.inf], np.nan)
    return df
