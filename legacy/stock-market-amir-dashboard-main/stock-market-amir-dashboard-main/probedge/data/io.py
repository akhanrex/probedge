import os
import pandas as pd
import numpy as np
from functools import lru_cache

DEFAULT_TAG_COLS = [
    "PrevDayContext",
    "OpenLocation",
    "FirstCandleType",
    "OpeningTrend",
    "RangeStatus",
]


@lru_cache(maxsize=16)
def load_master(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    _ = (os.path.getmtime(path), os.path.getsize(path))  # cache key side-effects
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if "Date" not in df.columns:
        return pd.DataFrame()

    def parse_date(x):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                return pd.to_datetime(x, format=fmt)
            except Exception:
                continue
        return pd.to_datetime(x, errors="coerce")

    df["Date"] = df["Date"].apply(parse_date)
    df = df[df["Date"].notna()].copy()
    for c in DEFAULT_TAG_COLS + ["Result"]:
        if c not in df.columns:
            df[c] = np.nan
    return df


def precompute_master(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "g": pd.DataFrame(),
            "oh": pd.DataFrame(),
            "dates": pd.Series(dtype="datetime64[ns]"),
        }
    g = df.copy()
    g["Date"] = pd.to_datetime(g["Date"], errors="coerce")
    g = g[g["Date"].notna()].reset_index(drop=True)
    # Canon columns (C_*) and a compact signature for the 5 tags
    for c in DEFAULT_TAG_COLS:
        if c not in g.columns:
            g[c] = np.nan
        g[f"{c}_C"] = g[c].apply(
            lambda v: (str(v).strip().upper() if pd.notna(v) else "∅")
        )
    g["sig5"] = (
        g[[f"{c}_C" for c in DEFAULT_TAG_COLS]].astype(str).agg("|".join, axis=1)
    )
    # Result onehot will be computed in core later; keep placeholder
    return {"g": g, "oh": pd.DataFrame(index=g.index), "dates": g["Date"]}
