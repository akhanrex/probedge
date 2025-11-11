import os
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "./data")
TM5_DIR  = os.path.join(DATA_DIR, "tm5")
os.makedirs(TM5_DIR, exist_ok=True)

COLUMNS = ["start_ts","end_ts","Open","High","Low","Close"]

def _ensure(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=COLUMNS)
    return df

def append_bar(symbol: str, closed_bar: dict):
    path = os.path.join(TM5_DIR, f"{symbol}.parquet")
    row = pd.DataFrame([{
        "start_ts": closed_bar.get("start_ts"),
        "end_ts":   closed_bar.get("end_ts"),
        "Open":     float(closed_bar.get("Open")),
        "High":     float(closed_bar.get("High")),
        "Low":      float(closed_bar.get("Low")),
        "Close":    float(closed_bar.get("Close")),
    }])
    if os.path.exists(path):
        old = pd.read_parquet(path)
        df = pd.concat([old, row], ignore_index=True)
    else:
        df = row
    df = df.sort_values("end_ts").drop_duplicates(subset=["end_ts"], keep="last").reset_index(drop=True)
    df.to_parquet(path, index=False)

def read_all(symbol: str) -> pd.DataFrame:
    path = os.path.join(TM5_DIR, f"{symbol}.parquet")
    if not os.path.exists(path):
        return _ensure(None)
    return _ensure(pd.read_parquet(path))
