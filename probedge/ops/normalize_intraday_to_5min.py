# ops/normalize_intraday_to_5min.py

import pandas as pd
from pathlib import Path
from probedge.infra.settings import SETTINGS

INTRA_DIR = Path(getattr(SETTINGS.paths, "intraday", "data/intraday"))

def normalize_symbol(sym: str) -> None:
    path = INTRA_DIR / f"{sym}_5minute.csv"
    if not path.exists():
        print(f"[{sym}] no intraday file at {path}, skipping")
        return

    print(f"[{sym}] normalizing {path} -> pure 5-minute bars")
    df = pd.read_csv(path)
    if df.empty:
        print(f"[{sym}] empty file, skipping")
        return

    # --- parse DateTime safely, handle +05:30 tz, then drop tz to make it naive local ---
    dt = pd.to_datetime(df["DateTime"], errors="coerce")
    if hasattr(dt.dt, "tz") and dt.dt.tz is not None:
        dt = dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    df["DateTime_local"] = dt
    df = df.dropna(subset=["DateTime_local"])
    df = df.sort_values("DateTime_local")

    # --- floor to 5-minute buckets and aggregate ---
    df["bucket"] = df["DateTime_local"].dt.floor("5min")

    grouped = df.groupby("bucket", sort=True)

    out = grouped.agg(
        Open=("Open", "first"),
        High=("High", "max"),
        Low=("Low", "min"),
        Close=("Close", "last"),
        Volume=("Volume", "sum"),
    ).reset_index().rename(columns={"bucket": "DateTime"})

    out["Date"] = out["DateTime"].dt.normalize()

    # --- format back to your standard strings ---
    out["DateTime"] = out["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")

    out = out[["DateTime", "Open", "High", "Low", "Close", "Volume", "Date"]]

    # overwrite original file
    out.to_csv(path, index=False)
    print(f"[{sym}] wrote {len(out)} 5-min bars → {path}")

def main():
    for sym in SETTINGS.symbols:
        normalize_symbol(sym)

if __name__ == "__main__":
    main()
