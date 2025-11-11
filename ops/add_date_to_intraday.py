from pathlib import Path
import pandas as pd

INTRA = Path("data/intraday")
INTRA.mkdir(parents=True, exist_ok=True)

def ensure_date_col(csv_path: Path):
    df = pd.read_csv(csv_path)
    if "DateTime" not in df.columns:
        print(f"[SKIP] {csv_path} (no DateTime)")
        return
    # Parse and normalize to IST date
    dt = pd.to_datetime(df["DateTime"], errors="coerce", utc=True)
    # If it wasn't timezone-aware, treat it as IST
    if dt.dt.tz is None:
        dt = pd.to_datetime(df["DateTime"], errors="coerce").dt.tz_localize("Asia/Kolkata")
    else:
        dt = dt.dt.tz_convert("Asia/Kolkata")
    df["Date"] = dt.dt.tz_localize(None).dt.normalize()
    df.to_csv(csv_path, index=False)
    print(f"[OK] {csv_path} → added/updated Date")

def main():
    for p in sorted(INTRA.glob("*_5minute.csv")):
        ensure_date_col(p)

if __name__ == "__main__":
    main()
