from __future__ import annotations

from pathlib import Path
import pandas as pd


def main():
    fills_path = Path("data/journal/fills.csv")
    if not fills_path.exists():
        print(f"[fills_to_daily] No fills.csv found at {fills_path}")
        return

    df = pd.read_csv(fills_path)

    if df.empty:
        print("[fills_to_daily] fills.csv is empty, nothing to do.")
        return

    # --- Normalize day to date ---
    df["day"] = pd.to_datetime(df["day"]).dt.date

    # --- Drop duplicate fills (in case run_range was executed multiple times) ---
    dedup_cols = [
        "day",
        "mode",
        "symbol",
        "side",
        "qty",
        "entry",
        "stop",
        "target1",
        "target2",
    ]
    before = len(df)
    df = df.drop_duplicates(subset=dedup_cols, keep="last").reset_index(drop=True)
    after = len(df)
    print(f"[fills_to_daily] Deduped fills rows: {before} -> {after}")

    # --- Save a cleaned fills snapshot ---
    clean_path = Path("data/journal/fills_clean.csv")
    df.to_csv(clean_path, index=False)
    print(f"[fills_to_daily] Wrote cleaned fills to {clean_path}")

    # --- Build day-wise portfolio PnL ---
    daily = (
        df.groupby("day", as_index=False)
        .agg(
            trades=("symbol", "count"),
            portfolio_pnl_r1=("pnl_r1", "sum"),
            portfolio_pnl_r2=("pnl_r2", "sum"),
        )
        .sort_values("day")
    )

    # Your backtests for this slice used 10k risk/day
    DAILY_RISK_RS = 10_000.0
    daily["portfolio_R2"] = daily["portfolio_pnl_r2"] / DAILY_RISK_RS

    out_path = Path("data/journal/fills_daily.csv")
    daily.to_csv(out_path, index=False)
    print(f"[fills_to_daily] Wrote daily summary to {out_path}")
    print("\n=== HEAD ===")
    print(daily.head())
    print("\n=== TAIL ===")
    print(daily.tail())


if __name__ == "__main__":
    main()
