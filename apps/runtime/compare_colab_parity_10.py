from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


def load_colab_daily_scaled() -> pd.DataFrame:
    """
    Read data/backtest/colab_all_10stocks.csv (trimmed universe) and
    compute the *expected* portfolio R2 under your current live logic:

      - Colab PNL_R2 is per-symbol with 10k risk per symbol.
      - Live system uses 10k DAILY RISK split equally.
      - So live-equivalent portfolio_R2 for a day is:

          portfolio_R2 = sum(PNL_R2_colab for active symbols)
                         / (active_trades * 10_000)

      where active_trades = number of symbols actually traded that day.
    """
    path = Path("data/backtest/colab_all_10stocks.csv")
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run colab_prepare_10_universe first."
        )

    df = pd.read_csv(path)

    # --- 1) Normalize day ---
    if "Date" not in df.columns:
        raise RuntimeError("Expected a 'Date' column in colab_all_10stocks.csv")

    df["day"] = pd.to_datetime(df["Date"]).dt.normalize()

    # --- 2) Identify active trades (non-ABSTAIN) ---
    # Colab has Pick + Skip. We consider a row active if Skip != 'ABSTAIN'.
    skip_col = "Skip" if "Skip" in df.columns else None
    if skip_col is None:
        # Fallback: treat all as active
        df["is_active"] = True
    else:
        df["is_active"] = ~df[skip_col].fillna("").astype(str).str.upper().eq("ABSTAIN")

    # PNL_R2 column (per-symbol, per-day)
    pnl_col = None
    for cand in ("PNL_R2", "pnl_r2", "Pnl_R2"):
        if cand in df.columns:
            pnl_col = cand
            break
    if pnl_col is None:
        raise RuntimeError("Could not find a PNL_R2 column in Colab CSV.")

    # --- 3) Group by day and compute sums ---
    grp = df.groupby("day", as_index=False)

    daily = grp.agg(
        colab_sum_pnl_r2=(pnl_col, "sum"),
        colab_active_trades=("is_active", "sum"),
    )

    # colab_active_trades is a float because of agg; cast to int
    daily["colab_active_trades"] = daily["colab_active_trades"].astype(int)

    # We will *not* scale here yet; we’ll scale using the system trades
    # when we join with fills_daily.
    return daily


def load_system_daily() -> pd.DataFrame:
    """
    Read data/journal/fills_daily.csv and clean it.
    This file comes from apps.runtime.fills_to_daily.
    """
    path = Path("data/journal/fills_daily.csv")
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run apps.runtime.fills_to_daily first."
        )

    df = pd.read_csv(path)

    # Drop any bogus header-rows that slipped in as data (e.g. 'day' row)
    day_parsed = pd.to_datetime(df["day"], errors="coerce")
    df = df[day_parsed.notna()].copy()
    df["day"] = day_parsed[day_parsed.notna()].dt.normalize()

    # Ensure numeric columns
    for col in ("trades", "portfolio_pnl_r1", "portfolio_pnl_r2", "portfolio_R2"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("day").reset_index(drop=True)
    return df


def main():
    colab_daily = load_colab_daily_scaled()
    system_daily = load_system_daily()

    # Inner join on day so we only compare overlapping days
    merged = pd.merge(colab_daily, system_daily, on="day", how="inner")

    if merged.empty:
        print("[compare_parity] No overlapping days between Colab and system.")
        return

    # Live system uses DAILY_RISK = 10_000 (from our design).
    DAILY_RISK = 10_000.0

    # Compute expected portfolio_R2 from Colab PNL using *system trade count*
    # Formula:
    #   portfolio_R2_expected = sum(PNL_R2_colab) / (system_trades * DAILY_RISK)
    # This matches: system risk per trade = DAILY_RISK / system_trades.
    merged["system_trades"] = merged["trades"].astype(int)
    merged["colab_portfolio_R2_expected"] = (
        merged["colab_sum_pnl_r2"]
        / (merged["system_trades"] * DAILY_RISK)
        .replace({0: np.nan})
    )

    merged["R2_diff"] = merged["portfolio_R2"] - merged["colab_portfolio_R2_expected"]

    print("=== Colab vs System Daily Parity (10-stock universe) ===")
    cols_show = [
        "day",
        "colab_active_trades",
        "system_trades",
        "colab_sum_pnl_r2",
        "colab_portfolio_R2_expected",
        "portfolio_pnl_r2",
        "portfolio_R2",
        "R2_diff",
    ]
    # Some columns might not exist if names changed slightly; filter safely
    cols_show = [c for c in cols_show if c in merged.columns]

    print(merged[cols_show].to_string(index=False))

    # Quick health check
    tol = 1e-4
    max_abs_diff = merged["R2_diff"].abs().max()
    print()
    print(f"[compare_parity] Max |R2_diff| across days = {max_abs_diff:.6f}")
    if max_abs_diff < tol:
        print("[compare_parity] ✅ System matches Colab daily R2 within tolerance.")
    else:
        print("[compare_parity] ⚠ R2 differences above tolerance – inspect rows above.")


if __name__ == "__main__":
    main()
