from __future__ import annotations

import pandas as pd

# Fixed daily risk in both Colab and system
DAILY_RISK_RS = 10_000.0

# Logical 10-stock universe we care about
TEN_UNIVERSE = {
    "TMPV",
    "SBIN",
    "RECLTD",
    "JSWENERGY",
    "LT",
    "COALINDIA",
    "ABB",
    "LICI",
    "ETERNAL",
    "JIOFIN",
}

COLAB_PATH = "data/backtest/colab_all_10stocks.csv"
FILLS_PATH = "data/journal/fills.csv"


def _normalise_colab_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure we have a 'symbol' column in Colab data matching system fills.

    colab_prepare_10_universe already created a root-symbol style column for the
    10-stock universe (TMPV, SBIN, ...). We use it if present; otherwise we
    reconstruct it from the 'Symbol' column.
    """
    cols = {c.lower(): c for c in df.columns}

    # Prefer root_symbol if colab_prepare_10_universe left it in the CSV
    root_col = None
    for key in ("root_symbol", "symbol_root", "root"):
        if key in cols:
            root_col = cols[key]
            break

    if root_col is None:
        # Fallback: derive from "Symbol" like 'SBIN_5MINUTE' -> 'SBIN'
        if "symbol" not in cols:
            raise RuntimeError("Colab CSV missing 'Symbol' column for symbol mapping")
        sym_col = cols["symbol"]

        def to_root(s: str) -> str:
            s = str(s)
            # Typical pattern: TATAMOTORS_5MINUTE, ABB_5MINUTE, etc.
            return s.split("_")[0]

        df["symbol"] = df[sym_col].apply(to_root)
    else:
        df["symbol"] = df[root_col].astype(str)

    # Make sure symbol labels are clean, uppercased
    df["symbol"] = df["symbol"].str.strip().str.upper()
    return df


def load_colab_symbol_daily() -> pd.DataFrame:
    """
    Load Colab 10-stock universe file and aggregate to (day, symbol) PNL_R2
    for active trades only (Skip != 'ABSTAIN').

    Returns columns: [day, symbol, colab_pnl_r2]
    """
    colab = pd.read_csv(COLAB_PATH)
    colab.columns = [str(c).strip() for c in colab.columns]

    # Normalise symbol
    colab = _normalise_colab_symbols(colab)

    # Normalise date
    if "Date" not in colab.columns:
        raise RuntimeError("Colab CSV missing 'Date' column")
    colab["day"] = pd.to_datetime(colab["Date"]).dt.date

    # Filter to our 10-stock universe
    colab = colab[colab["symbol"].isin(TEN_UNIVERSE)].copy()

    # Active trades only: Skip != 'ABSTAIN'
    if "Skip" not in colab.columns:
        raise RuntimeError("Colab CSV missing 'Skip' column")
    colab["Skip"] = colab["Skip"].astype(str).str.upper().str.strip()
    colab_active = colab[colab["Skip"] != "ABSTAIN"].copy()

    # PNL_R2 must exist
    if "PNL_R2" not in colab_active.columns:
        raise RuntimeError("Colab CSV missing 'PNL_R2' column")

    colab_active["PNL_R2"] = pd.to_numeric(colab_active["PNL_R2"], errors="coerce").fillna(0.0)

    # Aggregate to one row per (day, symbol)
    colab_symbol_daily = (
        colab_active.groupby(["day", "symbol"], as_index=False)
        .agg(colab_pnl_r2=("PNL_R2", "sum"))
    )

    return colab_symbol_daily


def load_system_symbol_daily() -> pd.DataFrame:
    """
    Load system fills and aggregate to (day, symbol) PNL_R2.

    Returns columns: [day, symbol, system_pnl_r2]
    """
    fills = pd.read_csv(FILLS_PATH)
    fills.columns = [str(c).strip() for c in fills.columns]

    # Only paper mode parity is relevant
    if "mode" in fills.columns:
        fills = fills[fills["mode"].astype(str) == "paper"].copy()

    # Normalise day and symbol
    if "day" not in fills.columns:
        raise RuntimeError("fills.csv missing 'day' column")
    if "symbol" not in fills.columns:
        raise RuntimeError("fills.csv missing 'symbol' column")

    fills["day"] = pd.to_datetime(fills["day"]).dt.date
    fills["symbol"] = fills["symbol"].astype(str).str.strip().str.upper()

    # Restrict to our 10-stock universe just in case
    fills = fills[fills["symbol"].isin(TEN_UNIVERSE)].copy()

    if "pnl_r2" not in fills.columns:
        raise RuntimeError("fills.csv missing 'pnl_r2' column")

    fills["pnl_r2"] = pd.to_numeric(fills["pnl_r2"], errors="coerce").fillna(0.0)

    system_symbol_daily = (
        fills.groupby(["day", "symbol"], as_index=False)
        .agg(system_pnl_r2=("pnl_r2", "sum"))
    )
    return system_symbol_daily


def main() -> None:
    colab_symbol_daily = load_colab_symbol_daily()
    system_symbol_daily = load_system_symbol_daily()

    # For each (day, symbol) that the SYSTEM actually traded, pull the Colab PNL_R2.
    merged = pd.merge(
        system_symbol_daily,
        colab_symbol_daily,
        on=["day", "symbol"],
        how="left",
        indicator=True,
    )

    # If any system trades have no matching Colab row, flag them
    missing_mask = merged["_merge"] != "both"
    if missing_mask.any():
        missing_rows = merged.loc[missing_mask, ["day", "symbol"]].drop_duplicates()
        print("[compare_parity] ⚠ Warning: system trades with no Colab row:")
        print(missing_rows.to_string(index=False))
    merged.drop(columns=["_merge"], inplace=True)

    # Aggregate per day over the intersection
    daily = (
        merged.groupby("day", as_index=False)
        .agg(
            system_trades=("symbol", "count"),
            portfolio_pnl_r2=("system_pnl_r2", "sum"),
            colab_sum_pnl_r2=("colab_pnl_r2", "sum"),
        )
    )

    # Also compute how many active Colab trades existed per day (across the 10 stocks)
    colab_day_counts = (
        colab_symbol_daily.groupby("day", as_index=False)
        .agg(colab_active_trades=("symbol", "count"))
    )

    daily = pd.merge(daily, colab_day_counts, on="day", how="left")

    # Expected portfolio R2 if system used Colab PNL but scaled down by risk_per_trade
    # Each system trade uses risk_per_trade = DAILY_RISK_RS / system_trades
    # Colab PNL is sized at 10k/stock, so we scale by 1/system_trades:
    #   expected_portfolio_pnl_r2 = colab_sum_pnl_r2 / system_trades
    #   expected_R2               = expected_portfolio_pnl_r2 / DAILY_RISK_RS
    daily["colab_portfolio_R2_expected"] = (
        daily["colab_sum_pnl_r2"] / (daily["system_trades"] * DAILY_RISK_RS)
    )

    daily["portfolio_R2"] = daily["portfolio_pnl_r2"] / DAILY_RISK_RS
    daily["R2_diff"] = daily["portfolio_R2"] - daily["colab_portfolio_R2_expected"]

    daily = daily[
        [
            "day",
            "colab_active_trades",
            "system_trades",
            "colab_sum_pnl_r2",
            "colab_portfolio_R2_expected",
            "portfolio_pnl_r2",
            "portfolio_R2",
            "R2_diff",
        ]
    ].sort_values("day")

    print("=== Colab vs System Daily Parity (10-stock universe, INTERSECTION ONLY) ===")
    if daily.empty:
        print("(no days with system trades found)")
        return

    print(daily.to_string(index=False))

    max_abs = daily["R2_diff"].abs().max()
    print(f"\n[compare_parity] Max |R2_diff| across days = {max_abs:.6f}")
    if max_abs < 1e-3:
        print("[compare_parity] ✅ R2 parity within tolerance for all days.")
    else:
        print("[compare_parity] ⚠ R2 differences above tolerance – inspect rows above.")


if __name__ == "__main__":
    main()
