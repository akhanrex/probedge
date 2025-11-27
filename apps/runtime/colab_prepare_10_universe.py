from __future__ import annotations

from pathlib import Path
import pandas as pd

# Our 10-stock live universe
UNIVERSE_10 = [
    "TMPV", "SBIN", "RECLTD", "JSWENERGY", "LT",
    "COALINDIA", "ABB", "LICI", "ETERNAL", "JIOFIN",
]


def main():
    src = Path("data/backtest/colab_all_full.csv")
    dst = Path("data/backtest/colab_all_10stocks.csv")

    if not src.exists():
        raise FileNotFoundError(
            f"Expected big Colab CSV at {src}. "
            "Copy your ALL_BACKTEST file there as colab_all_full.csv."
        )

    print(f"[colab_prepare_10] Reading {src} ...")
    df = pd.read_csv(src)

    if df.empty:
        raise RuntimeError("Source colab_all_full.csv is empty")

    # Try to locate the symbol column
    symbol_col = None
    for cand in ("symbol", "Symbol", "SYMBOL"):
        if cand in df.columns:
            symbol_col = cand
            break

    if symbol_col is None:
        raise RuntimeError(
            "Could not find a symbol column in Colab CSV. "
            "Expected one of: symbol, Symbol, SYMBOL"
        )

    # Filter to our 10-stock universe
    mask = df[symbol_col].astype(str).isin(UNIVERSE_10)
    filtered = df[mask].copy()

    if filtered.empty:
        raise RuntimeError(
            "After filtering to 10-stock universe, no rows remain. "
            "Check that symbol names in Colab CSV match: "
            f"{UNIVERSE_10}"
        )

    dst.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(dst, index=False)

    print(f"[colab_prepare_10] Kept {filtered.shape[0]} rows for 10-stock universe")
    print(f"[colab_prepare_10] Wrote trimmed file to {dst}")


if __name__ == "__main__":
    main()
