from __future__ import annotations

from pathlib import Path
import pandas as pd

# Our live 10-stock "logical" universe
LOGICAL_UNIVERSE = [
    "TMPV", "SBIN", "RECLTD", "JSWENERGY", "LT",
    "COALINDIA", "ABB", "LICI", "ETERNAL", "JIOFIN",
]

# Allow extra roots for matching, especially TMPV <-> TATAMOTORS
MATCH_ROOTS = {
    "TMPV": ["TMPV", "TATAMOTORS"],
    "SBIN": ["SBIN"],
    "RECLTD": ["RECLTD"],
    "JSWENERGY": ["JSWENERGY"],
    "LT": ["LT"],
    "COALINDIA": ["COALINDIA"],
    "ABB": ["ABB"],
    "LICI": ["LICI"],
    "ETERNAL": ["ETERNAL"],
    "JIOFIN": ["JIOFIN"],
}
ALL_ALLOWED_ROOTS = sorted({r for roots in MATCH_ROOTS.values() for r in roots})


def extract_root(sym: str) -> str:
    """
    Turn things like 'SBIN-EQ', 'SBIN.NS ', '  sbin ' into 'SBIN'.
    For TATAMOTORS we keep 'TATAMOTORS' as root (and map it to TMPV later).
    """
    if sym is None:
        return ""
    s = str(sym).strip().upper()

    # Split on common separators and take first token
    for sep in [" ", "-", ".", "_", "/"]:
        if sep in s:
            s = s.split(sep, 1)[0]
            break
    return s


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

    print("[colab_prepare_10] Columns:", list(df.columns))

    # --- 1) Find symbol-like column ---
    symbol_col = None
    for cand in ("symbol", "Symbol", "SYMBOL"):
        if cand in df.columns:
            symbol_col = cand
            break
    if symbol_col is None:
        # Fallback: look for any column whose name hints at symbol/ticker
        candidates = [
            c for c in df.columns
            if "sym" in c.lower() or "ticker" in c.lower() or "scrip" in c.lower()
        ]
        if len(candidates) == 1:
            symbol_col = candidates[0]

    if symbol_col is None:
        raise RuntimeError(
            "Could not find a symbol column in Colab CSV. "
            "Expected something like 'symbol', 'SYMBOL', or a ticker/scrip column."
        )

    print(f"[colab_prepare_10] Using symbol column: {symbol_col}")

    # --- 2) Build root symbols ---
    sym_raw = df[symbol_col].astype(str)
    df["_root_symbol"] = sym_raw.map(extract_root)

    print("[colab_prepare_10] Sample of raw symbol -> root_symbol:")
    sample = (
        df[[symbol_col, "_root_symbol"]]
        .drop_duplicates()
        .head(20)
        .to_string(index=False)
    )
    print(sample)

    # --- 3) Filter to allowed roots ---
    mask = df["_root_symbol"].isin(ALL_ALLOWED_ROOTS)
    filtered = df[mask].copy()

    print("[colab_prepare_10] Counts by root_symbol (before filter):")
    print(df["_root_symbol"].value_counts().head(30))

    print("[colab_prepare_10] Counts by root_symbol (after filter):")
    print(filtered["_root_symbol"].value_counts())

    if filtered.empty:
        raise RuntimeError(
            "After filtering by allowed roots, no rows remain.\n"
            f"Allowed roots: {ALL_ALLOWED_ROOTS}\n"
            "Check the 'Sample of raw symbol -> root_symbol' above and "
            "confirm how your 10 stocks are named in the Colab CSV."
        )

    # --- 4) Optional: map TATAMOTORS root => TMPV logical symbol ---
    def map_root_to_logical(root: str) -> str:
        r = root.upper()
        for logical, roots in MATCH_ROOTS.items():
            if r in roots:
                return logical
        return r  # fallback

    filtered["_logical_symbol"] = filtered["_root_symbol"].map(map_root_to_logical)

    dst.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(dst, index=False)

    print(
        f"[colab_prepare_10] Kept {filtered.shape[0]} rows "
        f"for logical 10-stock universe: {LOGICAL_UNIVERSE}"
    )
    print(f"[colab_prepare_10] Wrote trimmed file to {dst}")


if __name__ == "__main__":
    main()
