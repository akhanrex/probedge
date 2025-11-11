#!/usr/bin/env python3
"""
Symbol -> instrument_token map for Kite.

Fill these with your live instrument tokens (NOT trading symbols).
Quick way:
  - kite.instruments("NSE") and search rows where tradingsymbol == our symbol.
  - Or use Zerodha instruments master CSV.

NOTE: Keep only the ones you trade to avoid confusion.
"""
TOKENS = {
    # ---- FILL THESE with real tokens ----
    # Examples shown are placeholders and WILL NOT WORK. Replace them!
    "TATAMOTORS": 0,   # e.g., 884737
    "LT":         0,   # e.g., 2939649
    "SBIN":       0,   # e.g., 779521
}

def token_for(symbol: str) -> int:
    t = TOKENS.get(symbol.upper(), 0)
    if not t or t <= 0:
        raise RuntimeError(f"No instrument token configured for {symbol}. Edit infra/tokens.py")
    return t
