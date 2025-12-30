from __future__ import annotations
import json
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect

from probedge.infra.settings import SETTINGS

# Logical universe (what Probedge uses everywhere)
UNIVERSE = SETTINGS.symbols

# Mapping from our logical name -> Kite trading symbol
# (TATAMOTORS is called TMPV at the exchange)
ALIAS_TO_KITE = {
    "TATAMOTORS": "TMPV",
}

TOKENS_PATH = Path("data/tokens_5min.csv")


def make_kite() -> KiteConnect:
    api_key = SETTINGS.kite_api_key
    if not api_key:
        raise RuntimeError("KITE_API_KEY not configured in .env")

    sess_file = SETTINGS.kite_session_file
    if not sess_file or not sess_file.exists():
        raise RuntimeError(f"Kite session file not found: {sess_file}")

    with sess_file.open("r", encoding="utf-8") as f:
        sess = json.load(f)

    access_token = sess.get("access_token")
    if not access_token:
        raise RuntimeError("Kite session file has no access_token")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def main():
    kite = make_kite()

    print("[tokens] Fetching NSE instruments from Kite…")
    instruments = kite.instruments("NSE")

    rows = []
    for sym in UNIVERSE:
        kite_sym = ALIAS_TO_KITE.get(sym, sym)
        matches = [
            inst for inst in instruments
            if inst.get("tradingsymbol") == kite_sym
        ]
        if not matches:
            print(f"[tokens] WARNING: no instrument found for {sym} (kite_sym={kite_sym})")
            continue

        inst = matches[0]
        rows.append({
            "symbol": sym,                         # our logical name
            "kite_symbol": kite_sym,               # what Kite calls it
            "exchange": inst.get("exchange"),
            "instrument_token": inst.get("instrument_token"),
        })
        print(f"[tokens] {sym} -> {kite_sym} -> token {inst.get('instrument_token')}")

    if not rows:
        raise RuntimeError("No tokens resolved; cannot write tokens_5min.csv")

    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(TOKENS_PATH, index=False)
    print(f"[tokens] Wrote {TOKENS_PATH} with {len(rows)} rows")


if __name__ == "__main__":
    main()

