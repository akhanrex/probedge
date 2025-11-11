#!/usr/bin/env python3
import os
from infra.broker_kite import get_kite

SYMS = ("TATAMOTORS","LT","SBIN")

def main():
    kite = get_kite()
    ins = kite.instruments("NSE")
    wanted = {s: None for s in SYMS}
    for r in ins:
        ts = r.get("tradingsymbol")
        if ts in wanted and wanted[ts] is None:
            wanted[ts] = r
    print("\n# Copy these into infra/tokens.py (TOKENS dict):\n")
    for s in SYMS:
        row = wanted.get(s)
        if not row:
            print(f"# {s}: NOT FOUND in NSE instruments")
            continue
        print(f'    "{s}": {row["instrument_token"]},   # {row["tradingsymbol"]} ({row["exchange"]})')

if __name__ == "__main__":
    main()
