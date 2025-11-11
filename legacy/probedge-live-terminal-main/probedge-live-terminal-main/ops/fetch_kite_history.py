#!/usr/bin/env python3
import os, sys, argparse, datetime as dt
from pathlib import Path
from dotenv import dotenv_values
import pandas as pd
from kiteconnect import KiteConnect

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
ROOT = Path(__file__).resolve().parents[1]
ENV = dotenv_values(str(ROOT/".env"))
DATA_DIR = ROOT/"data"/"paper"

SYMBOL_ALIAS = {
    "TATAMOTORS": "TATAMOTORS",
    "TMPV": "TATAMOTORS",
    "LT": "LT",
    "SBIN": "SBIN",
}

def _kite():
    api_key = ENV.get("KITE_API_KEY"); api_secret = ENV.get("KITE_API_SECRET")
    access_token = ENV.get("KITE_ACCESS_TOKEN")
    if not (api_key and api_secret):
        raise SystemExit("Missing KITE_API_KEY/KITE_API_SECRET in .env")
    if not access_token:
        raise SystemExit("Missing KITE_ACCESS_TOKEN in .env (run your auth helper)")
    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)
    return kc


def _nse_token_map(kc):
    """Return dict trading_symbol -> instrument_token for NSE."""
    print("[fetch] Loading NSE instruments ...")
    rows = kc.instruments(exchange="NSE")
    mp = {r["tradingsymbol"].upper(): int(r["instrument_token"]) for r in rows}
    return mp


def _daterange(d0: dt.date, d1: dt.date):
    cur = d0
    while cur <= d1:
        yield cur
        cur = cur + dt.timedelta(days=1)


def _ist(dt_obj: dt.datetime) -> dt.datetime:
    if dt_obj.tzinfo is None:
        return dt_obj.replace(tzinfo=IST)
    return dt_obj.astimezone(IST)


def fetch_range(symbols, start_date, end_date):
    kc = _kite()
    tkns = _nse_token_map(kc)

    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

    for sym in symbols:
        key = SYMBOL_ALIAS.get(sym.upper(), sym.upper())
        if key not in tkns:
            print(f"[fetch][WARN] {sym}: trading symbol not found on NSE; skipping")
            continue
        token = tkns[key]
        print(f"[fetch] {sym}: token={token}")

        for day in _daterange(start_date, end_date):
            # Skip weekends automatically for speed (NSE closed)
            if day.weekday() >= 5:
                continue
            dstr = day.strftime("%Y-%m-%d")
            outdir = DATA_DIR/dstr
            outdir.mkdir(parents=True, exist_ok=True)
            outcsv = outdir/f"{key}.csv"
            if outcsv.exists():
                print(f"[fetch] {sym} {dstr}: exists -> skip")
                continue

            # 09:15 to 15:30 IST window
            from_dt = dt.datetime(day.year, day.month, day.day, 9, 15, tzinfo=IST)
            to_dt   = dt.datetime(day.year, day.month, day.day, 15, 30, tzinfo=IST)

            try:
                candles = kc.historical_data(
                    instrument_token=token,
                    from_date=from_dt,
                    to_date=to_dt,
                    interval="5minute",
                    continuous=False,
                    oi=False,
                )
            except Exception as e:
                print(f"[fetch][ERR] {sym} {dstr}: {e}")
                continue

            if not candles:
                print(f"[fetch] {sym} {dstr}: no candles")
                continue

            # Normalize -> DataFrame with DateTime (ISO IST) + end_ts (epoch seconds)
            recs = []
            for c in candles:
                # Kite returns start time of the 5-min bar
                start_dt = _ist(c["date"])  # ensure IST tz
                end_dt = start_dt + dt.timedelta(minutes=5)
                recs.append({
                    "symbol": sym,
                    "DateTime": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "start_ts": int(start_dt.timestamp()),
                    "end_ts": int(end_dt.timestamp()),
                    "Open": float(c["open"]),
                    "High": float(c["high"]),
                    "Low": float(c["low"]),
                    "Close": float(c["close"]),
                    "Volume": int(c.get("volume", 0) or 0),
                })

            df = pd.DataFrame.from_records(recs)
            # Keep only market bars
            if df.empty:
                print(f"[fetch] {sym} {dstr}: empty after normalization")
                continue
            df.to_csv(outcsv, index=False)
            print(f"[fetch] wrote {outcsv}  ({len(df)} bars)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", required=True, help="Comma separated list e.g. TATAMOTORS,LT,SBIN")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    sdt = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
    edt = dt.datetime.strptime(args.end, "%Y-%m-%d").date()

    fetch_range(syms, sdt, edt)

if __name__ == "__main__":
    main()
