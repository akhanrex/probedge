# ops/backfill_intraday_kite.py
import os, time, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date
from kiteconnect import KiteConnect
from probedge.infra.settings import SETTINGS

INTRA = Path(getattr(SETTINGS.paths, "intraday", "data/intraday"))
INTRA.mkdir(parents=True, exist_ok=True)

api_key   = os.environ["KITE_API_KEY"]
acc_token = os.environ["KITE_ACCESS_TOKEN"]
kite = KiteConnect(api_key=api_key)
kite.set_access_token(acc_token)

# Build tradingsymbol -> instrument_token map (do this once per day)
inst = pd.DataFrame(kite.instruments("NSE"))
tok = {r.tradingsymbol.upper(): int(r.instrument_token) for _,r in inst.iterrows()}

def fetch_day_1min(token: int, d: date):
    frm = datetime(d.year,d.month,d.day,9,15)
    to  = datetime(d.year,d.month,d.day,15,30)
    data = kite.historical_data(token, frm, to, "minute", continuous=False, oi=False)
    df = pd.DataFrame(data)
    if df.empty: return df
    df.rename(columns={"date":"DateTime","open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"}, inplace=True)
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df["Date"] = df["DateTime"].dt.normalize()
    return df

def to_5min(df1m: pd.DataFrame) -> pd.DataFrame:
    if df1m.empty: return df1m
    df1m = df1m.set_index("DateTime").sort_index()
    o = df1m["Open"].resample("5min").first()
    h = df1m["High"].resample("5min").max()
    l = df1m["Low"].resample("5min").min()
    c = df1m["Close"].resample("5min").last()
    v = df1m["Volume"].resample("5min").sum()
    out = pd.concat([o,h,l,c,v], axis=1).dropna().reset_index()
    out["Date"] = out["DateTime"].dt.normalize()
    return out[["DateTime","Open","High","Low","Close","Volume","Date"]]

def read_tm5(sym):
    p = INTRA / f"{sym}_5minute.csv"
    if p.exists():
        df = pd.read_csv(p, parse_dates=["DateTime"])
        df["Date"] = pd.to_datetime(df.get("Date", df["DateTime"].dt.normalize()))
        return df
    return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume","Date"])

def write_tm5(sym, df):
    df = df.sort_values("DateTime").drop_duplicates("DateTime", keep="last")
    df.to_csv(INTRA / f"{sym}_5minute.csv", index=False)

symbols = [s.upper() for s in SETTINGS.symbols]
end = datetime.today().date()
start = end - timedelta(days=730)

for s in symbols:
    token = tok.get(s)
    if not token:
        print(f"[{s}] No instrument token in NSE instruments; check symbol spelling or use BSE mapping.")
        continue
    cur = read_tm5(s)
    have = set(pd.to_datetime(cur["Date"]).dt.date.unique()) if not cur.empty else set()
    needed = [d for d in (start + timedelta(n) for n in range((end-start).days+1))
              if d.weekday()<5 and d not in have]  # weekdays only; exchanges/holidays will just return empty
    adds=[]
    for d in needed:
        try:
            df1 = fetch_day_1min(token, d)
            if df1.empty: continue
            df5 = to_5min(df1)
            adds.append(df5)
            time.sleep(0.25)
        except Exception as e:
            print(f"[{s}] {d}: {e}")
            time.sleep(0.5)
    if adds:
        new = pd.concat([cur]+adds, ignore_index=True) if not cur.empty else pd.concat(adds, ignore_index=True)
        write_tm5(s, new)
        print(f"[{s}] backfilled +{len(adds)} day(s) → rows={len(new)}")
    else:
        print(f"[{s}] up to date")
