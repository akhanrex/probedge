import os, json, time
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from kiteconnect import KiteConnect

ROOT = Path(__file__).resolve().parents[1]
INTRA_DIR = ROOT / "data" / "intraday"
CFG_SYM = ROOT / "config" / "symbol_map.json"
TOK_CACHE = ROOT / "data" / "diagnostics" / "kite_tokens.csv"
INTRA_DIR.mkdir(parents=True, exist_ok=True)
TOK_CACHE.parent.mkdir(parents=True, exist_ok=True)

api_key  = os.environ["KITE_API_KEY"]
acc_token= os.environ["KITE_ACCESS_TOKEN"]
kite = KiteConnect(api_key=api_key)
kite.set_access_token(acc_token)

sym_map = json.loads(CFG_SYM.read_text())
# Resolve instrument_token from tradingsymbol (NSE)
def resolve_tokens():
    if TOK_CACHE.exists():
        return pd.read_csv(TOK_CACHE)
    rows=[]
    for ins in kite.instruments(exchange="NSE"):
        rows.append(ins)
    df = pd.DataFrame(rows)
    df.to_csv(TOK_CACHE, index=False)
    return df

ins_df = resolve_tokens()

def token_for(tradingsymbol: str):
    r = ins_df[ins_df["tradingsymbol"]==tradingsymbol]
    if r.empty:
        raise RuntimeError(f"tradingsymbol not found on NSE: {tradingsymbol}")
    return int(r.iloc[0]["instrument_token"])

def dtrange_days(n_days:int):
    # last n calendar days including today (we'll fetch per-day windows)
    today = datetime.now()
    days = [(today - timedelta(days=i)).date() for i in range(n_days)]
    return sorted(set(days))

def read_existing(sym: str) -> pd.DataFrame:
    p = INTRA_DIR / f"{sym}_5minute.csv"
    if not p.exists():
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume","Date"])
    df = pd.read_csv(p)
    # normalize
    dt = pd.to_datetime(df.get("DateTime", pd.Series(dtype=str)), errors="coerce")
    # keep raw strings; we’ll reformat on write — but compute Date now
    d  = pd.to_datetime(df.get("Date", pd.Series(dtype=str)), errors="coerce")
    if d.isna().all() and not dt.isna().all():
        d = dt.dt.tz_localize(None).dt.normalize()
    df["DateTime"] = dt
    df["Date"] = d
    return df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime")

def fetch_one_day(inst_token: int, day) -> pd.DataFrame:
    # 09:00–15:30 IST to be safe window; interval=5minute
    start = datetime(day.year, day.month, day.day, 9, 0)
    end   = datetime(day.year, day.month, day.day, 15, 30)
    candles = kite.historical_data(inst_token, start, end, interval="5minute", continuous=False, oi=False)
    if not candles:
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume","Date"])
    df = pd.DataFrame(candles)
    df = df.rename(columns={"date":"DateTime","open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})
    df["DateTime"] = pd.to_datetime(df["DateTime"], utc=True).dt.tz_convert("Asia/Kolkata")
    df["Date"] = df["DateTime"].dt.tz_localize(None).dt.normalize()
    # drop timezone for storage, but keep +05:30 text on write
    df["DateTime"] = df["DateTime"].dt.tz_localize(None)
    return df[["DateTime","Open","High","Low","Close","Volume","Date"]]

LOOKBACK_DAYS = 15  # adjust as needed
for sym, tsym in sym_map.items():
    cur = read_existing(sym)
    have_days = set(pd.to_datetime(cur["Date"]).dt.normalize()) if not cur.empty else set()
    target_days = [pd.Timestamp(d) for d in dtrange_days(LOOKBACK_DAYS)]
    need = [d for d in target_days if d.normalize() not in have_days]
    if not need:
        print(f"[{sym}] up-to-date, rows={len(cur)}"); 
        # still rewrite to unify format
        if not cur.empty:
            out = cur.copy()
            out["DateTime"] = out["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
            out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
            out.to_csv(INTRA_DIR / f"{sym}_5minute.csv", index=False)
        continue

    itok = token_for(tsym)
    adds=[]
    for d in need:
        try:
            df = fetch_one_day(itok, d.date())
            if not df.empty:
                adds.append(df)
            time.sleep(0.25)
        except Exception as e:
            print(f"[{sym}] {d.date()} fetch ERR {e}")
            time.sleep(0.5)

    new = (pd.concat([cur] + adds, ignore_index=True) if not cur.empty else pd.concat(adds, ignore_index=True)) if adds else cur
    if new.empty:
        print(f"[{sym}] no data written (empty)"); 
        continue
    new = new.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime").drop_duplicates("DateTime", keep="last")
    out = new.copy()
    out["DateTime"] = out["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    path = INTRA_DIR / f"{sym}_5minute.csv"
    out.to_csv(path, index=False)
    added = 0 if not adds else sum(len(x) for x in adds)
    print(f"[{sym}] intraday rows={len(new)} (added {added}) → {path}")

print("Done backfill.")
