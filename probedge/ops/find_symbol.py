from kiteconnect import KiteConnect
from dotenv import dotenv_values
cfg = dotenv_values(".env")
kc = KiteConnect(api_key=cfg["KITE_API_KEY"])
if cfg.get("KITE_ACCESS_TOKEN"): kc.set_access_token(cfg["KITE_ACCESS_TOKEN"])
rows = kc.instruments()

def find(q, limit=20):
    q = q.upper()
    out = [(r["exchange"], r["tradingsymbol"], r.get("name",""), r["instrument_token"])
           for r in rows
           if q in str(r.get("tradingsymbol","")).upper() or q in str(r.get("name","")).upper()]
    return out[:limit]

for q in ["BAJAJHFL","BAJAJ","HOLDING","SWIGGY","ZOMATO","ETERNAL","TMPV"]:
    print(f"\n== {q} ==")
    for e,ts,name,tok in find(q):
        print(f"{e:3}  {ts:15}  {name[:32]:32}  {tok}")
