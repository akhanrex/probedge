import os, json, math
from pathlib import Path
import pandas as pd
from kiteconnect import KiteConnect
from probedge.infra.settings import SETTINGS
from dotenv import load_dotenv


# --- Load .env from repo root so KITE_* are available ---
ROOT = Path(__file__).resolve().parents[1]  # /Users/.../probedge/probedge
dotenv_path = ROOT / ".env"
load_dotenv(dotenv_path)

# Now these will work:
# os.environ["KITE_API_KEY"]
# os.environ["KITE_ACCESS_TOKEN"]

# --- kite init ---
api_key = os.environ["KITE_API_KEY"]
acc_tok = os.environ["KITE_ACCESS_TOKEN"]
kite = KiteConnect(api_key=api_key)
kite.set_access_token(acc_tok)

# --- paths ---
# For backfill we hard-code to the repo's data/intraday folder,
# ignoring any templated {sym} settings.
INTRA_DIR = ROOT / "data" / "intraday"
INTRA_DIR.mkdir(parents=True, exist_ok=True)


# --- symbol map ---
mp = {}
p = Path("config/symbol_map.json")
if p.exists():
    mp = json.loads(p.read_text())

# --- build instrument map once ---
print("Downloading NSE instruments…")
instruments = kite.instruments("NSE")
by_ts = {row["tradingsymbol"].upper(): row for row in instruments}


# Logical-symbol → Kite tradingsymbol overrides
SYMBOL_TS_OVERRIDE = {
    # Tata Motors ordinary share: now trades as TMPV on NSE
    "TATAMOTORS": "TMPV",
}

def resolve_tradingsymbol(sym: str) -> str:
    """
    Resolve our logical symbol (e.g. TATAMOTORS) to a Kite tradingsymbol
    using symbol_map.json first, then hard overrides (e.g. TMPV).
    """
    logical = sym.upper()
    ts = logical

    # 1) Try symbol_map.json if present
    if logical in mp:
        v = mp[logical]
        if isinstance(v, dict):
            ts = v.get("tradingsymbol", logical).upper()
        elif isinstance(v, str):
            ts = v.upper()

    # 2) Apply hard overrides (e.g. TATAMOTORS → TMPV)
    ts = SYMBOL_TS_OVERRIDE.get(logical, ts)

    return ts

def ts_for(sym: str) -> int:
    """
    Return Kite instrument_token for our logical symbol.
    Uses the by_ts dict built from kite.instruments("NSE").
    """
    ts = resolve_tradingsymbol(sym)
    row = by_ts.get(ts)
    if not row:
        raise ValueError(f"Tradingsymbol not found on NSE: {ts} (for {sym})")
    return int(row["instrument_token"])



def path_for(sym: str) -> Path:
    return INTRA_DIR / f"{sym}_5minute.csv"

def fetch_day(token: int, day: pd.Timestamp) -> pd.DataFrame:
    fr = pd.Timestamp(day).tz_localize("Asia/Kolkata").replace(hour=9,  minute=0,  second=0, microsecond=0)
    to = pd.Timestamp(day).tz_localize("Asia/Kolkata").replace(hour=15, minute=30, second=0, microsecond=0)
    data = kite.historical_data(token, fr.to_pydatetime(), to.to_pydatetime(), interval="minute", continuous=False, oi=False)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # Kite returns 'date' tz-aware; normalize to naive local for our pipeline
    df["DateTime"] = pd.to_datetime(df["date"]).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df = df.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})
    df["Date"] = df["DateTime"].dt.normalize()
    return df[["DateTime","Open","High","Low","Close","Volume","Date"]].sort_values("DateTime")

# --- trading days set (last 120 present on exchange calendar via instruments dump) ---
# Conservative: derive from any existing symbol that has most data, else fallback to last 180 calendar days.
def last_n_days(n=120):
    # calendar from pandas bdate_range ~ Indian holidays ignored -> we overshoot and rely on empty fetches being skipped
    end = pd.Timestamp.today(tz="Asia/Kolkata").normalize().tz_localize(None)
    # start a bit early to be safe
    start = end - pd.Timedelta(days=int(n*2))
    return pd.date_range(start, end, freq="B").date

days = list(last_n_days(120))

for sym in SETTINGS.symbols:
    # ts_for already returns the Kite instrument_token (int)
    token = ts_for(sym)
    path = path_for(sym)
    cur = pd.DataFrame()
    if path.exists():
        cur = pd.read_csv(path)
        if not cur.empty:
            # --- unify schema for existing file ---
            # 1) Build DateTime (tz-naive, Asia/Kolkata) from either:
            #    - existing "DateTime" column (string with or without tz)
            #    - OR old "date" column (string)
            if "DateTime" in cur.columns:
                cur_dt = pd.to_datetime(cur["DateTime"], utc=True, errors="coerce")
            elif "date" in cur.columns:
                cur_dt = pd.to_datetime(cur["date"], utc=True, errors="coerce")
            else:
                raise ValueError(f"{path} has neither DateTime nor date column")

            # Convert to Asia/Kolkata local and then drop tz → naive
            cur_dt = cur_dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            cur["DateTime"] = cur_dt

            # 2) Build Date column as normalized local date
            if "Date" in cur.columns:
                cur_date = pd.to_datetime(cur["Date"], utc=True, errors="coerce")
                cur_date = cur_date.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None).dt.normalize()
                cur["Date"] = cur_date
            else:
                cur["Date"] = cur["DateTime"].dt.normalize()


            # 2) Normalize OHLCV to capital names
            rename_map = {}
            for key, std in [
                ("open", "Open"),
                ("high", "High"),
                ("low", "Low"),
                ("close", "Close"),
                ("volume", "Volume"),
            ]:
                if key in cols:
                    rename_map[cols[key]] = std
            if rename_map:
                cur = cur.rename(columns=rename_map)

            # 3) Build Date column
            if "Date" in cur.columns:
                cur["Date"] = pd.to_datetime(cur["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()
            else:
                cur["Date"] = cur["DateTime"].dt.tz_localize(None).dt.normalize()
    have = set(cur["Date"].dropna().unique()) if not cur.empty else set()
    adds = []
    for d in days:
        d = pd.Timestamp(d)
        if d in have:  # already have day
            continue
        try:
            df = fetch_day(token, d)
            if not df.empty:
                adds.append(df)
        except Exception as e:
            print(f"[{sym}] {d.date()} fetch ERR {e}")

    if adds:
        new = pd.concat([cur] + adds, ignore_index=True) if not cur.empty else pd.concat(adds, ignore_index=True)
        new = (
            new
            .dropna(subset=["DateTime","Open","High","Low","Close"])
            .sort_values("DateTime")
            .drop_duplicates("DateTime", keep="last")
        )
    else:
        new = cur

    if new.empty:
        print(f"[{sym}] no data written (still empty)")
        continue

    # write as ISO w/ +05:30 string (your existing convention)
    out = new.copy()
    out["DateTime"] = out["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)
    print(f"[{sym}] intraday rows={len(new)} (added {sum(len(x) for x in adds) if adds else 0}) → {path}")

print("Done backfill.")

