import os, json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
from kiteconnect import KiteConnect

API_KEY = os.environ["KITE_API_KEY"]
ACCESS  = os.environ["KITE_ACCESS_TOKEN"]

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS)

MAP_PATH = Path("config/symbol_map.json")
SYMBOL_MAP = json.loads(MAP_PATH.read_text())

def _intraday_path(sym: str) -> Path:
    base = Path("data/intraday"); base.mkdir(parents=True, exist_ok=True)
    return base / f"{sym}_5minute.csv"

def _nse_instruments():
    try:
        return kite.instruments("NSE")
    except TypeError:
        return [x for x in kite.instruments() if x.get("exchange")=="NSE"]

BY_TS = {x["tradingsymbol"]: x for x in _nse_instruments()}

def token_for(tradingsymbol: str) -> int:
    x = BY_TS.get(tradingsymbol)
    if not x:
        raise RuntimeError(f"tradingsymbol not found on NSE: {tradingsymbol}")
    return int(x["instrument_token"])

def _ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["DateTime","Date","Open","High","Low","Close","Volume"])

    # Accept various incoming column namings
    if "DateTime" in df.columns:
        dt = pd.to_datetime(df["DateTime"], errors="coerce", utc=True)
    elif "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce", utc=True)
    else:
        raise ValueError("No DateTime/date column in dataframe")

    # Convert to IST
    dt = dt.dt.tz_convert("Asia/Kolkata")
    df["DateTime"] = dt

    # Build/clean Date
    if "Date" in df.columns:
        d = pd.to_datetime(df["Date"], errors="coerce")
    else:
        d = pd.NaT
    df["Date"] = pd.to_datetime(d, errors="coerce")
    df.loc[df["Date"].isna(), "Date"] = df["DateTime"].dt.tz_localize(None).dt.normalize()

    # Coerce OHLCV
    for up, lo in (("Open","open"),("High","high"),("Low","low"),("Close","close"),("Volume","volume")):
        if up in df.columns:
            df[up] = pd.to_numeric(df[up], errors="coerce")
        elif lo in df.columns:
            df[up] = pd.to_numeric(df[lo], errors="coerce")
        else:
            df[up] = pd.NA

    out = (df[["DateTime","Date","Open","High","Low","Close","Volume"]]
             .dropna(subset=["DateTime","Open","High","Low","Close"])
             .sort_values("DateTime")
             .drop_duplicates("DateTime", keep="last")
             .reset_index(drop=True))
    return out

def fetch_5min_df(inst_token: int, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    rows = []
    cur = start_dt
    while cur < end_dt:
        chunk_end = min(cur + timedelta(days=60), end_dt)
        part = kite.historical_data(
            instrument_token=inst_token,
            from_date=cur, to_date=chunk_end,
            interval="5minute", continuous=False, oi=False
        )
        rows.extend(part)
        cur = chunk_end
    df = pd.DataFrame(rows)
    if df.empty:
        return _ensure_schema(df)
    if "oi" in df.columns:
        df.drop(columns=["oi"], inplace=True, errors="ignore")
    return _ensure_schema(df)

def merge_and_write(sym: str, fetched: pd.DataFrame) -> Path:
    path = _intraday_path(sym)
    new_df = _ensure_schema(fetched)

    if path.exists():
        cur = pd.read_csv(path)
        cur = _ensure_schema(cur)
        both = pd.concat([cur, new_df], ignore_index=True)
    else:
        both = new_df

    both = (both.dropna(subset=["DateTime","Open","High","Low","Close"])
                 .sort_values("DateTime")
                 .drop_duplicates("DateTime", keep="last")
                 .reset_index(drop=True))

    out = both.copy()
    # Write DateTime as ISO with colon in offset
    out["DateTime"] = out["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(r"(\+)(\d{2})(\d{2})$", r"\1\2:\3", regex=True)
    out["Date"]     = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)
    return path

def main():
    DAYS = int(os.environ.get("BACKFILL_DAYS", "120"))
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=DAYS)

    for sym, tsym in SYMBOL_MAP.items():
        try:
            itok = token_for(tsym)
            df   = fetch_5min_df(itok, start_dt, end_dt)
            path = merge_and_write(sym, df)
            print(f"[{sym}] wrote {len(df)} rows → {path}")
        except Exception as e:
            print(f"[{sym}] ERROR: {e}")

if __name__ == "__main__":
    main()
