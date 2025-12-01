import os
import json
from pathlib import Path
import argparse

import pandas as pd
from pandas.api.types import is_datetime64tz_dtype
from kiteconnect import KiteConnect
from dotenv import load_dotenv

from probedge.infra.settings import SETTINGS

# ------------------------------------------------------
# 1) ENV + Kite init
# ------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]  # .../probedge/probedge
dotenv_path = ROOT / ".env"
load_dotenv(dotenv_path)

api_key = os.getenv("KITE_API_KEY")
acc_tok = os.getenv("KITE_ACCESS_TOKEN")

if not api_key or not acc_tok:
    raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN missing in .env")

kite = KiteConnect(api_key=api_key)
kite.set_access_token(acc_tok)

# ------------------------------------------------------
# 2) Paths
# ------------------------------------------------------
INTRA_DIR = Path(getattr(SETTINGS.paths, "intraday", "data/intraday"))
INTRA_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------
# 3) symbol_map + overrides (TMPV)
# ------------------------------------------------------
mp: dict = {}
sym_map_path = ROOT / "config" / "symbol_map.json"
if sym_map_path.exists():
    mp = json.loads(sym_map_path.read_text())

print("Downloading NSE instruments…")
instruments = kite.instruments("NSE")
by_ts = {row["tradingsymbol"].upper(): row for row in instruments}

# Logical-symbol → hard overrides (TATAMOTORS → TMPV)
SYMBOL_TS_OVERRIDE = {
    "TATAMOTORS": "TMPV",
}


def resolve_tradingsymbol(sym: str) -> str:
    """
    Resolve our logical symbol (e.g. TATAMOTORS) to a Kite tradingsymbol,
    using symbol_map.json, then hard overrides.
    """
    logical = sym.upper()
    ts = logical

    # 1) symbol_map.json if present
    if logical in mp:
        v = mp[logical]
        if isinstance(v, dict):
            ts = v.get("tradingsymbol", logical).upper()
        elif isinstance(v, str):
            ts = v.upper()

    # 2) hard override (TMPV)
    ts = SYMBOL_TS_OVERRIDE.get(logical, ts)
    return ts


def instrument_token_for(sym: str) -> int:
    ts = resolve_tradingsymbol(sym)
    row = by_ts.get(ts)
    if not row:
        raise ValueError(f"Tradingsymbol not found on NSE: {ts} (for {sym})")
    return int(row["instrument_token"])


def path_for(sym: str) -> Path:
    return INTRA_DIR / f"{sym}_5minute.csv"


# ------------------------------------------------------
# 4) Existing CSV normalisation
# ------------------------------------------------------
def unify_existing(path: Path) -> pd.DataFrame:
    """
    Read existing intraday CSV in either:
      - legacy format: date,open,high,low,close,volume
      - new format: DateTime,Open,High,Low,Close,Volume,(Date?)
    and return DataFrame with columns:
      DateTime (naive, Asia/Kolkata local clock)
      Open, High, Low, Close, Volume
      Date (normalized date)
    """
    if not path.exists():
        return pd.DataFrame()

    cur = pd.read_csv(path)
    if cur.empty:
        return cur

    # Legacy schema: "date,open,high,low,close,volume"
    if "date" in cur.columns and "open" in cur.columns:
        dt = pd.to_datetime(cur["date"], errors="coerce")
        cur["DateTime"] = dt
        cur["Open"] = cur["open"]
        cur["High"] = cur["high"]
        cur["Low"] = cur["low"]
        cur["Close"] = cur["close"]
        cur["Volume"] = cur["volume"]

    # Newer schema: "DateTime,Open,High,Low,Close,Volume"
    elif "DateTime" in cur.columns:
        dt = pd.to_datetime(cur["DateTime"], errors="coerce")
        # If tz-aware (e.g. strings with +05:30) → convert to naive Asia/Kolkata
        if is_datetime64tz_dtype(dt.dtype):
            dt = dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        cur["DateTime"] = dt

        if "Open" not in cur.columns or "High" not in cur.columns:
            raise ValueError(f"{path} missing OHLC columns.")
    else:
        raise ValueError(f"{path} has neither 'date' nor 'DateTime' columns")

    cur["Date"] = cur["DateTime"].dt.normalize()

    cols = ["DateTime", "Open", "High", "Low", "Close", "Volume", "Date"]
    cur = cur[cols].dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
    return cur


# ------------------------------------------------------
# 5) Kite fetch for one day
# ------------------------------------------------------
def fetch_day(token: int, day: pd.Timestamp) -> pd.DataFrame:
    """
    Pull 1-minute data for one day and convert to our 5-min compatible schema.
    We keep 1-min bars here; 5-min aggregation is handled elsewhere in pipeline
    via read_tm5_csv → 5-minute resample.
    """
    fr = (
        pd.Timestamp(day)
        .tz_localize("Asia/Kolkata")
        .replace(hour=9, minute=0, second=0, microsecond=0)
    )
    to = (
        pd.Timestamp(day)
        .tz_localize("Asia/Kolkata")
        .replace(hour=15, minute=30, second=0, microsecond=0)
    )

    data = kite.historical_data(
        token,
        fr.to_pydatetime(),
        to.to_pydatetime(),
        interval="minute",
        continuous=False,
        oi=False,
    )
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    # Kite returns tz-aware; convert to naive Asia/Kolkata local time
    dt = pd.to_datetime(df["date"]).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df["DateTime"] = dt
    df = df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    df["Date"] = df["DateTime"].dt.normalize()

    cols = ["DateTime", "Open", "High", "Low", "Close", "Volume", "Date"]
    return df[cols].sort_values("DateTime")


# ------------------------------------------------------
# 6) Calendar: last N business days
# ------------------------------------------------------
def last_n_bdays(n: int):
    today = pd.Timestamp.today(tz="Asia/Kolkata").normalize().tz_localize(None)
    start = today - pd.Timedelta(days=int(n * 2))  # overshoot and let empty fetches drop
    return pd.date_range(start, today, freq="B").date


# ------------------------------------------------------
# 7) Main backfill
# ------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Backfill intraday via Kite")
    parser.add_argument("--days", type=int, default=120, help="Number of recent business days")
    args = parser.parse_args()

    days = [pd.Timestamp(d) for d in last_n_bdays(args.days)]

    for sym in SETTINGS.symbols:
        try:
            token = instrument_token_for(sym)
        except Exception as e:
            print(f"[{sym}] SKIP – {e}")
            continue

        path = path_for(sym)
        cur = unify_existing(path)
        have = set(cur["Date"].unique()) if not cur.empty else set()
        adds = []

        for d in days:
            d_norm = d.normalize()
            if d_norm in have:
                continue
            try:
                df_d = fetch_day(token, d)
                if not df_d.empty:
                    adds.append(df_d)
            except Exception as e:
                print(f"[{sym}] {d.date()} fetch ERR {e}")

        if adds:
            new = (
                pd.concat([cur] + adds, ignore_index=True)
                if not cur.empty
                else pd.concat(adds, ignore_index=True)
            )
        else:
            new = cur

        if new.empty:
            print(f"[{sym}] no data written (still empty)")
            continue

        new = (
            new.dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
            .sort_values("DateTime")
            .drop_duplicates("DateTime", keep="last")
        )

        out = new.copy()
        # Keep existing convention: ISO string with +05:30
        out["DateTime"] = out["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
        out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
        out.to_csv(path, index=False)

        added_rows = sum(len(a) for a in adds) if adds else 0
        print(f"[{sym}] intraday rows={len(new)} (added {added_rows}) → {path}")

    print("Done backfill.")


if __name__ == "__main__":
    main()
