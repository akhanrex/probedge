import os, json, time, argparse
from datetime import datetime, date, timedelta, time as dtime
import pandas as pd

from kiteconnect import KiteConnect

IST = "Asia/Kolkata"

def load_kite_from_session(session_path: str) -> KiteConnect:
    with open(session_path, "r") as f:
        j = json.load(f)

    api_key = j.get("api_key") or os.getenv("KITE_API_KEY") or os.getenv("KITE_APIKEY")
    access_token = j.get("access_token") or os.getenv("KITE_ACCESS_TOKEN")

    if not api_key or not access_token:
        raise RuntimeError(
            f"Missing api_key/access_token. session_path={session_path}. "
            "Expected keys: api_key + access_token (or env vars KITE_API_KEY/KITE_ACCESS_TOKEN)."
        )

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def get_last_dt_from_csv(csv_path: str) -> datetime | None:
    if not os.path.exists(csv_path):
        return None
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" in df.columns and "time" in df.columns:
        dt = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
    elif "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], errors="coerce")
    else:
        raise RuntimeError(f"CSV schema not recognized: {csv_path}. Need date+time or datetime.")
    dt = dt.dropna()
    if dt.empty:
        return None
    # treat as IST wall-time already
    return dt.max().to_pydatetime()

def to_ist_naive(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    # If tz-aware, convert to IST then drop tz
    try:
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_convert(IST).dt.tz_localize(None)
    except Exception:
        pass
    return dt

def fetch_5m_range(kite: KiteConnect, token: int, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """Fetch 5m candles in 55-day chunks (safe for Kite limits)."""
    all_rows = []
    cur = start_dt
    # normalize to chunk boundaries
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=55), end_dt)
        # Kite expects datetimes
        data = kite.historical_data(
            instrument_token=token,
            from_date=cur,
            to_date=chunk_end,
            interval="5minute",
            continuous=False,
            oi=False
        )
        if data:
            all_rows.extend(data)
        time.sleep(0.35)  # rate limit friendly
        cur = chunk_end + timedelta(days=1)

    if not all_rows:
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume"])

    df = pd.DataFrame(all_rows)
    # Kite returns: date, open, high, low, close, volume
    df["DateTime"] = to_ist_naive(df["date"])
    out = pd.DataFrame({
        "DateTime": df["DateTime"],
        "Open": pd.to_numeric(df["open"], errors="coerce"),
        "High": pd.to_numeric(df["high"], errors="coerce"),
        "Low": pd.to_numeric(df["low"], errors="coerce"),
        "Close": pd.to_numeric(df["close"], errors="coerce"),
        "Volume": pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype("int64"),
    }).dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime")
    return out

def normalize_to_date_time_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    df = df.dropna(subset=["DateTime"]).sort_values("DateTime")
    df["date"] = df["DateTime"].dt.strftime("%Y-%m-%d")
    df["time"] = df["DateTime"].dt.strftime("%H:%M:%S")
    out = df[["date","time","Open","High","Low","Close","Volume"]].copy()
    out.columns = ["date","time","open","high","low","close","volume"]
    # numeric cleanup
    for c in ["open","high","low","close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")
    out = out.dropna(subset=["date","time","open","high","low","close"])
    return out

def load_existing_as_canonical(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame(columns=["date","time","open","high","low","close","volume"])

    # Auto-detect separator (comma vs tab etc.)
    df = pd.read_csv(csv_path, sep=None, engine="python")
    df.columns = [c.strip().lower() for c in df.columns]

    # If `date` column already includes time (datetime), split it into date+time
    if "date" in df.columns and "time" not in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce")
        if dt.notna().any():
            df["date"] = dt.dt.strftime("%Y-%m-%d")
            df["time"] = dt.dt.strftime("%H:%M:%S")

    # If `datetime` exists, derive date+time from it
    if ("date" not in df.columns or "time" not in df.columns) and "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], errors="coerce")
        df["date"] = dt.dt.strftime("%Y-%m-%d")
        df["time"] = dt.dt.strftime("%H:%M:%S")

    if not ("date" in df.columns and "time" in df.columns):
        raise RuntimeError(f"Existing CSV missing usable datetime columns: {csv_path} "
                           f"(need date+time OR date-as-datetime OR datetime)")

    # Ensure OHLCV exist
    for c in ["open","high","low","close"]:
        if c not in df.columns:
            raise RuntimeError(f"Missing column '{c}' in {csv_path}")
    if "volume" not in df.columns:
        df["volume"] = 0

    # Build canonical internal frame
    df["DateTime"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
    df = df.dropna(subset=["DateTime"]).sort_values("DateTime")

    out = df[["DateTime","open","high","low","close","volume"]].copy()
    out.columns = ["DateTime","Open","High","Low","Close","Volume"]

    # numeric cleanup
    for c in ["Open","High","Low","Close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce").fillna(0).astype("int64")

    out = out.dropna(subset=["DateTime","Open","High","Low","Close"]).reset_index(drop=True)
    return out


def resolve_tokens(kite: KiteConnect, symbols: list[str]) -> dict[str,int]:
    inst = kite.instruments("NSE")
    # tradingsymbol -> token
    mp = {}
    by_ts = {row["tradingsymbol"]: row["instrument_token"] for row in inst if row.get("segment") == "NSE"}
    missing = []
    for s in symbols:
        ts = s.strip().upper()
        tok = by_ts.get(ts)
        if tok is None:
            missing.append(ts)
        else:
            mp[ts] = tok
    if missing:
        raise RuntimeError(f"Could not resolve NSE instrument_token for: {missing}")
    return mp

def pick_end_date(default_today: date) -> date:
    # If market still running (before 15:30), use yesterday to avoid partial day in backtest files.
    now = datetime.now()
    if now.time() < dtime(15, 31):
        return default_today - timedelta(days=1)
    return default_today

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True, help="Path to kite_session.json")
    ap.add_argument("--dir", required=True, help="Folder containing *_5MINUTE.csv files")
    ap.add_argument("--symbols", nargs="+", required=True, help="e.g. HAL PNB ADANIPOWER ADANIGREEN NTPC")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (default: last completed trading day)")
    ap.add_argument("--rebuild_from", default=None, help="YYYY-MM-DD (if set, IGNORE existing CSV and rebuild from this date)")
    args = ap.parse_args()

    kite = load_kite_from_session(args.session)
    syms = [s.strip().upper() for s in args.symbols]
    tokens = resolve_tokens(kite, syms)

    if args.end:
        end_day = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        end_day = pick_end_date(date.today())

    end_dt = datetime.combine(end_day, dtime(15, 30))

    for s in syms:
        csv_path = os.path.join(args.dir, f"{s}_5MINUTE.csv")
        if args.rebuild_from:
            existing = pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Volume"])
            start_dt = datetime.strptime(args.rebuild_from, "%Y-%m-%d")
            start_dt = start_dt.replace(hour=9, minute=15, second=0, microsecond=0)
        else:
            existing = load_existing_as_canonical(csv_path)
            last_dt = existing["DateTime"].max().to_pydatetime() if not existing.empty else None
            if last_dt is None:
                start_dt = datetime(2015, 1, 1, 9, 15)
            else:
                start_dt = last_dt + timedelta(minutes=5)


        if start_dt > end_dt:
            print(f"[{s}] already up-to-date. last_dt={last_dt}")
            continue

        print(f"[{s}] fetching from {start_dt} to {end_dt} ...")
        fetched = fetch_5m_range(kite, tokens[s], start_dt, end_dt)

        if fetched.empty:
            print(f"[{s}] no new candles returned.")
            continue

        merged = pd.concat([existing, fetched], ignore_index=True)
        merged = merged.drop_duplicates(subset=["DateTime"], keep="last").sort_values("DateTime").reset_index(drop=True)

        out = normalize_to_date_time_cols(merged)
        os.makedirs(args.dir, exist_ok=True)
        out.to_csv(csv_path, index=False)
        print(f"[{s}] updated -> {csv_path} | rows={len(out)} | last={out.iloc[-1]['date']} {out.iloc[-1]['time']}")

    print("DONE")

if __name__ == "__main__":
    main()
