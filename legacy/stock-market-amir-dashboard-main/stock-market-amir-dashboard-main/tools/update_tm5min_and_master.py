# tools/update_tm5min_and_master.py
from __future__ import annotations
import os
import argparse
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

from probedge.core.classifiers import (
    compute_prevdaycontext_robust,
    compute_openingtrend_robust,
    compute_result_0940_1505,
    compute_openlocation_from_df,
    compute_first_candletype,
    compute_rangestatus,
    prev_trading_day_ohlc,
    slice_window as _slice_between,
)

# --- repo-local defaults ---
TM5_PATH = Path("data/intraday/tm5min.csv")
TM_MASTER_PATH = Path("data/masters/TataMotors_Master.csv")
SYMBOL = "NSE:TATAMOTORS"

# Try to reuse intraday helpers if present (preferred for Kite fetch)
try:
    from app.intraday_utils import slice_intraday_by_dates, try_fetch_kite_5m_for_dates
except Exception:
    slice_intraday_by_dates = None
    try_fetch_kite_5m_for_dates = None

def _detect_repo_root() -> Path:
    here = Path(__file__).resolve()
    for p in [here, *here.parents]:
        if (p / ".git").exists() or (p / "data").exists():
            return p
    return here.parent

REPO_ROOT = _detect_repo_root()

TM5_PRIMARY = (REPO_ROOT / "data/intraday/tm5min.csv").resolve()
TM5_ALT     = (REPO_ROOT / "data/intraday/TATAMOTORS/tm5min.csv").resolve()
TM5_PATH    = TM5_PRIMARY
TM_MASTER_PATH = (REPO_ROOT / "data/masters/TataMotors_Master.csv").resolve()

def _read_tm5() -> pd.DataFrame:
    def _load_one(p: Path) -> pd.DataFrame:
        if not p.exists():
            return pd.DataFrame()
        df = pd.read_csv(p)
        cols = {c.lower(): c for c in df.columns}
        def c_(x): return cols.get(x.lower(), x)
        df = df.rename(columns={
            c_("DateTime"): "DateTime", c_("datetime"): "DateTime",
            c_("Open"): "Open",  c_("open"): "Open",
            c_("High"): "High",  c_("high"): "High",
            c_("Low"): "Low",    c_("low"): "Low",
            c_("Close"): "Close",c_("close"): "Close",
            c_("Date"): "Date",
        })
        df["DateTime"] = _to_ist_naive_series(df["DateTime"])
        if "Date" not in df.columns or df["Date"].isna().all():
            df["Date"] = df["DateTime"].dt.date.astype(str)
        return df

    dfs = [_load_one(TM5_PRIMARY), _load_one(TM5_ALT)]
    dfs = [d for d in dfs if not d.empty]
    if not dfs:
        TM5_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
        TM5_ALT.parent.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close","Date"])

    df = pd.concat(dfs, ignore_index=True)
    df = df.dropna(subset=["DateTime","Open","High","Low","Close"])
    df = df.sort_values("DateTime").drop_duplicates(subset=["DateTime"], keep="last").reset_index(drop=True)
    return df

def _write_tm5(df: pd.DataFrame) -> str:
    out = df.copy()
    out["DateTime"] = pd.to_datetime(out["DateTime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    for p in (TM5_PRIMARY, TM5_ALT):
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".csv.tmp")
        out.to_csv(tmp, index=False)
        os.replace(tmp, p)

    return str(TM5_PRIMARY.resolve())

def _read_master() -> pd.DataFrame:
    if not TM_MASTER_PATH.exists():
        TM_MASTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        return pd.DataFrame(columns=[
            "Date","PrevDayContext","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus","Result"
        ])
    m = pd.read_csv(TM_MASTER_PATH)
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
    return m.dropna(subset=["Date"]).copy()

def _write_master(df: pd.DataFrame) -> None:
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    TM_MASTER_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(TM_MASTER_PATH, index=False)

def _to_ist_naive_series(s):
    s = pd.to_datetime(s, errors="coerce")
    try:
        if getattr(s.dt, "tz", None) is not None:
            return s.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    except Exception:
        try:
            return s.dt.tz_localize(None)
        except Exception:
            pass
    return s

def _to_ist_naive_ts(ts):
    ts = pd.to_datetime(ts, errors="coerce")
    try:
        if ts.tzinfo is not None:
            return ts.tz_convert("Asia/Kolkata").tz_localize(None)
    except Exception:
        try:
            return ts.tz_localize(None)
        except Exception:
            pass
    return ts

def _trading_days_to_fetch(last_dt_in_file: Optional[pd.Timestamp]) -> List[pd.Timestamp]:
    today = _to_ist_naive_ts(pd.Timestamp.today()).normalize()
    # include last day to extend current-day bars
    start = today if last_dt_in_file is None else _to_ist_naive_ts(last_dt_in_file).normalize()
    days = pd.date_range(start=start, end=today, freq="D")
    return [d for d in days if d.weekday() < 5]

def update_tm5min(kite=None, start: Optional[str]=None, end: Optional[str]=None) -> Dict:
    df = _read_tm5()
    prev_rows = len(df)

    if start and end:
        want_days = pd.date_range(start=pd.to_datetime(start), end=pd.to_datetime(end), freq="D")
        want_days = [d for d in want_days if d.weekday() < 5]
    else:
        last_dt = None if df.empty else pd.to_datetime(df["DateTime"].iloc[-1])
        want_days = _trading_days_to_fetch(last_dt)

    if not want_days:
        return {"ok": True, "msg": "No trading days to fetch.", "rows_added": 0, "path": str(TM5_PATH)}

    fetched = {}
    if try_fetch_kite_5m_for_dates is not None:
        try:
            # prefer the robust one you just integrated
            fetched = try_fetch_kite_5m_for_dates("tm", want_days, kite)
        except TypeError:
            # older signature fallback
            fetched = try_fetch_kite_5m_for_dates("tm", want_days, kite)

    add_rows = []
    for d in want_days:
        day_df = fetched.get(pd.to_datetime(d).normalize(), pd.DataFrame())
        if day_df is None or day_df.empty:
            continue
        g = day_df.copy()
        cols = {c.lower(): c for c in g.columns}
        def c_(x): return cols.get(x.lower(), x)
        g = g.rename(columns={
            c_("DateTime"): "DateTime", c_("datetime"): "DateTime",
            c_("Open"): "Open",  c_("open"): "Open",
            c_("High"): "High",  c_("high"): "High",
            c_("Low"): "Low",    c_("low"): "Low",
            c_("Close"): "Close",c_("close"): "Close",
            c_("Volume"): "Volume", c_("volume"): "Volume",
        })
        g["DateTime"] = pd.to_datetime(g["DateTime"], errors="coerce")
        g["DateTime"] = _to_ist_naive_series(g["DateTime"])
        g = g.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime")
        if g.empty:
            continue
        g["Date"] = g["DateTime"].dt.normalize().dt.strftime("%Y-%m-%d")
        add_rows.append(g[["DateTime","Open","High","Low","Close","Date"]])

    if add_rows:
        new_df = pd.concat([df] + add_rows, ignore_index=True)
        new_df = new_df.sort_values("DateTime").drop_duplicates(subset=["DateTime"], keep="last").reset_index(drop=True)
        delta = len(new_df) - prev_rows
        written = _write_tm5(new_df)
        return {
            "ok": True,
            "msg": "Appended bars" if delta > 0 else "No new bars (dedup)",
            "rows_added": max(0, delta),
            "path": written,
            "rows_now": len(new_df),
            "mirrors": [str(TM5_PRIMARY.resolve()), str(TM5_ALT.resolve())],
        }

    return {"ok": True, "msg": "No new bars fetched", "rows_added": 0, "path": str(TM5_PATH)}

def update_master_from_tm5min(target_day: Optional[str]=None) -> Dict:
    intr = _read_tm5()
    if intr.empty:
        return {"ok": False, "msg": "tm5min is empty."}

    day = pd.to_datetime(target_day).normalize() if target_day else pd.to_datetime(intr["DateTime"].dt.normalize().max())
    day_df = intr[intr["DateTime"].dt.normalize().eq(day)].copy()
    prev_ohlc = prev_trading_day_ohlc(intr, day)
    prev_ctx = compute_prevdaycontext_robust(prev_ohlc["open"], prev_ohlc["high"], prev_ohlc["low"], prev_ohlc["close"]) if prev_ohlc else "TR"

    open_loc      = compute_openlocation_from_df(day_df, prev_ohlc)
    first_candle  = compute_first_candletype(day_df, prev_ohlc=prev_ohlc)
    opening_trend = compute_openingtrend_robust(day_df)
    range_status  = compute_rangestatus(day_df, open_loc, prev_ohlc)
    result_label, _ = compute_result_0940_1505(day_df)

    m = _read_master()
    row = {"Date": day, "PrevDayContext": prev_ctx, "OpenLocation": open_loc, "FirstCandleType": first_candle,
           "OpeningTrend": opening_trend, "RangeStatus": range_status, "Result": result_label}

    if m.empty:
        out = pd.DataFrame([row])
    else:
        m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
        mask = m["Date"].eq(day)
        if mask.any():
            idx = m.index[mask][0]
            for k, v in row.items(): m.at[idx, k] = v
            out = m
        else:
            out = pd.concat([m, pd.DataFrame([row])], ignore_index=True)
    _write_master(out)
    return {"ok": True, "msg": "Upserted master", "path": str(TM_MASTER_PATH), "date": day.date().isoformat()}

def run_daily_update(kite=None, when: Optional[str]=None) -> Dict:
    _ = update_tm5min(kite=kite)
    return update_master_from_tm5min(target_day=when)

def _parse_args():
    ap = argparse.ArgumentParser(description="Update tm5min and master (TM) from Kite")
    ap.add_argument("--start", help="YYYY-MM-DD (backfill start, optional)")
    ap.add_argument("--end", help="YYYY-MM-DD (backfill end, optional)")
    ap.add_argument("--date", help="YYYY-MM-DD (update master for this day; default last day in tm5min)")
    ap.add_argument("--only-intraday", action="store_true", help="Only update tm5min")
    ap.add_argument("--only-master", action="store_true", help="Only update master (no fetch)")
    return ap.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    if args.only_intraday:
        print(update_tm5min(kite=None, start=args.start, end=args.end))
    elif args.only_master:
        print(update_master_from_tm5min(target_day=args.date))
    else:
        print(run_daily_update(kite=None, when=args.date))
