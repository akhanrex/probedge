# probedge/updater/weekly.py
from __future__ import annotations
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, Any, Optional, Tuple
import numpy as np
import pandas as pd

from probedge.core.classifiers import (
    compute_prevdaycontext_robust,
    compute_openingtrend_robust,
    compute_result_0940_1505,
    compute_first_candletype,
    compute_rangestatus,
    compute_openlocation,  # needs today_open + prev H/L
)

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    from backports.zoneinfo import ZoneInfo  # fallback if needed
IST = ZoneInfo("Asia/Kolkata")

__all__ = ["update_master_if_needed", "compute_live_weekly_tags"]

# ----------------------------- Zerodha helpers -----------------------------
def _get_instrument_token(kite, tradingsymbol: str, exchange: str = "NSE") -> Optional[int]:
    try:
        ts = tradingsymbol.replace(f"{exchange}:", "") if ":" in tradingsymbol else tradingsymbol
        ins = kite.instruments(exchange)
        for row in ins:
            if row.get("tradingsymbol") == ts:
                return int(row.get("instrument_token"))
    except Exception:
        pass
    return None

def _fetch_5min_day(kite, instrument_token: int, day: date) -> pd.DataFrame:
    if instrument_token is None:
        return pd.DataFrame()
    try:
        start_dt = datetime.combine(day, dtime(9, 15)).replace(tzinfo=IST)
        end_dt = datetime.combine(day, dtime(15, 30)).replace(tzinfo=IST)
        data = kite.historical_data(instrument_token, start_dt, end_dt, interval="5minute", oi=False)
        df = pd.DataFrame(data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame()

# ----------------------------- IO helpers -----------------------------
def _atomic_save_csv(df: pd.DataFrame, path: str) -> None:
    import os, tempfile, shutil
    tmpdir = tempfile.mkdtemp()
    try:
        tmpfile = os.path.join(tmpdir, "master_tmp.csv")
        df.to_csv(tmpfile, index=False)
        shutil.move(tmpfile, path)
    finally:
        try: os.rmdir(tmpdir)
        except Exception: pass

def _parse_master(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=[
            "Date","PrevDayContext","OpenLocation","FirstCandleType",
            "OpeningTrend","RangeStatus","DayHigh","DayLow","Result"
        ])
    def _pd(x):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try: return pd.to_datetime(x, format=fmt)
            except Exception: continue
        return pd.to_datetime(x, errors="coerce")
    if "Date" not in df.columns: df["Date"] = np.nan
    df["Date"] = df["Date"].apply(_pd)
    df = df[df["Date"].notna()].copy()
    for c in ["PrevDayContext","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus","DayHigh","DayLow","Result"]:
        if c not in df.columns: df[c] = np.nan
    return df

def _ist_now() -> datetime:
    return datetime.now(IST)

def _is_market_closed_now() -> bool:
    now = _ist_now().time()
    return (now.hour, now.minute) >= (15, 40)

def _next_trading_date(d: date) -> date:
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:  # Sat/Sun
        nd += timedelta(days=1)
    return nd

# ----------------------------- Prev-day helpers -----------------------------
from typing import Optional as _Opt

def _prev_day_row(master_df: pd.DataFrame, today_d: date) -> _Opt[pd.Series]:
    if master_df is None or master_df.empty or "Date" not in master_df.columns:
        return None
    m = master_df.copy()
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.date
    m = m[m["Date"].notna()]
    prev = m[m["Date"] < today_d]
    if prev.empty: return None
    return prev.sort_values("Date").iloc[-1]

def _extract_prev_HL(prev_row: _Opt[pd.Series]) -> Tuple[_Opt[float], _Opt[float]]:
    if prev_row is None: return None, None
    highs, lows = ("DAYHIGH","HIGH","PREVDAYHIGH","PDH"), ("DAYLOW","LOW","PREVDAYLOW","PDL")
    pr = {k.upper(): v for k, v in prev_row.items()}
    pd_high = next((float(pr[h]) for h in highs if h in pr and pd.notna(pr.get(h))), None)
    pd_low  = next((float(pr[l]) for l in lows  if l in pr and pd.notna(pr.get(l))), None)
    return pd_high, pd_low

def _robust_prevday_context_from_fetch(kite, token: int, d: date) -> str:
    # fetch previous trading day's bars and compute robust PDC
    prev_d = d - timedelta(days=1)
    while prev_d.weekday() >= 5:
        prev_d -= timedelta(days=1)
    prev_bars = _fetch_5min_day(kite, token, prev_d)
    if prev_bars.empty:
        return "TR"
    o = float(prev_bars.iloc[0]["open"])
    c = float(prev_bars.iloc[-1]["close"])
    H = float(prev_bars["high"].max())
    L = float(prev_bars["low"].min())
    return compute_prevdaycontext_robust(o, H, L, c)

# ----------------------------- Public: updater -----------------------------
def update_master_if_needed(kite, master_csv_path: str, symbol: str = "NSE:TATAMOTORS", upto: Optional[date] = None) -> int:
    df = _parse_master(master_csv_path)
    today_d = _ist_now().date()
    stop_d = upto or (today_d if _is_market_closed_now() else (today_d - timedelta(days=1)))

    last_have = df["Date"].max().date() if len(df) else None
    if last_have is not None and last_have >= stop_d:
        return 0

    token = _get_instrument_token(kite, symbol)
    if token is None:
        return 0

    cur = (last_have + timedelta(days=1)) if last_have else stop_d
    if last_have is None:
        found_any = False
        probe = stop_d
        for _ in range(8):
            if probe.weekday() >= 5:
                probe -= timedelta(days=1); continue
            bars = _fetch_5min_day(kite, token, probe)
            if isinstance(bars, pd.DataFrame) and not bars.empty:
                cur = probe; found_any = True; break
            probe -= timedelta(days=1)
        if not found_any: return 0

    def _pdh_pdl_for_prev(d: date) -> tuple[Optional[float], Optional[float]]:
        prev_row = _prev_day_row(df, d)
        pdh, pdl = _extract_prev_HL(prev_row)
        if pdh is not None and pdl is not None and pdh > pdl:
            return float(pdh), float(pdl)
        prev_d = d - timedelta(days=1)
        while prev_d.weekday() >= 5:
            prev_d -= timedelta(days=1)
        prev_bars = _fetch_5min_day(kite, token, prev_d)
        if prev_bars.empty: return None, None
        return float(prev_bars["high"].max()), float(prev_bars["low"].min())

    new_rows = []
    d = cur
    while d <= stop_d:
        if d.weekday() >= 5:
            d = _next_trading_date(d); continue
        bars = _fetch_5min_day(kite, token, d)
        if bars.empty:
            d = _next_trading_date(d); continue

        bars = bars.sort_values("date").reset_index(drop=True)
        day_high, day_low = float(bars["high"].max()), float(bars["low"].min())
        pdh, pdl = _pdh_pdl_for_prev(d)
        today_open = float(bars.iloc[0]["open"]) if pd.notna(bars.iloc[0].get("open")) else None

        # canonical intraday df for classifier functions
        day_df = bars.rename(columns={
            "date":"DateTime", "open":"Open", "high":"High", "low":"Low", "close":"Close"
        })[["DateTime","Open","High","Low","Close"]].copy()

        # PrevDayContext (robust)
        prev_ctx = _robust_prevday_context_from_fetch(kite, token, d)

        # tags via classifier
        prev_ohlc = {"high": float(pdh), "low": float(pdl), "open": np.nan, "close": np.nan} if (pdh is not None and pdl is not None) else None

        open_loc = compute_openlocation(today_open, prev_ohlc) if (today_open is not None and prev_ohlc) else ""
        fc_type  = compute_first_candletype(day_df, prev_ohlc=prev_ohlc) or ""
        o_trend  = compute_openingtrend_robust(day_df) or ""
        r_stat   = compute_rangestatus(day_df, open_loc, prev_ohlc) or ""

        # robust post-open result (canonical)
        result_label, _ = compute_result_0940_1505(day_df)

        new_rows.append({
            "Date": d,
            "PrevDayContext": prev_ctx,
            "OpenLocation": open_loc,
            "FirstCandleType": fc_type,
            "OpeningTrend": o_trend,
            "RangeStatus": r_stat,
            "DayHigh": day_high,
            "DayLow": day_low,
            "Result": result_label,
        })

        # extend df in-memory so subsequent days can see previous row
        df = pd.concat([df, pd.DataFrame([{
            "Date": pd.Timestamp(d),
            "PrevDayContext": prev_ctx,
            "OpenLocation": open_loc,
            "FirstCandleType": fc_type,
            "OpeningTrend": o_trend,
            "RangeStatus": r_stat,
            "DayHigh": day_high,
            "DayLow": day_low,
            "Result": result_label,
        }])], ignore_index=True)

        d = _next_trading_date(d)

    if not new_rows:
        return 0

    master = _parse_master(master_csv_path)
    add_df = pd.DataFrame(new_rows)
    add_df["Date"] = pd.to_datetime(add_df["Date"]).dt.normalize()
    master["Date"] = pd.to_datetime(master["Date"]).dt.normalize()

    merged = pd.concat([master, add_df], ignore_index=True)
    merged = merged.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")

    for c in ["PrevDayContext","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus","DayHigh","DayLow","Result"]:
        if c not in merged.columns: merged[c] = pd.NA

    out = merged.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    _atomic_save_csv(out, master_csv_path)
    return len(add_df)

# ----------------------------- Public: live compute -----------------------------
# ----------------------------- Public: live compute -----------------------------
def compute_live_weekly_tags(master_df: pd.DataFrame, kite, symbol: str = "NSE:TATAMOTORS") -> Dict[str, Any]:
    """
    Live tags computed using the centralized classifiers (single source of truth).
    """
    today = _ist_now().date()

    # Prev-day H/L from master (best-effort, used for OpenLocation banding)
    prev_row = _prev_day_row(master_df, today)
    pd_high, pd_low = _extract_prev_HL(prev_row)

    # Pull today’s 5m bars from Kite
    bars = pd.DataFrame()
    token = None
    try:
        token = _get_instrument_token(kite, symbol)
        bars = _fetch_5min_day(kite, token, today) if token is not None else pd.DataFrame()
    except Exception:
        bars = pd.DataFrame()

    # Normalize to classifier schema
    if isinstance(bars, pd.DataFrame) and not bars.empty:
        day_df = bars.rename(columns={
            "date": "DateTime", "open": "Open", "high": "High", "low": "Low", "close": "Close"
        })[["DateTime", "Open", "High", "Low", "Close"]].copy()
        day_df["DateTime"] = pd.to_datetime(day_df["DateTime"], errors="coerce")
        day_df = day_df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"]).sort_values("DateTime")
    else:
        day_df = pd.DataFrame(columns=["DateTime", "Open", "High", "Low", "Close"])

    # Today's open
    try:
        today_open = float(day_df["Open"].iloc[0]) if not day_df.empty else None
    except Exception:
        today_open = None

    # Live PrevDayContext (ROBUST): compute from actual prior-day OHLC via fetch (not from prior master row)
    try:
        prev_ctx = _robust_prevday_context_from_fetch(kite, token, today) if token is not None else "TR"
    except Exception:
        prev_ctx = "TR"

    # Build prev OHLC dict for OpenLocation (if we have H/L)
    prev_ohlc = None
    if pd_high is not None and pd_low is not None:
        prev_ohlc = {"open": np.nan, "close": np.nan, "high": float(pd_high), "low": float(pd_low)}

    # Compute tags (centralized)
    prev_ohlc = None
    if pd_high is not None and pd_low is not None:
        prev_ohlc = {"open": np.nan, "close": np.nan, "high": float(pd_high), "low": float(pd_low)}

    # OpenLocation first (needed by RangeStatus)
    try:
        open_location = compute_openlocation(today_open, prev_ohlc) if (prev_ohlc and today_open is not None) else None
    except Exception:
        open_location = None

    # FirstCandleType needs prev_ohlc
    try:
        first_candle = compute_first_candletype(day_df, prev_ohlc=prev_ohlc)
    except Exception:
        first_candle = None

    # OpeningTrend as before
    try:
        opening_trend = compute_openingtrend_robust(day_df)
    except Exception:
        opening_trend = None

    # RangeStatus needs OpenLocation + prev_ohlc
    try:
        range_status = compute_rangestatus(day_df, open_location, prev_ohlc)
    except Exception:
        range_status = None


    # Phase readiness gates
    bars_len = int(len(day_df)) if isinstance(day_df, pd.DataFrame) else 0
    ready_prev = (pd_high is not None and pd_low is not None and pd_high > pd_low)
    ready_open = isinstance(today_open, float)
    ready_bar1 = bars_len >= 1
    ready_bar5 = bars_len >= 5

    tags = {
        "PrevDayContext": prev_ctx or "TR",
        "OpenLocation": open_location,
        "FirstCandleType": first_candle,
        "OpeningTrend": opening_trend,
        "RangeStatus": range_status,
    }

    return {
        "today_open": today_open,
        "pd_high": pd_high,
        "pd_low": pd_low,
        "bars_len": bars_len,
        "bars": day_df,
        "ready": {
            "prev": ready_prev,
            "open": ready_open,
            "bar1": ready_bar1,
            "bar5": ready_bar5,
        },
        "phase": (3 if ready_bar5 else 2 if ready_bar1 else 1 if (ready_prev and ready_open) else 0),
        "tags": tags,
    }
