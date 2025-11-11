# app/intraday_utils.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from datetime import time as dtime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

INTRADAY_FILES = {
    "tm":   "data/intraday/tm5min.csv",
    "lt":   "data/intraday/LT_5minute.csv",
    "sbin": "data/intraday/SBIN_5minute.csv",
}
ALT_INTRADAY_FILES = {
    "tm": "data/intraday/TATAMOTORS/tm5min.csv",
}

_INTRADAY_CACHE: Dict[str, pd.DataFrame] = {}

def _full_path_for(inst_key: str) -> str:
    return INTRADAY_FILES.get(inst_key, "")

def _norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime","open","high","low","close","volume","date"])
    g = df.copy()
    cols_lc = {c.lower(): c for c in g.columns}
    def have(x): return x in cols_lc
    def col(x):  return cols_lc.get(x)
    ren = {}
    for src, dst in (("open","open"),("high","high"),("low","low"),("close","close"),("volume","volume")):
        if have(src): ren[col(src)] = dst
    g.rename(columns=ren, inplace=True)
    if have("datetime"):
        g["datetime"] = pd.to_datetime(g[col("datetime")], errors="coerce")
    elif have("date"):
        g["datetime"] = pd.to_datetime(g[col("date")], errors="coerce")
    else:
        raise ValueError("No 'datetime' or 'date' column to parse timestamps from")
    for c in ("open","high","low","close","volume"):
        if c in g.columns:
            g[c] = pd.to_numeric(g[c], errors="coerce")
    g = g.dropna(subset=["datetime","open","high","low","close"]).copy()
    try:
        tz = g["datetime"].dt.tz
    except Exception:
        tz = None
    if tz is None:
        g["datetime"] = g["datetime"].dt.tz_localize(IST)
    else:
        g["datetime"] = g["datetime"].dt.tz_convert(IST)
    start_t, end_t = dtime(9, 15), dtime(15, 30)
    g = g[(g["datetime"].dt.time >= start_t) & (g["datetime"].dt.time <= end_t)].copy()
    if "volume" not in g.columns:
        g["volume"] = np.nan
    g["date"] = g["datetime"].dt.date
    g = g.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    return g[["datetime","open","high","low","close","volume","date"]]

def load_intraday_all(inst_key: str, *, force_reload: bool = False) -> pd.DataFrame:
    if (not force_reload) and (inst_key in _INTRADAY_CACHE):
        return _INTRADAY_CACHE[inst_key]
    primary = _full_path_for(inst_key)
    alt = ALT_INTRADAY_FILES.get(inst_key, None)
    def _safe_read(p):
        if not p or not os.path.exists(p):
            return pd.DataFrame()
        try:
            return pd.read_csv(p)
        except Exception:
            try:
                return pd.read_csv(p, engine="python")
            except Exception:
                return pd.DataFrame()
    frames = []
    df_primary = _safe_read(primary)
    df_alt = _safe_read(alt) if alt else pd.DataFrame()
    if not df_primary.empty: frames.append(df_primary)
    if not df_alt.empty:     frames.append(df_alt)
    if not frames:
        g = pd.DataFrame(columns=["datetime","open","high","low","close","volume","date"])
        _INTRADAY_CACHE[inst_key] = g
        return g
    merged_raw = pd.concat(frames, ignore_index=True)
    g = _norm_columns(merged_raw)
    g = g.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    _INTRADAY_CACHE[inst_key] = g
    return g

def latest_dates_from_matches(df_matches: pd.DataFrame, max_days: int = 10) -> List[pd.Timestamp]:
    if df_matches is None or df_matches.empty or "Date" not in df_matches.columns:
        return []
    d = pd.to_datetime(df_matches["Date"], errors="coerce").dropna().dt.normalize()
    uniq = d.drop_duplicates().sort_values(ascending=False)
    return uniq.head(max_days).tolist()

def slice_intraday_by_dates(inst_key: str, dates: List[pd.Timestamp]) -> Dict[pd.Timestamp, pd.DataFrame]:
    out: Dict[pd.Timestamp, pd.DataFrame] = {}
    g = load_intraday_all(inst_key)
    if g.empty:
        return out
    g["date_ts"] = pd.to_datetime(g["date"]).dt.normalize()
    want = set(pd.to_datetime(pd.Series(dates)).dt.normalize().tolist())
    sub = g.loc[g["date_ts"].isin(want)].copy()
    for dt in sorted(want, reverse=True):
        dd = sub.loc[sub["date_ts"].eq(dt)].copy()
        if not dd.empty:
            out[dt] = dd
    return out

INSTRUMENT_TOKEN = {
    "NSE:TATAMOTORS": 884737,
    "NSE:LT":         None,
    "NSE:SBIN":       None,
    "NSE:ADANIENT":   6401,
}

def _kite_symbol_for(inst_key: str) -> str:
    _map = {"tm": "NSE:TATAMOTORS", "lt": "NSE:LT", "sbin": "NSE:SBIN", "ae": "NSE:ADANIENT"}
    return _map.get(inst_key, "")

def _instrument_token_for(inst_key: str) -> Optional[int]:
    sym = _kite_symbol_for(inst_key)
    return INSTRUMENT_TOKEN.get(sym)

def _resolve_kite_token_dynamically(kite, inst_key: str) -> Optional[int]:
    try:
        if kite is None:
            return None
        if not hasattr(kite, "_pe_instr_cache"):
            kite._pe_instr_cache = {}
        cache = kite._pe_instr_cache
        if "NSE" not in cache:
            cache["NSE"] = kite.instruments("NSE")
        symbols = {"tm":"TATAMOTORS","lt":"LT","sbin":"SBIN","ae":"ADANIENT"}
        tsym = symbols.get(inst_key, "")
        if not tsym:
            return None
        matches = [r for r in cache["NSE"] if str(r.get("tradingsymbol")) == tsym]
        eq = [r for r in matches if str(r.get("segment")) == "NSE" and str(r.get("instrument_type")) in ("EQ","EQN")]
        chosen = eq[0] if eq else (matches[0] if matches else None)
        return int(chosen["instrument_token"]) if chosen and "instrument_token" in chosen else None
    except Exception:
        return None

def try_fetch_kite_5m_for_dates(inst_key: str, dates: List[pd.Timestamp], kite) -> Dict[pd.Timestamp, pd.DataFrame]:
    if kite is None or not dates:
        return {}
    def _fetch_for_dates(tok: Optional[int]) -> Dict[pd.Timestamp, pd.DataFrame]:
        out: Dict[pd.Timestamp, pd.DataFrame] = {}
        if not tok:
            return out
        for dt in dates:
            start = pd.Timestamp(dt.date(), tz=IST).replace(hour=9, minute=15, second=0, microsecond=0)
            end   = pd.Timestamp(dt.date(), tz=IST).replace(hour=15, minute=30, second=0, microsecond=0)
            try:
                data = kite.historical_data(
                    instrument_token=tok,
                    from_date=start.tz_localize(None).to_pydatetime(),
                    to_date=end.tz_localize(None).to_pydatetime(),
                    interval="5minute", continuous=False, oi=False,
                )
                if not data:
                    continue
                kdf = pd.DataFrame(data)
                if kdf.empty:
                    continue
                kdf = kdf.rename(columns={"date": "datetime"})
                kdf["datetime"] = pd.to_datetime(kdf["datetime"], errors="coerce")
                try:
                    if kdf["datetime"].dt.tz is None:
                        kdf["datetime"] = kdf["datetime"].dt.tz_localize(IST)
                    else:
                        kdf["datetime"] = kdf["datetime"].dt.tz_convert(IST)
                except Exception:
                    kdf["datetime"] = pd.to_datetime(kdf["datetime"], errors="coerce")
                    kdf["datetime"] = kdf["datetime"].dt.tz_localize(IST)
                kdf["date"] = kdf["datetime"].dt.date
                kdf = kdf[["datetime","open","high","low","close","volume","date"]]
                kdf = kdf[(kdf["datetime"].dt.time >= dtime(9,15)) & (kdf["datetime"].dt.time <= dtime(15,30))].copy()
                out[pd.to_datetime(dt).normalize()] = kdf.sort_values("datetime").reset_index(drop=True)
            except Exception:
                continue
        return out
    token_static = _instrument_token_for(inst_key)
    results = _fetch_for_dates(token_static)
    if not results:
        token_dyn = _resolve_kite_token_dynamically(kite, inst_key)
        if token_dyn and token_dyn != token_static:
            results = _fetch_for_dates(token_dyn)
    return results

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

def _nearest_prev_trading_date(all_dates: pd.Series, cur: pd.Timestamp) -> Optional[pd.Timestamp]:
    if all_dates is None or all_dates.empty:
        return None
    a = pd.to_datetime(all_dates).dt.normalize().drop_duplicates().sort_values()
    idx = a.searchsorted(cur.normalize())
    return a.iloc[idx - 1] if idx > 0 else None

def ensure_intraday_up_to_date(inst_key: str, kite=None) -> List[pd.Timestamp]:
    cur = load_intraday_all(inst_key, force_reload=True)
    if (cur is None or cur.empty) and (kite is not None):
        from pandas.tseries.offsets import BDay
        today_ist = pd.Timestamp.now(IST).normalize()
        want_dates = [(today_ist - BDay(i)).normalize() for i in range(0, 6)]
        fetched = try_fetch_kite_5m_for_dates(inst_key, want_dates, kite)
        if fetched:
            frames = [df_day for _, df_day in fetched.items() if df_day is not None and not df_day.empty]
            if frames:
                merged = _norm_columns(pd.concat(frames, ignore_index=True))
                merged = merged.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
                path = _full_path_for(inst_key)
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                out = merged.copy()
                out["datetime"] = out["datetime"].dt.tz_convert(IST).dt.tz_localize(None)
                out.to_csv(path, index=False)
                _INTRADAY_CACHE[inst_key] = merged
                return sorted(pd.to_datetime(merged["date"]).dropna().map(pd.Timestamp).map(lambda x: x.normalize()).unique().tolist())
        return []
    before = set(pd.to_datetime(cur["date"]).dropna().map(pd.Timestamp).map(lambda x: x.normalize()))
    last_present = pd.to_datetime(cur["date"]).dropna().max()
    if pd.isna(last_present):
        return []
    last_present = pd.Timestamp(last_present).tz_localize(None)
    today_ist = pd.Timestamp.now(IST).normalize()
    start_day = pd.Timestamp(last_present).normalize()
    if start_day > today_ist or kite is None:
        return []
    want_dates = []
    d = start_day
    while d <= today_ist:
        want_dates.append(d)
        d += pd.Timedelta(days=1)
    fetched = try_fetch_kite_5m_for_dates(inst_key, want_dates, kite)
    if not fetched:
        return []
    frames = [cur] if not cur.empty else []
    for _, df_day in fetched.items():
        if df_day is None or df_day.empty:
            continue
        frames.append(df_day)
    if not frames:
        return []
    merged = pd.concat(frames, ignore_index=True)
    merged = _norm_columns(merged)
    merged = merged.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    path = _full_path_for(inst_key)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    out = merged.copy()
    out["datetime"] = out["datetime"].dt.tz_convert(IST).dt.tz_localize(None)
    out.to_csv(path, index=False)
    _INTRADAY_CACHE[inst_key] = merged
    after = set(pd.to_datetime(merged["date"]).dropna().map(pd.Timestamp).map(lambda x: x.normalize()))
    added = sorted(list(after - before))
    return added

def upsert_master_from_5m(inst_key: str, master_path: str, dates: List[pd.Timestamp]) -> int:
    if not dates:
        return 0
    g = load_intraday_all(inst_key)
    if g is None or g.empty:
        return 0
    def norm_day(ts: pd.Timestamp) -> pd.Timestamp:
        return pd.to_datetime(ts).normalize()
    want = [norm_day(d) for d in dates]
    try:
        m = pd.read_csv(master_path)
    except Exception:
        m = pd.DataFrame()
    if "Date" not in m.columns:
        m = pd.DataFrame(columns=["Date","Open","High","Low","Close"])
    for c in ("Open","High","Low","Close"):
        if c not in m.columns:
            m[c] = np.nan
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
    g["date_ts"] = pd.to_datetime(g["date"]).map(pd.Timestamp).map(lambda x: x.normalize())
    touched = 0
    rows_to_append = []
    for d in want:
        day = g.loc[g["date_ts"].eq(d)].copy()
        if day.empty:
            continue
        day = day.sort_values("datetime")
        ohlc = {
            "Date":  d,
            "Open":  float(day["open"].iloc[0]),
            "High":  float(day["high"].max()),
            "Low":   float(day["low"].min()),
            "Close": float(day["close"].iloc[-1]),
        }
        idx_list = m.index[m["Date"].eq(d)].tolist()
        if not idx_list:
            full_row = {c: (ohlc[c] if c in ohlc else np.nan) for c in m.columns}
            for c in ("Date","Open","High","Low","Close"):
                if c not in full_row:
                    full_row[c] = ohlc.get(c, np.nan)
            rows_to_append.append(full_row)
            touched += 1
        else:
            i = idx_list[-1]
            needs_update = False
            for c in ("Open","High","Low","Close"):
                val = m.at[i, c]
                if pd.isna(val) or (str(val).strip() == "") or (pd.to_numeric(val, errors="coerce") in (None, 0.0)):
                    m.at[i, c] = ohlc[c]
                    needs_update = True
            if needs_update:
                touched += 1
    if rows_to_append:
        m = pd.concat([m, pd.DataFrame(rows_to_append)], ignore_index=True)
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce")
    m = m.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    Path(master_path).parent.mkdir(parents=True, exist_ok=True)
    m_out = m.copy()
    m_out["Date"] = m_out["Date"].dt.strftime("%Y-%m-%d")
    m_out.to_csv(master_path, index=False)
    return touched

# ===== Full sync from 5m → master with tags (no Kite required) =====
from pathlib import Path as _Path

def _read_master_flexible(master_path: str) -> pd.DataFrame:
    if not master_path or not os.path.exists(master_path):
        return pd.DataFrame(columns=[
            "Date","PrevDayContext","OpenLocation","FirstCandleType",
            "OpeningTrend","RangeStatus","Result","Open","High","Low","Close"
        ])
    m = pd.read_csv(master_path)
    if "Date" not in m.columns:
        m["Date"] = pd.NaT
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
    return m.dropna(subset=["Date"]).copy()

def _write_master_flexible(df: pd.DataFrame, master_path: str) -> str:
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    _Path(master_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        out.to_csv(master_path, index=False)
        return master_path
    except Exception:
        alt = f"/mnt/data/{_Path(master_path).name}"
        out.to_csv(alt, index=False)
        return alt

def _to_tm_style(intra_norm: pd.DataFrame) -> pd.DataFrame:
    z = intra_norm.copy()
    z = z.rename(columns={
        "datetime": "DateTime",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })
    dt = pd.to_datetime(z["DateTime"], errors="coerce")
    try:
        tzinfo = dt.dt.tz
    except Exception:
        tzinfo = None
    if tzinfo is None:
        dt = dt.dt.tz_localize(IST)
    else:
        dt = dt.dt.tz_convert(IST)
    z["DateTime"] = dt.dt.tz_localize(None)
    return z[["DateTime","Open","High","Low","Close","Volume"]]

def ensure_5m_and_master_up_to_date(inst_key: str, master_path: str, kite=None) -> Dict[str, int]:
    """
    Backfill/extend 5m via Kite (if available) AND always sync/patch master from whatever 5m we have.
    Returns:
      {"dates_5m_added": N, "bars_5m_appended": B, "master_rows_added": M}
    where:
      - dates_5m_added = new trading dates added (not bars)
      - bars_5m_appended = new 5-min rows appended (including inside existing dates)
      - master_rows_added = rows inserted/updated in master (based on NaN/0 patch policy)
    """
    # Snapshot bars BEFORE
    try:
        g_before = load_intraday_all(inst_key, force_reload=True)
        bars_before = int(len(g_before)) if g_before is not None else 0
    except Exception:
        bars_before = 0

    # 1) Try to add/extend 5m days via Kite (safe no-op if kite is None)
    try:
        dates_added = ensure_intraday_up_to_date(inst_key, kite=kite)
    except Exception:
        dates_added = []

    # Snapshot bars AFTER
    try:
        g_after = load_intraday_all(inst_key, force_reload=True)
        bars_after = int(len(g_after)) if g_after is not None else bars_before
    except Exception:
        bars_after = bars_before

    bars_appended = max(0, bars_after - bars_before)

    # 2) Always sync master from ALL 5m dates (patch NaN/0 OHLC)
    try:
        g = load_intraday_all(inst_key)
        all_5m_dates = (
            pd.to_datetime(g["date"]).dt.normalize().drop_duplicates().tolist()
            if g is not None and not g.empty and "date" in g.columns else []
        )
    except Exception:
        all_5m_dates = []

    try:
        master_added = upsert_master_from_5m(inst_key, master_path, all_5m_dates)
    except Exception:
        master_added = 0

    return {
        "dates_5m_added": len(dates_added),
        "bars_5m_appended": bars_appended,
        "master_rows_added": master_added,
    }
    

def sync_master_full_from_5m(inst_key: str, master_path: str) -> Dict[str, int | str]:
    g = load_intraday_all(inst_key, force_reload=True)
    if g is None or g.empty:
        return {"rows_added": 0, "rows_updated": 0, "path": master_path}
    intr = _to_tm_style(g)
    intr["Day"] = intr["DateTime"].dt.normalize()
    m = _read_master_flexible(master_path)
    for c in ["PrevDayContext","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus","Result","Open","High","Low","Close"]:
        if c not in m.columns:
            m[c] = np.nan
    added, updated = 0, 0
    from probedge.core.classifiers import (
        compute_prevdaycontext_robust,
        compute_openingtrend_robust,
        compute_result_0940_1505,
        compute_openlocation_from_df,
        compute_first_candletype,
        compute_rangestatus,
        prev_trading_day_ohlc,
    )
    for day, day_df in intr.groupby("Day"):
        day_df = day_df.sort_values("DateTime").reset_index(drop=True)
        prev_ohlc = prev_trading_day_ohlc(intr, day)
        if prev_ohlc and all(k in prev_ohlc for k in ("open","high","low","close")):
            prev_ctx = compute_prevdaycontext_robust(prev_ohlc["open"], prev_ohlc["high"], prev_ohlc["low"], prev_ohlc["close"])
        else:
            prev_ctx = "TR"
        open_loc      = compute_openlocation_from_df(day_df, prev_ohlc)
        first_candle  = compute_first_candletype(day_df, prev_ohlc=prev_ohlc)
        opening_trend = compute_openingtrend_robust(day_df)
        range_status  = compute_rangestatus(day_df, open_loc, prev_ohlc)
        result_label, _ = compute_result_0940_1505(day_df)
        ohlc = {
            "Open":  float(day_df["Open"].iloc[0]),
            "High":  float(day_df["High"].max()),
            "Low":   float(day_df["Low"].min()),
            "Close": float(day_df["Close"].iloc[-1]),
        }
        mask = m["Date"].eq(day)
        row_data = {
            "Date": day,
            "PrevDayContext": prev_ctx,
            "OpenLocation": open_loc,
            "FirstCandleType": first_candle,
            "OpeningTrend": opening_trend,
            "RangeStatus": range_status,
            "Result": result_label,
            **ohlc,
        }
        if not mask.any():
            full_row = {c: (row_data[c] if c in row_data else (np.nan if c != "Date" else day)) for c in m.columns}
            for k, v in row_data.items():
                if k not in full_row:
                    m[k] = np.nan
                    full_row[k] = v
            m = pd.concat([m, pd.DataFrame([full_row])], ignore_index=True)
            added += 1
        else:
            idx = m.index[mask][0]
            for k, v in row_data.items():
                if k not in m.columns:
                    m[k] = np.nan
                m.at[idx, k] = v
            updated += 1
    m = m.copy()
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce")
    m = m.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    path_used = _write_master_flexible(m, master_path)
    return {"rows_added": int(added), "rows_updated": int(updated), "path": path_used}
