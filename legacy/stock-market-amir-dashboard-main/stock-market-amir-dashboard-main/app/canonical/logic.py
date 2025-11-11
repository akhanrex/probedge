# app/canonical/logic.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import date, datetime, time as dtime, timedelta
import os
import numpy as np
import pandas as pd

# ---------- Normalizers ----------
def _norm_intraday_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize intraday CSV to one 'datetime' + OHLCV, without ever using
    pandas' dict-of-units (year/month/day) path that triggers 'duplicate keys'."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

    g = df.copy()

    # 1) Case-insensitive de-dup of columns (keep first)
    raw = [str(c).strip() for c in g.columns]
    lower = [c.lower() for c in raw]
    keep_mask, seen = [], set()
    for lc in lower:
        if lc in seen:
            keep_mask.append(False)
        else:
            keep_mask.append(True)
            seen.add(lc)
    g = g.loc[:, keep_mask].copy()
    g.columns = [c.lower().strip() for c in g.columns]

    def _pick(*names):
        for n in names:
            if n in g.columns:
                obj = g[n]
                # If duplicated label yields a DataFrame for some reason, take first col
                if isinstance(obj, pd.DataFrame):
                    obj = obj.iloc[:, 0]
                return obj
        return None

    # 2) Build a single datetime column (string-based only)
    s_date = _pick("datetime", "timestamp", "date")
    s_time = _pick("time", "t")

    if s_date is not None and s_time is not None and s_date.name == "date":
        # explicit join to avoid dict assembly
        dt_series = pd.to_datetime(
            s_date.astype(str).str.strip() + " " + s_time.astype(str).str.strip(),
            errors="coerce"
        )
    elif s_date is not None:
        dt_series = pd.to_datetime(s_date.astype(str).str.strip(), errors="coerce")
    elif s_time is not None:
        dt_series = pd.to_datetime(s_time.astype(str).str.strip(), errors="coerce")
    else:
        # last resort: try any single column that looks date-like by name
        like = [c for c in g.columns if any(k in c for k in ("date", "time", "stamp"))]
        if like:
            dt_series = pd.to_datetime(g[like[0]].astype(str).str.strip(), errors="coerce")
        else:
            # refuse to assemble from units; fail loudly with columns listed
            raise ValueError(f"No recognizable datetime columns in intraday CSV. Columns: {list(g.columns)}")

    g["datetime"] = dt_series

    # 3) Map OHLCV (broad synonyms, case-insensitive)
    rename = {}
    synonyms = {
        "open":   ("open", "o", "openprice", "open_price"),
        "high":   ("high", "h", "highprice", "high_price"),
        "low":    ("low", "l", "lowprice", "low_price"),
        "close":  ("close", "c", "closeprice", "close_price", "last", "lastprice"),
        "volume": ("volume", "vol", "v", "totaltradedvolume", "qty", "volume_traded"),
    }
    for want, cands in synonyms.items():
        if want in g.columns:
            continue
        for c in cands:
            if c in g.columns:
                rename[c] = want
                break
    if rename:
        g = g.rename(columns=rename)

    # 4) Ensure required cols, drop bad rows, sort
    for k in ["open", "high", "low", "close", "volume"]:
        if k not in g.columns:
            g[k] = pd.NA

    g = g.dropna(subset=["datetime", "open", "high", "low", "close"], how="any")
    g = g.sort_values("datetime").reset_index(drop=True)
    return g[["datetime", "open", "high", "low", "close", "volume"]]
    
def _slice_date(df: pd.DataFrame, d: date) -> pd.DataFrame:
    if df is None or df.empty or "datetime" not in df.columns:
        return pd.DataFrame()
    m = df["datetime"].dt.date == d
    return df.loc[m].copy().sort_values("datetime").reset_index(drop=True)

# ---------- Reused logic (trimmed & file-friendly) ----------
def _prevday_context_from_prev_result(prev_row):
    if prev_row is None:
        return None

    # normalize prev_row keys once
    pr = {str(k).upper(): v for k, v in prev_row.items()}

    # prefer explicit prev-day-context if present
    for k in ("PREVDAYCONTEXT","PREV_DAY_CONTEXT","PREVCONTEXT","PREVBIAS","PREV DAY CONTEXT"):
        v = pr.get(k)
        if v is not None and pd.notna(v):
            vv = str(v).strip().upper()
            if vv in ("BULL","BEAR","TR","RANGE","TRADING RANGE"):
                return "TR" if "TR" in vv or "RANGE" in vv else vv

    # fallback: derive from previous day's Result
    r = str(prev_row.get("Result","")).upper().replace(" ","")
    if "BULL" in r and "BEAR" not in r: return "BULL"
    if "BEAR" in r and "BULL" not in r: return "BEAR"
    return "TR"

def _is_pure_doji_weekly(o,c,h,l, body_pct=0.5, center_pct=0.2) -> bool:
    if any(pd.isna([o,c,h,l])): return False
    rng = h - l
    if rng == 0: return False
    body = abs(c - o)
    if body > body_pct * rng: return False
    body_center = (o + c) / 2
    range_center = (h + l) / 2
    if abs(body_center - range_center) > center_pct * rng: return False
    if (h - max(o,c)) < 0.05*rng or (min(o,c) - l) < 0.05*rng: return False
    return True

def open_location_weekly(today_open: Optional[float], pd_high: Optional[float], pd_low: Optional[float], band: float = 0.30) -> Optional[str]:
    if any(x is None for x in [today_open, pd_high, pd_low]): return None
    o, H, L = float(today_open), float(pd_high), float(pd_low)
    if H <= L: return "OIM"
    rng = H - L
    if o < L: return "OBR"
    if o <= L + band*rng: return "OOL"
    if o > H: return "OAR"
    if o >= H - band*rng: return "OOH"
    return "OIM"

def first_candle_type_weekly(bars: pd.DataFrame, pd_high: float, pd_low: float) -> str:
    if bars is None or len(bars) < 1 or any(pd.isna([pd_high, pd_low])): return ""
    b1 = bars.iloc[0]
    o,h,l,c = float(b1["open"]),float(b1["high"]),float(b1["low"]),float(b1["close"])
    if _is_pure_doji_weekly(o,c,h,l): return "DOJI"
    prev_rng = float(pd_high - pd_low)
    if prev_rng <= 0: return ""
    if (h - l) > 0.7*prev_rng: return "HUGE OPEN"
    upto = min(5, len(bars))
    for i in range(1,upto):
        hi, lo = float(bars.iloc[i]["high"]), float(bars.iloc[i]["low"])
        if any(abs(x - y) > 0.9*prev_rng for x in [h,l] for y in [hi,lo]):
            return "HUGE OPEN"
    return "NORMAL"

def opening_trend_weekly(bars: pd.DataFrame, pd_high: float, pd_low: float, first_open: float) -> str:
    if bars is None or len(bars) < 5: return ""
    highs = [float(x) for x in bars["high"].iloc[:5]]
    lows  = [float(x) for x in bars["low"].iloc[:5]]
    opens = [float(x) for x in bars["open"].iloc[:5]]
    closes= [float(x) for x in bars["close"].iloc[:5]]
    fh, fl, fo, fc = highs[0], lows[0], opens[0], closes[0]
    prev_range = float(pd_high - pd_low)
    if prev_range <= 0: return ""
    first_range = float(fh - fl)
    if first_range >= 0.5 * prev_range:
        if fc > fo: return "BULL"
        if fc < fo: return "BEAR"
    for i in range(1,5):
        if fh - lows[i] > 0.4*prev_range: return "BEAR"
        if highs[i] - fl > 0.4*prev_range: return "BULL"
    up = down = 0
    for i in range(1,5):
        if closes[i] > closes[i-1]: up += 1; down = 0
        elif closes[i] < closes[i-1]: down += 1; up = 0
        else: up = down = 0
        if up >= 3: return "BULL"
        if down >= 3: return "BEAR"
    spread = max(highs) - min(lows)
    if first_range and spread < 0.4*first_range: return "TR"
    up_count = sum(closes[i] > closes[i-1] for i in range(1,5))
    dn_count = sum(closes[i] < closes[i-1] for i in range(1,5))
    if up_count > dn_count: return "BULL"
    if dn_count > up_count: return "BEAR"
    return "TR"

def _val_at_or_before(bars: pd.DataFrame, hh: int, mm: int, col: str):
    if bars is None or bars.empty: return None
    m = bars[bars["datetime"].dt.time <= dtime(hh, mm)]
    return float(m.iloc[-1][col]) if len(m) else None

def new_range_status_weekly(bars: pd.DataFrame, pd_high: float, pd_low: float, open_location: str) -> str:
    if bars is None or len(bars) < 5 or any(pd.isna([pd_high, pd_low])): return ""
    upto = min(5, len(bars))
    in_range = above = below = False
    for i in range(upto):
        o = float(bars.iloc[i]["open"]); c = float(bars.iloc[i]["close"])
        if (pd_low <= o <= pd_high) or (pd_low <= c <= pd_high): in_range = True
        if (o > pd_high) or (c > pd_high): above = True
        if (o < pd_low)  or (c < pd_low):  below = True
    ol = (open_location or "").upper()
    if ol == "OBR":
        if not in_range and below: return "SBR"
        if in_range and above:     return "WAR"
        if in_range and not above: return "SWR"
        if above and not in_range: return "WAR"
    elif ol == "OAR":
        if not in_range and above: return "SAR"
        if in_range and below:     return "WBR"
        if in_range and not below: return "SWR"
        if below and not in_range: return "WBR"
    else:
        if above and not below: return "WAR"
        if below and not above: return "WBR"
        if in_range and not above and not below: return "SWR"
    return ""

# ---------- Canonical compute ----------
@dataclass
class CanonicalRow:
    Date: date
    PrevDayContext_NEW: str
    OpenLocation_NEW: str
    FirstCandleType_NEW: str
    OpeningTrend_NEW: str
    RangeStatus_NEW: str
    DayHigh_NEW: Optional[float]
    DayLow_NEW: Optional[float]

def _prev_row_before(master: pd.DataFrame, d: date) -> Optional[pd.Series]:
    if master is None or master.empty or "Date" not in master.columns:
        return None
    m = master.copy()
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.date
    m = m[m["Date"].notna()]
    prev = m[m["Date"] < d]
    if prev.empty: return None
    return prev.sort_values("Date").iloc[-1]

def _pdh_pdl_from_prev(master: pd.DataFrame, intraday: pd.DataFrame, d: date) -> Tuple[Optional[float], Optional[float]]:
    prev = _prev_row_before(master, d)
    # Prefer master DayHigh/DayLow if available
    if prev is not None:
        for H_key in ("DayHigh","DAYHIGH","High","HIGH"):
            if H_key in prev.index and pd.notna(prev[H_key]):
                pdh = float(prev[H_key]); break
        else: pdh = None
        for L_key in ("DayLow","DAYLOW","Low","LOW"):
            if L_key in prev.index and pd.notna(prev[L_key]):
                pdl = float(prev[L_key]); break
        else: pdl = None
        if pdh is not None and pdl is not None and pdh > pdl:
            return pdh, pdl
    # Fallback: previous trading day's intraday
    prev_d = d - timedelta(days=1)
    while prev_d.weekday() >= 5:  # skip weekend
        prev_d -= timedelta(days=1)
    prev_bars = _slice_date(intraday, prev_d)
    if prev_bars.empty: return None, None
    return float(prev_bars["high"].max()), float(prev_bars["low"].min())

def compute_canonical_for_day(master: pd.DataFrame, intraday: pd.DataFrame, d: date) -> Optional[CanonicalRow]:
    bars = _slice_date(intraday, d)
    if bars is None or bars.empty:
        return None
    bars = _norm_intraday_cols(bars)
    pdh, pdl = _pdh_pdl_from_prev(master, intraday, d)
    if not (isinstance(pdh, float) and isinstance(pdl, float) and pdh > pdl):
        return None
    today_open = float(bars.iloc[0]["open"])
    prev_ctx = _prevday_context_from_prev_result(_prev_row_before(master, d)) or "TR"
    ol = open_location_weekly(today_open, pdh, pdl) or ""
    fct = first_candle_type_weekly(bars, pdh, pdl) or ""
    otr = opening_trend_weekly(bars, pdh, pdl, today_open) or ""
    rgs = new_range_status_weekly(bars, pdh, pdl, ol or "") or ""
    return CanonicalRow(
        Date=d,
        PrevDayContext_NEW=prev_ctx,
        OpenLocation_NEW=ol,
        FirstCandleType_NEW=fct,
        OpeningTrend_NEW=otr,
        RangeStatus_NEW=rgs,
        DayHigh_NEW=float(bars["high"].max()),
        DayLow_NEW=float(bars["low"].min()),
    )

def batch_compute(master: pd.DataFrame, intraday: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    intraday = _norm_intraday_cols(intraday)
    days = pd.date_range(start=start_d, end=end_d, freq="D")
    out: List[CanonicalRow] = []
    for d in days:
        if d.weekday() >= 5:  # skip weekends
            continue
        row = compute_canonical_for_day(master, intraday, d.date())
        if row is not None:
            out.append(row)
    if not out: return pd.DataFrame()
    df = pd.DataFrame([r.__dict__ for r in out])
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    return df

# ---------- Evaluation & writing ----------
def evaluate_against_master(master: pd.DataFrame, canon: pd.DataFrame, cols: List[str]) -> Dict[str, any]:
    if master is None or master.empty or canon is None or canon.empty:
        return {"matches": {}, "merged": pd.DataFrame()}
    m = master.copy()
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
    merged = pd.merge(m, canon, on="Date", how="inner", suffixes=("", "_NEW"))
    stats = {}
    for c in cols:
        old = merged[c].astype(str).str.strip().str.upper().fillna("")
        new = merged[f"{c}_NEW"].astype(str).str.strip().str.upper().fillna("")
        stats[c] = float((old == new).mean()) if len(merged) else 0.0
    return {"matches": stats, "merged": merged}

def write_side_by_side(master: pd.DataFrame, canon: pd.DataFrame, out_path: str, cols: List[str]) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    m = master.copy()
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
    c = canon.copy()
    c["Date"] = pd.to_datetime(c["Date"], errors="coerce").dt.normalize()
    merged = pd.merge(m, c, on="Date", how="outer")
    # ensure *_NEW exist
    for col in cols:
        if f"{col}_NEW" not in merged.columns:
            merged[f"{col}_NEW"] = np.nan
    # write
    out = merged.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out.to_csv(out_path, index=False)
    return out_path
