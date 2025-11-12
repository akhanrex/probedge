from __future__ import annotations
from fastapi import APIRouter, HTTPException, Query
import numpy as np
import pandas as pd
from typing import Tuple, Optional

from apps.storage.tm5 import read_tm5, read_master
from apps.utils.dates import to_minutes, in_range, SESSION_START, ORB_END

router = APIRouter(prefix="/api", tags=["plan"])

EDGE_PP = 5.0          # edge threshold in percentage points
CONF_FLOOR = 55.0      # minimum confidence
REQUIRE_OT_ALIGN = True
CLOSE_PCT = 0.0025     # 'close to' threshold as % of entry
CLOSE_FR_ORB = 0.25    # or 25% of ORB range

def _orb(df: pd.DataFrame) -> Tuple[float,float]:
    mins = to_minutes(df["DateTime"])
    mask = in_range(mins, SESSION_START, ORB_END)
    w = df.loc[mask]
    if w.empty:
        return np.nan, np.nan
    return float(w["High"].max()), float(w["Low"].min())

def _open_location(open_px: float, prev_hi: float, prev_lo: float) -> str:
    if np.isnan(prev_hi) or np.isnan(prev_lo) or np.isnan(open_px):
        return "NA"
    if open_px > prev_hi: return "OOH"  # open outside above range
    if open_px < prev_lo: return "OOL"  # open outside below range
    # inside range
    mid = (prev_hi + prev_lo) / 2.0
    return "OAR" if abs(open_px - mid) <= (prev_hi - prev_lo) * 0.15 else "OIM"

def _opening_trend(df: pd.DataFrame) -> str:
    # Compare 09:40 close vs 09:15 open; fallback to first/last of first 5 bars
    mins = to_minutes(df["DateTime"])
    mask = in_range(mins, SESSION_START, ORB_END)
    w = df.loc[mask]
    if len(w) < 2:
        return "TR"
    first_open = float(w.iloc[0]["Open"])
    last_close = float(w.iloc[-1]["Close"])
    if last_close > first_open: return "BULL"
    if last_close < first_open: return "BEAR"
    return "TR"

def _prev_day_context(prev_day_df: pd.DataFrame) -> str:
    if prev_day_df.empty: return "TR"
    o = float(prev_day_df.iloc[0]["Open"])
    c = float(prev_day_df.iloc[-1]["Close"])
    if c > o: return "BULL"
    if c < o: return "BEAR"
    return "TR"

def _is_close(a: float, b: float, entry_px: float, orb_rng: float) -> bool:
    thr = np.inf
    parts = []
    if np.isfinite(entry_px) and entry_px > 0:
        parts.append(entry_px * CLOSE_PCT)
    if np.isfinite(orb_rng):
        parts.append(abs(orb_rng) * CLOSE_FR_ORB)
    if parts:
        thr = min(parts)
    return np.isfinite(a) and np.isfinite(b) and abs(a - b) <= thr

@router.get("/plan")
def get_plan(symbol: str = Query(..., min_length=1)):
    sym = symbol.strip().upper()
    try:
        tm5 = read_tm5(sym)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    if tm5.empty:
        raise HTTPException(409, "tm5 empty")

    last_date = tm5["Date"].max()
    prev_date = pd.to_datetime(last_date) - pd.Timedelta(days=1)

    day_df = tm5[tm5["Date"] == last_date]
    prev_df = tm5[tm5["Date"] == prev_date.date()]

    # Compute today's quick tags from tm5
    prev_hi = float(prev_df["High"].max()) if not prev_df.empty else np.nan
    prev_lo = float(prev_df["Low"].min()) if not prev_df.empty else np.nan
    open_px = float(day_df.iloc[0]["Open"]) if not day_df.empty else np.nan

    otoday = _opening_trend(day_df)
    ol_today = _open_location(open_px, prev_hi, prev_lo)
    pdc_today = _prev_day_context(prev_df)

    # Use master for historical matching stats
    try:
        master = read_master(sym)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    cols = {c.lower(): c for c in master.columns}
    def col(name: str) -> Optional[str]:
        for c in [name, name.upper(), name.lower()]:
            if c in master.columns: return c
        for k in cols:
            if k.startswith(name.lower()):
                return cols[k]
        return None

    c_ot = col("OpeningTrend")
    c_ol = col("OpenLocation")
    c_pdc = col("PrevDayContext")
    m = master.copy()
    if c_ot: m = m[m[c_ot].astype(str).str.upper() == otoday]
    if c_ol: m = m[m[c_ol].astype(str).str.upper() == ol_today]
    if c_pdc: m = m[m[c_pdc].astype(str).str.upper() == pdc_today]

    # Count bull/bear outcomes if 'Result' (or similar) exists
    res_col = None
    for c in ["Result","Direction","Side","Pick"]:
        if c in m.columns: res_col = c; break
    b = r = 0
    if res_col:
        b = int((m[res_col].astype(str).str.upper() == "BULL").sum())
        r = int((m[res_col].astype(str).str.upper() == "BEAR").sum())
    n = int(len(m))
    gap = (b - r) / max(1, n) * 100.0
    conf = 100.0 * max(b, r) / max(1, n)

    # Choose display pick
    pick = "ABSTAIN"
    level = "core"
    if n >= 20 and np.isfinite(gap) and gap >= EDGE_PP and conf >= CONF_FLOOR:
        pick = "BULL" if b > r else "BEAR"
    if REQUIRE_OT_ALIGN and pick != "ABSTAIN" and otoday in ("BULL","BEAR") and pick != otoday:
        pick = "ABSTAIN"

    reason = (
        f"{level} freq: OT={otoday}, OL={ol_today}, PDC={pdc_today} | "
        f"BULL={b}, BEAR={r}, N={n}, gap={gap:.2f}pp, conf={conf:.1f}% "
        f"{'| OT-align' if REQUIRE_OT_ALIGN else ''}"
    )

    # ORB range for helper signals (for UI to show proximity)
    h,l = _orb(day_df) if not day_df.empty else (np.nan, np.nan)
    orb_rng = h - l if (np.isfinite(h) and np.isfinite(l)) else np.nan
    last_close = float(day_df.iloc[-1]["Close"]) if not day_df.empty else np.nan
    near_high = _is_close(last_close, h, last_close, orb_rng) if np.isfinite(last_close) else False
    near_low  = _is_close(last_close, l, last_close, orb_rng) if np.isfinite(last_close) else False

    return {
        "symbol": sym,
        "date": str(last_date),
        "ot": otoday, "ol": ol_today, "pdc": pdc_today,
        "display_pick": pick,
        "confidence": conf,
        "reason": reason,
        "hist_counts": {"bull": b, "bear": r, "n": n, "edge_pp": gap},
        "orb": {"high": h, "low": l, "range": orb_rng, "near_high": near_high, "near_low": near_low},
    }
