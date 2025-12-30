
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

from probedge.infra.constants import (
    LOOKBACK_YEARS, MIN3, MIN2, MIN1, MIN0,
    EDGE_PP, CONF_FLOOR, REQUIRE_OT_ALIGN
)

IST = ZoneInfo("Asia/Kolkata")

def _norm(x) -> str:
    return str(x or "").strip().upper()

def apply_lookback(m: pd.DataFrame, asof: str | None):
    day = pd.to_datetime(asof, errors="coerce") if asof else pd.Timestamp(datetime.now(tz=IST).date())
    if day is pd.NaT:
        day = pd.Timestamp(datetime.now(tz=IST).date())
    day = pd.Timestamp(day).normalize()

    if "Date" in m.columns:
        m = m.copy()
        m["Date"] = pd.to_datetime(m["Date"], errors="coerce")
        m = m[m["Date"].dt.dayofweek < 5].copy()  # Mon-Fri only
        start = day - pd.DateOffset(years=int(LOOKBACK_YEARS))
        m = m[(m["Date"] < day) & (m["Date"] >= start)].copy()

    # normalize key cols (batch-style)
    for col in ("OpeningTrend","OpenLocation","PrevDayContext","Result","FirstCandleType","RangeStatus"):
        if col in m.columns:
            m[col] = m[col].astype(str).str.strip().str.upper().replace({"NAN": ""})

    return m, day

def _decide(df: pd.DataFrame):
    if df is None or df.empty or "Result" not in df.columns:
        return "ABSTAIN", 0, 0, 0, 0, np.nan

    lab = df["Result"].astype(str).str.strip().str.upper()
    lab = lab[lab.isin(["BULL","BEAR"])]
    b = int((lab == "BULL").sum())
    r = int((lab == "BEAR").sum())
    n = b + r
    if n == 0:
        return "ABSTAIN", 0, b, r, n, np.nan

    bull_pct = 100.0 * b / n
    bear_pct = 100.0 * r / n
    gap = abs(bull_pct - bear_pct)
    pick = "BULL" if b > r else ("BEAR" if r > b else "ABSTAIN")
    conf = int(round(100.0 * max(b, r) / n))
    return pick, conf, b, r, n, gap

def select_hist_batch_parity(m: pd.DataFrame, ot: str, ol: str, pdc: str):
    otN, olN, pdcN = _norm(ot), _norm(ol), _norm(pdc)

    # manual terminal requires full selection (matches freq3 UX)
    if not otN or not olN or not pdcN:
        empty = m.iloc[0:0].copy()
        return empty, {
            "level": None, "bull_n": 0, "bear_n": 0, "total": 0,
            "gap_pp": 0.0, "pick": "ABSTAIN", "conf_pct": 0,
            "reason": "select PDC · OL · OT"
        }

    base = m.copy()

    def _match(df: pd.DataFrame, use_ol: bool, use_pdc: bool) -> pd.DataFrame:
        x = df
        if "OpeningTrend" in x.columns:
            x = x[x["OpeningTrend"] == otN]
        if use_ol and olN and "OpenLocation" in x.columns:
            x = x[x["OpenLocation"] == olN]
        if use_pdc and pdcN and "PrevDayContext" in x.columns:
            x = x[x["PrevDayContext"] == pdcN]
        return x

    # L3 -> L2 -> L1 -> L0 selection (len-based, exactly like batch)
    level = "L3"
    hist = _match(base, True, True)

    if len(hist) < MIN3:
        level, hist = "L2", _match(base, True, False)

    if len(hist) < (MIN2 if level == "L2" else MIN3):
        level, hist = "L1", _match(base, False, False)

    if len(hist) < (MIN1 if level == "L1" else (MIN2 if level == "L2" else MIN3)):
        level, hist = "L0", base

    pick, conf, b, r, n, gap = _decide(hist)

    def try_level(df: pd.DataFrame, lvl: str):
        p,c,B,R,N,G = _decide(df)
        return p,c,B,R,N,G,lvl,df

    # Broadening only when gap < EDGE_PP (exact batch behavior)
    if np.isfinite(gap) and gap < EDGE_PP:
        if level == "L3":
            p2,c2,B2,R2,N2,G2,lv2,h2 = try_level(_match(base, True, False), "L2")
            if N2 >= MIN2 and np.isfinite(G2) and G2 >= EDGE_PP:
                pick,conf,b,r,n,gap,level,hist = p2,c2,B2,R2,N2,G2,lv2,h2

        if level in ("L3","L2") and np.isfinite(gap) and gap < EDGE_PP:
            p1,c1,B1,R1,N1,G1,lv1,h1 = try_level(_match(base, False, False), "L1")
            if N1 >= MIN1 and np.isfinite(G1) and G1 >= EDGE_PP:
                pick,conf,b,r,n,gap,level,hist = p1,c1,B1,R1,N1,G1,lv1,h1

        if level in ("L3","L2","L1") and np.isfinite(gap) and gap < EDGE_PP:
            p0,c0,B0,R0,N0,G0,lv0,h0 = try_level(base, "L0")
            if N0 >= MIN0 and np.isfinite(G0) and G0 >= EDGE_PP:
                pick,conf,b,r,n,gap,level,hist = p0,c0,B0,R0,N0,G0,lv0,h0

    req = {"L3": MIN3, "L2": MIN2, "L1": MIN1, "L0": MIN0}[level]
    display_pick = pick if (n >= req and np.isfinite(gap) and gap >= EDGE_PP and conf >= CONF_FLOOR) else "ABSTAIN"

    # OT alignment (batch)
    if REQUIRE_OT_ALIGN and display_pick != "ABSTAIN" and otN in ("BULL","BEAR") and display_pick != otN:
        display_pick = "ABSTAIN"

    reason = (
        f"{level} freq: OT={otN or '-'}, OL={olN or '-'}, PDC={pdcN or '-'} | "
        f"BULL={b}, BEAR={r}, N={n}, gap={(0.0 if (not np.isfinite(gap)) else gap):.1f}pp, conf={conf}%"
        f"{' | OT-align' if REQUIRE_OT_ALIGN else ''}"
    )

        # counts over ALL outcomes for this selected level (BULL/BEAR/TR)
    tr_n = 0
    if hist is not None and (not hist.empty) and "Result" in hist.columns:
        lab_all = hist["Result"].astype(str).str.strip().str.upper()
        tr_n = int((lab_all == "TR").sum())
    counts = {"BULL": int(b), "BEAR": int(r), "TR": int(tr_n), "TOTAL": int(b + r + tr_n)}

# return history rows ONLY where Result in {BULL,BEAR} (batch counts)
    if "Result" in hist.columns:
        lab = hist["Result"].astype(str).str.strip().str.upper()
        hist_bb = hist[lab.isin(["BULL","BEAR"])].copy()
    else:
        hist_bb = hist.iloc[0:0].copy()

    meta = {
        "level": level, "bull_n": int(b), "bear_n": int(r), "total": int(n),
        "gap_pp": float(gap) if np.isfinite(gap) else 0.0,
        "pick": display_pick, "conf_pct": int(conf), "reason": reason,
        "counts": counts,
        "tr_n": int(tr_n),
        "total_all": int(counts["TOTAL"])
    }
    return hist_bb, meta
