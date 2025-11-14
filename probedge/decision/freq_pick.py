import pandas as pd
import numpy as np
from probedge.infra.constants import LOOKBACK_YEARS, EDGE_PP, CONF_FLOOR, MIN3, MIN2, MIN1, MIN0, REQUIRE_OT_ALIGN

def _norm(s): return str(s).strip().upper()

def freq_pick(day, master: pd.DataFrame):
    """Return (display_pick, conf_pct, reason, level, stats_dict)."""
    if master is None or master.empty:
        return "ABSTAIN", 0, "no master", "L0", {}

    day = pd.to_datetime(day).normalize()
    mrow = master.loc[pd.to_datetime(master["Date"]).dt.normalize() == day]
    if mrow.empty:
        return "ABSTAIN", 0, "missing master row", "L0", {}

    def g(col):
        try: return _norm(mrow[col].iloc[0])
        except Exception: return ""

    otoday = g("OpeningTrend"); ol_today = g("OpenLocation"); pdc_today = g("PrevDayContext")

    base = master[
        (pd.to_datetime(master["Date"]).dt.normalize() < day) &
        (pd.to_datetime(master["Date"]).dt.normalize() >= (day - pd.DateOffset(years=LOOKBACK_YEARS)))
    ].copy()

    def _match(df, use_ol, use_pdc):
        m = df[df["OpeningTrend"].astype(str).str.upper().str.strip() == otoday]
        if use_ol and ol_today:
            m = m[m["OpenLocation"].astype(str).str.upper().str.strip() == ol_today]
        if use_pdc and pdc_today:
            m = m[m["PrevDayContext"].astype(str).str.upper().str.strip() == pdc_today]
        return m

    def decide(df):
        lab = df.get("Result", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
        lab = lab[(lab == "BULL") | (lab == "BEAR")]
        b = int((lab == "BULL").sum()); r = int((lab == "BEAR").sum()); n = b + r
        if n == 0: return "ABSTAIN", 0, b, r, n, np.nan
        bull_pct = 100.0 * b / n; bear_pct = 100.0 * r / n; gap = abs(bull_pct - bear_pct)
        pick = "BULL" if b > r else ("BEAR" if r > b else "ABSTAIN")
        conf = int(round(100.0 * max(b, r) / n))
        return pick, conf, b, r, n, gap

    level = "L3"; hist = _match(base, True, True)
    if len(hist) < MIN3: level, hist = "L2", _match(base, True, False)
    if len(hist) < (MIN2 if level=="L2" else MIN3): level, hist = "L1", _match(base, False, False)
    if len(hist) < (MIN1 if level=="L1" else (MIN2 if level=="L2" else MIN3)): level, hist = "L0", base

    pick, conf, b, r, n, gap = decide(hist)

    # broaden if edge weak
    def try_level(df, lvl):
        p,c,B,R,N,G = decide(df)
        return (p,c,B,R,N,G,lvl,df)

    if not np.isnan(gap) and gap < EDGE_PP:
        if level == "L3":
            p2,c2,B2,R2,N2,G2,lv2,h2 = try_level(_match(base, True, False), "L2")
            if N2 >= MIN2 and (not np.isnan(G2) and G2 >= EDGE_PP): pick,conf,b,r,n,gap,level,hist = p2,c2,B2,R2,N2,G2,lv2,h2
        if (level in ("L3","L2")) and (gap < EDGE_PP):
            p1,c1,B1,R1,N1,G1,lv1,h1 = try_level(_match(base, False, False), "L1")
            if N1 >= MIN1 and (not np.isnan(G1) and G1 >= EDGE_PP): pick,conf,b,r,n,gap,level,hist = p1,c1,B1,R1,N1,G1,lv1,h1
        if (level in ("L3","L2","L1")) and (gap < EDGE_PP):
            p0,c0,B0,R0,N0,G0,lv0,h0 = try_level(base, "L0")
            if N0 >= MIN0 and (not np.isnan(G0) and G0 >= EDGE_PP): pick,conf,b,r,n,gap,level,hist = p0,c0,B0,R0,N0,G0,lv0,h0

    req = {"L3":MIN3,"L2":MIN2,"L1":MIN1,"L0":MIN0}[level]
    display_pick = pick if (n >= req and (not np.isnan(gap) and gap >= EDGE_PP) and conf >= CONF_FLOOR) else "ABSTAIN"
    if REQUIRE_OT_ALIGN and display_pick != "ABSTAIN" and otoday in ("BULL","BEAR") and display_pick != otoday:
        display_pick = "ABSTAIN"

    reason = (f"{level} freq: OT={otoday or '-'}, OL={ol_today or '-'}, PDC={pdc_today or '-'} | "
              f"BULL={b}, BEAR={r}, N={n}, gap={(gap if not np.isnan(gap) else 0):.1f}pp, conf={conf}% "
              f"{'| OT-align' if REQUIRE_OT_ALIGN else ''}")

    stats = {"level": level, "B": b, "R": r, "N": n, "gap_pp": None if np.isnan(gap) else float(gap), "conf%": conf}
    return display_pick, int(conf), reason, level, stats
