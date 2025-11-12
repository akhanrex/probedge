# apps/api/routes/plan.py
from fastapi import APIRouter, HTTPException, Query
from probedge.infra.settings import SETTINGS
import pandas as pd, numpy as np, math, os
from datetime import time as dtime

router = APIRouter()

# ==== batch constants (unchanged) ====
T0 = dtime(9, 40); T1 = dtime(15, 5)
SESSION_START = dtime(9, 15); ORB_END = dtime(9, 35)
T0_M = 9*60 + 40; T1_M = 15*60 + 5; S_M = 9*60 + 15; E_M = 9*60 + 35

EDGE_PP = 8.0
CONF_FLOOR = 55
MIN3, MIN2, MIN1, MIN0 = 8, 6, 4, 3
REQUIRE_OT_ALIGN = True
CLOSE_PCT = 0.0025         # 0.25% of entry
CLOSE_FR_ORB = 0.20        # 20% of ORB range
DEFAULT_RISK_RS = 10000.0  # can override via env RISK_RS

# ==== robust 5-min reader (same mapping as batch) ====
def _read_tm5(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).replace("\ufeff","").strip() for c in df.columns]
    # find DateTime
    lc2orig = {c.lower(): c for c in df.columns}
    dt = None
    for key in ("datetime","date_time","timestamp","date"):
        if key in lc2orig:
            dt = pd.to_datetime(df[lc2orig[key]], errors="coerce"); break
    if dt is None and ("date" in lc2orig and "time" in lc2orig):
        dt = pd.to_datetime(df[lc2orig["date"]].astype(str)+" "+df[lc2orig["time"]].astype(str), errors="coerce")
    if dt is None:
        raise ValueError(f"No DateTime in {path}")
    if "DateTime" in df.columns: df["DateTime"] = dt
    else: df.insert(0, "DateTime", dt)

    # map OHLCV
    def pick(*aliases):
        for a in aliases:
            if a in lc2orig: return lc2orig[a]
        for c in df.columns:
            if c.lower() in aliases: return c
        return None
    m = {
        "Open": pick("open","o"), "High": pick("high","h"),
        "Low": pick("low","l"),   "Close": pick("close","c"),
        "Volume": pick("volume","vol","qty","quantity")
    }
    for k,v in m.items():
        if v and v != k: df.rename(columns={v:k}, inplace=True)
    for k in ("Open","High","Low","Close","Volume"):
        if k in df.columns: df[k] = pd.to_numeric(df[k], errors="coerce")

    df = (df.dropna(subset=["DateTime","Open","High","Low","Close"])
            .sort_values("DateTime").reset_index(drop=True))
    df["Date"] = df["DateTime"].dt.normalize()
    df["_mins"] = df["DateTime"].dt.hour*60 + df["DateTime"].dt.minute
    return df

def _slice(df_day, m0, m1):
    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    return df_day.loc[m, ["DateTime","Open","High","Low","Close","Date"]]

# ==== core tags (same as batch) ====
def _overlap_score(df):
    if df is None or len(df) < 2: return 0.0
    hi, lo = df["High"].to_numpy(float), df["Low"].to_numpy(float)
    ov = []
    for i in range(1, len(df)):
        num = max(0.0, min(hi[i], hi[i-1]) - max(lo[i], lo[i-1]))
        den = max(1e-9, (max(hi[i], hi[i-1]) - min(lo[i], lo[i-1])))
        ov.append(num/den)
    return float(np.mean(ov)) if ov else 0.0

def _dir_count(df): return int((df["Close"]>df["Open"]).sum() - (df["Close"]<df["Open"]).sum())

TH_MOVE=0.35; TH_RANGE=0.80; TH_TINY_MOVE=0.30; TH_POS_TOP=0.60; TH_POS_BOTTOM=0.40; TH_DIR=2; TH_OVERLAP=0.50
def compute_openingtrend_robust(df_day):
    win = _slice(df_day, 9*60+15, 9*60+40)
    if win.empty: return "TR"
    win = win.sort_values("DateTime")
    O0=float(win["Open"].iloc[0]); Cn=float(win["Close"].iloc[-1])
    Hmax=float(win["High"].max()); Lmin=float(win["Low"].min())
    move_pct = 100.0*(Cn-O0)/max(1e-9,O0)
    range_pct= 100.0*(Hmax-Lmin)/max(1e-9,O0)
    pos=0.5 if Hmax<=Lmin else (Cn-Lmin)/(Hmax-Lmin)
    dcount=_dir_count(win); ovl=_overlap_score(win)
    if (range_pct<TH_RANGE) and (abs(move_pct)<TH_TINY_MOVE) and (ovl>TH_OVERLAP): return "TR"
    v_dist= 1 if move_pct>=+TH_MOVE else (-1 if move_pct<=-TH_MOVE else 0)
    v_pos = 1 if pos>=TH_POS_TOP else (-1 if pos<=TH_POS_BOTTOM else 0)
    v_pers= 1 if dcount>=TH_DIR else (-1 if dcount<=-TH_DIR else 0)
    S=v_dist+v_pos+v_pers
    return "BULL" if S>=+2 else ("BEAR" if S<=-2 else "TR")

TH_NARROW=1.00; TH_BODY_STRONG=0.45; TH_BODY_WEAK=0.25; TH_CLV_BULL=0.65; TH_CLV_BEAR=0.35
def compute_prevdaycontext_robust(prev_O, prev_H, prev_L, prev_C):
    H,L,O,C=float(prev_H),float(prev_L),float(prev_O),float(prev_C)
    rng=max(1e-9,H-L); range_pct=100.0*rng/max(1e-9,C)
    body_frac=abs(C-O)/rng if rng>0 else 0.0; clv=(C-L)/rng if rng>0 else 0.5
    if (range_pct<=TH_NARROW) or (body_frac<=TH_BODY_WEAK): return "TR"
    if (clv>=TH_CLV_BULL) and (body_frac>=TH_BODY_STRONG): return "BULL"
    if (clv<=TH_CLV_BEAR) and (body_frac>=TH_BODY_STRONG): return "BEAR"
    return "TR"

def compute_openlocation(day_open, prev_h, prev_l):
    if not np.isfinite(day_open) or not np.isfinite(prev_h) or not np.isfinite(prev_l) or prev_h<=prev_l: return ""
    rng=prev_h-prev_l; o=float(day_open)
    if o < prev_l: return "OBR"
    if o <= prev_l + 0.3*rng: return "OOL"
    if o > prev_h: return "OAR"
    if o >= prev_h - 0.3*rng: return "OOH"
    return "OIM"

# ==== pick logic (same as batch) ====
def _freq_pick(day, master, lookback_years=6):
    mrow = master.loc[master["Date"] == day]
    if mrow.empty: return "ABSTAIN", 0, "missing master row", "L0", 0,0,0, np.nan
    def g(col):
        try: return str(mrow[col].iloc[0]).strip().upper()
        except: return ""
    otoday, ol_today, pdc_today = g("OpeningTrend"), g("OpenLocation"), g("PrevDayContext")
    base = master[(master["Date"] < day) & (master["Date"] >= (pd.to_datetime(day) - pd.DateOffset(years=lookback_years)))].copy()

    def _match(df, use_ol, use_pdc):
        m = df[df["OpeningTrend"] == otoday] if "OpeningTrend" in df.columns else df
        if use_ol and ("OpenLocation" in m.columns) and ol_today: m = m[m["OpenLocation"] == ol_today]
        if use_pdc and ("PrevDayContext" in m.columns) and pdc_today: m = m[m["PrevDayContext"] == pdc_today]
        return m

    def decide(df):
        lab = df.get("Result", pd.Series(dtype=str)).astype(str).str.strip().str.upper()
        lab = lab[(lab=="BULL") | (lab=="BEAR")]
        b = int((lab=="BULL").sum()); r = int((lab=="BEAR").sum()); n = b+r
        if n==0: return "ABSTAIN",0,b,r,n,np.nan
        bull_pct=100.0*b/n; bear_pct=100.0*r/n; gap=abs(bull_pct-bear_pct)
        pick = "BULL" if b>r else ("BEAR" if r>b else "ABSTAIN")
        conf = int(round(100.0 * max(b, r) / n))
        return pick, conf, b, r, n, gap

    level, hist = "L3", _match(base, True, True)
    if len(hist) < MIN3: level, hist = "L2", _match(base, True, False)
    if len(hist) < (MIN2 if level=="L2" else MIN3): level, hist = "L1", _match(base, False, False)
    if len(hist) < (MIN1 if level=="L1" else (MIN2 if level=="L2" else MIN3)): level, hist = "L0", base
    pick, conf, b, r, n, gap = decide(hist)

    # try broaden if weak edge
    def try_level(df, lvl):
        p,c,B,R,N,G = decide(df)
        return (p,c,B,R,N,G,lvl,df)
    if not np.isnan(gap) and gap < EDGE_PP:
        if level=="L3":
            p2,c2,B2,R2,N2,G2,lv2,h2 = try_level(_match(base, True, False), "L2")
            if N2>=MIN2 and (not np.isnan(G2) and G2>=EDGE_PP): pick,conf,b,r,n,gap,level,hist = p2,c2,B2,R2,N2,G2,lv2,h2
        if (level in ("L3","L2")) and (gap<EDGE_PP):
            p1,c1,B1,R1,N1,G1,lv1,h1 = try_level(_match(base, False, False), "L1")
            if N1>=MIN1 and (not np.isnan(G1) and G1>=EDGE_PP): pick,conf,b,r,n,gap,level,hist = p1,c1,B1,R1,N1,G1,lv1,h1
        if (level in ("L3","L2","L1")) and (gap<EDGE_PP):
            p0,c0,B0,R0,N0,G0,lv0,h0 = try_level(base, "L0")
            if N0>=MIN0 and (not np.isnan(G0) and G0>=EDGE_PP): pick,conf,b,r,n,gap,level,hist = p0,c0,B0,R0,N0,G0,lv0,h0

    # display gating
    req = {"L3":MIN3,"L2":MIN2,"L1":MIN1,"L0":MIN0}[level]
    display_pick = pick if (n>=req and (not np.isnan(gap) and gap>=EDGE_PP) and conf>=CONF_FLOOR) else "ABSTAIN"
    if REQUIRE_OT_ALIGN and display_pick!="ABSTAIN" and otoday in ("BULL","BEAR") and display_pick!=otoday:
        display_pick = "ABSTAIN"

    reason = (f"{level} freq: OT={otoday or '-'}, OL={ol_today or '-'}, PDC={pdc_today or '-'} | "
              f"BULL={b}, BEAR={r}, N={n}, gap={gap if np.isfinite(gap) else 'NA'}pp, conf={conf}% "
              f"{'| OT-align' if REQUIRE_OT_ALIGN else ''}")
    return display_pick, conf, reason, level, b, r, n, gap

def _is_close(a, b, entry_px, orb_rng):
    thr = np.inf; parts = []
    if np.isfinite(entry_px) and entry_px>0: parts.append(entry_px*CLOSE_PCT)
    if np.isfinite(orb_rng): parts.append(abs(orb_rng)*CLOSE_FR_ORB)
    if parts: thr = min(parts)
    return np.isfinite(a) and np.isfinite(b) and abs(a-b) <= thr

@router.get("/api/plan")
def get_plan(symbol: str = Query(..., min_length=1)):
    sym = symbol.strip().upper()
    ip = SETTINGS.paths.intraday.format(sym=sym)
    mp = SETTINGS.paths.masters.format(sym=sym)
    if not os.path.exists(ip): raise HTTPException(404, f"tm5 not found: {ip}")
    if not os.path.exists(mp): raise HTTPException(404, f"master not found: {mp}")

    tm5 = _read_tm5(ip)
    if tm5.empty: raise HTTPException(409, "tm5 empty")

    # Use the last available trading day in file (IST-naive)
    day = pd.to_datetime(tm5["Date"].max()).normalize()
    dfd = tm5[tm5["Date"].eq(day)].copy().sort_values("DateTime")
    if dfd.empty: raise HTTPException(409, "no bars for latest day")

    # Require a complete ORB and the 09:40 bar to exist
    orb = _slice(dfd, S_M, E_M)
    if len(orb) < 1: raise HTTPException(409, "ORB not ready")
    w0940_1505 = _slice(dfd, T0_M, T1_M)
    if w0940_1505.empty or w0940_1505["DateTime"].iloc[0].hour != 9 or w0940_1505["DateTime"].iloc[0].minute not in (40,):  # guard
        raise HTTPException(409, "09:40 bar not present")

    # Prev-day high/low from whole tm5
    daily = tm5.groupby("Date").agg(day_high=("High","max"), day_low=("Low","min")).reset_index().sort_values("Date")
    dp = daily.copy(); dp["prev_high"]=dp["day_high"].shift(1); dp["prev_low"]=dp["day_low"].shift(1)
    prev_row = dp[dp["Date"].eq(day)]
    prev_h = float(prev_row["prev_high"].iloc[0]) if not prev_row.empty else np.nan
    prev_l = float(prev_row["prev_low"].iloc[0])  if not prev_row.empty else np.nan

    # Tags for today (batch logic)
    ot = compute_openingtrend_robust(dfd)
    day_open = float(dfd["Open"].iloc[0]) if len(dfd) else np.nan
    pdc = ""
    # derive prev day OHLC for PDC
    try:
        prev_day = pd.to_datetime(day) - pd.Timedelta(days=1)
        prev_df = tm5[tm5["Date"].eq(prev_day)].copy().sort_values("DateTime")
        if not prev_df.empty:
            pdc = compute_prevdaycontext_robust(prev_df["Open"].iloc[0], prev_df["High"].max(),
                                                prev_df["Low"].min(), prev_df["Close"].iloc[-1])
    except: pass
    ol = compute_openlocation(day_open, prev_h, prev_l) if np.isfinite(prev_h) and np.isfinite(prev_l) else ""

    # Frequency pick from *historical* master file
    master = pd.read_csv(mp)
    master["Date"] = pd.to_datetime(master["Date"]).dt.normalize()
    pick, conf, reason, level, b, r, n, gap = _freq_pick(day, master)

    # Entry/SL/T1/T2 (batch SL exactly)
    entry = float(w0940_1505["Open"].iloc[0])
    orb_h, orb_l = float(orb["High"].max()), float(orb["Low"].min())
    rng = max(0.0, orb_h - orb_l)
    dbl_h, dbl_l = (orb_h + rng, orb_l - rng)
    orb_rng = (orb_h - orb_l) if (np.isfinite(orb_h) and np.isfinite(orb_l)) else np.nan

    if ot=="BULL" and pick=="BULL":
        stop = prev_l if (np.isfinite(prev_l) and _is_close(orb_l, prev_l, entry, orb_rng)) else orb_l
    elif ot=="BULL" and pick=="BEAR":
        stop = dbl_h
    elif ot=="BEAR" and pick=="BEAR":
        stop = prev_h if (np.isfinite(prev_h) and _is_close(orb_h, prev_h, entry, orb_rng)) else orb_h
    elif ot=="BEAR" and pick=="BULL":
        stop = dbl_l
    elif ot=="TR" and pick=="BEAR":
        stop = dbl_h
    elif ot=="TR" and pick=="BULL":
        stop = dbl_l
    else:
        # includes ABSTAIN → compute a neutral band anyway
        stop = dbl_l if pick=="BULL" else dbl_h

    long_side = (pick=="BULL")
    risk_per_share = (entry - stop) if long_side else (stop - entry)
    risk_rs = float(os.getenv("RISK_RS", DEFAULT_RISK_RS))
    qty = int(math.floor(risk_rs / risk_per_share)) if (np.isfinite(risk_per_share) and risk_per_share>0) else 0
    t1 = entry + risk_per_share if long_side else entry - risk_per_share
    t2 = entry + 2*risk_per_share if long_side else entry - 2*risk_per_share

    # final gating as in batch
    display = pick
    ready = True
    if pick=="ABSTAIN" or qty<=0:
        ready=False

    return {
        "symbol": sym, "date": str(day.date()),
        "tags": {"OpeningTrend": ot, "OpenLocation": ol, "PrevDayContext": pdc},
        "pick": display, "confidence": int(conf), "reason": reason,
        "entry": entry, "orb_high": orb_h, "orb_low": orb_l,
        "prev_high": prev_h, "prev_low": prev_l,
        "stop": stop, "t1": t1, "t2": t2,
        "risk_per_share": risk_per_share, "risk_rs": risk_rs, "qty": qty,
        "edge_pp": gap if np.isfinite(gap) else None, "hist_counts": {"bull": int(b), "bear": int(r), "n": int(n), "level": level},
        "ready": bool(ready)
    }
