# apps/api/routes/plan.py
from fastapi import APIRouter, HTTPException, Query


def _to_naive_date_series(s):
    import pandas as pd
    dt = pd.to_datetime(s, errors="coerce")
    try:
        # If tz-aware, drop tz
        dt = dt.dt.tz_localize(None)
    except Exception:
        pass
    return dt.dt.normalize()

def _to_naive_date(ts):
    import pandas as pd
    d = pd.to_datetime(ts, errors="coerce")
    try:
        d = d.tz_localize(None)
    except Exception:
        pass
    return d.normalize()

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

# ==== core tags = {
        "OpeningTrend": g("OpeningTrend"),
        "OpenLocation": g("OpenLocation"),
        "PrevDayContext": g("PrevDayContext"),
    }[level]
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
    last_dt = pd.to_datetime(tm5['Date'].max(), errors='coerce')
    day = _to_naive_date(last_dt)
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
        prev_last_dt = pd.to_datetime(tm5['Date'].max(), errors='coerce')
    day = _to_naive_date(last_dt)
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
