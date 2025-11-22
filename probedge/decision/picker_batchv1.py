
from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import time as _time

# ---- thresholds (mirror your batch code) ----
T0 = _time(9, 40); T1 = _time(15, 5)
SESSION_START = _time(9, 15); ORB_END = _time(9, 35)

LOOKBACK_YEARS = 6
EDGE_PP = 8.0
CONF_FLOOR = 55
MIN3, MIN2, MIN1, MIN0 = 8, 6, 4, 3
REQUIRE_OT_ALIGN = True

def _to_dt_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def slice_window(df_intraday: pd.DataFrame, start_hm, end_hm) -> pd.DataFrame:
    if df_intraday is None or df_intraday.empty:
        return pd.DataFrame(columns=["DateTime","Open","High","Low","Close"])
    d = df_intraday.copy()
    d["DateTime"] = _to_dt_series(d["DateTime"])
    t = d["DateTime"].dt.time
    return d[(t >= start_hm) & (t <= end_hm)].copy()

def prev_trading_day_ohlc(df_intraday: pd.DataFrame, day_norm: pd.Timestamp):
    if df_intraday is None or df_intraday.empty:
        return None
    dti = _to_dt_series(df_intraday["DateTime"]).dt.normalize()
    alld = sorted(dti.unique())
    p = None
    day_norm = pd.to_datetime(day_norm).normalize()
    for i in range(1, 8):
        cand = day_norm - pd.Timedelta(days=i)
        if cand in alld:
            p = cand; break
    if p is None:
        return None
    d = df_intraday[dti.eq(p)].copy()
    if d.empty:
        return None
    return {
        "open":  float(d.loc[d["DateTime"].idxmin(), "Open"]),
        "high":  float(d["High"].max()),
        "low":   float(d["Low"].min()),
        "close": float(d.loc[d["DateTime"].idxmax(), "Close"]),
    }

# --- Opening Trend (robust) ---
TH_MOVE = 0.35; TH_RANGE = 0.80; TH_TINY_MOVE = 0.30
TH_POS_TOP = 0.60; TH_POS_BOTTOM = 0.40; TH_DIR = 2; TH_OVERLAP = 0.50

def _overlap_score(df):
    if df is None or len(df) < 2: return 0.0
    hi = df['High'].to_numpy(); lo = df['Low'].to_numpy()
    ov = []
    for i in range(1, len(df)):
        num = max(0.0, min(hi[i], hi[i-1]) - max(lo[i], lo[i-1]))
        den = max(1e-9, (max(hi[i], hi[i-1]) - min(lo[i], lo[i-1])))
        ov.append(num/den)
    return float(np.mean(ov)) if ov else 0.0

def _dir_count(df):
    up = (df['Close'] > df['Open']).sum()
    dn = (df['Close'] < df['Open']).sum()
    return int(up - dn)

def compute_openingtrend_robust(df_day_intraday: pd.DataFrame) -> str:
    win = slice_window(df_day_intraday, _time(9,15), _time(9,40))
    if win.empty: return "TR"
    win = win.sort_values("DateTime")
    O0 = float(win['Open'].iloc[0]); Cn = float(win['Close'].iloc[-1])
    Hmax = float(win['High'].max()); Lmin = float(win['Low'].min())
    move_pct  = 100.0 * (Cn - O0) / max(1e-9, O0)
    range_pct = 100.0 * (Hmax - Lmin) / max(1e-9, O0)
    pos = 0.5 if Hmax<=Lmin else (Cn - Lmin) / (Hmax - Lmin)
    dcount = _dir_count(win); ovl = _overlap_score(win)
    if (range_pct < TH_RANGE) and (abs(move_pct) < TH_TINY_MOVE) and (ovl > TH_OVERLAP):
        return "TR"
    v_dist =  1 if move_pct >= +TH_MOVE else (-1 if move_pct <= -TH_MOVE else 0)
    v_pos  =  1 if pos      >= TH_POS_TOP else (-1 if pos <= TH_POS_BOTTOM else 0)
    v_pers =  1 if dcount   >= TH_DIR     else (-1 if dcount <= -TH_DIR     else 0)
    S = v_dist + v_pos + v_pers
    return "BULL" if S >= +2 else ("BEAR" if S <= -2 else "TR")

def compute_openlocation(day_open: float, prev_ohlc) -> str:
    if prev_ohlc is None or day_open is None or pd.isna(day_open):
        return ""
    H = float(prev_ohlc.get("high", np.nan)); L = float(prev_ohlc.get("low",  np.nan))
    if pd.isna(H) or pd.isna(L) or H <= L: return ""
    rng = H - L; o = float(day_open)
    if o < L:            return "OBR"
    if o <= L + 0.3*rng: return "OOL"
    if o > H:            return "OAR"
    if o >= H - 0.3*rng: return "OOH"
    return "OIM"

def compute_openlocation_from_df(df_day_intraday: pd.DataFrame, prev_ohlc=None) -> str:
    if df_day_intraday is None or df_day_intraday.empty or prev_ohlc is None:
        return ""
    d = df_day_intraday.sort_values("DateTime")
    try:
        day_open = float(d["Open"].iloc[0])
    except Exception:
        return ""
    return compute_openlocation(day_open, prev_ohlc)

# ---- TM5 reader (robust) ----
def read_tm5(tm5_path: str) -> pd.DataFrame:
    """
    Robust 5-minute intraday loader.

    Accepts any of these datetime layouts:
      - 'DateTime'
      - 'DATETIME'
      - 'date_time'
      - separate 'Date' + 'Time'

    Returns a DataFrame with:
      - DateTime: pandas datetime64[ns]
      - Date: normalized date (midnight)
    Drops rows with missing DateTime or OHLC.
    """
    from pathlib import Path

    p = Path(tm5_path)
    if not p.exists():
        raise FileNotFoundError(f"TM5 not found: {p}")

    df_raw = pd.read_csv(p)
    # clean column names
    df_raw.columns = [str(c).strip() for c in df_raw.columns]

    # --- find / build DateTime ---
    dt = None

    if "DateTime" in df_raw.columns:
        dt = pd.to_datetime(df_raw["DateTime"], errors="coerce")
    elif "DATETIME" in df_raw.columns:
        dt = pd.to_datetime(df_raw["DATETIME"], errors="coerce")
    elif "date_time" in df_raw.columns:
        dt = pd.to_datetime(df_raw["date_time"], errors="coerce")
    elif "Date" in df_raw.columns and "Time" in df_raw.columns:
        dt = pd.to_datetime(
            df_raw["Date"].astype(str).str.strip()
            + " "
            + df_raw["Time"].astype(str).str.strip(),
            errors="coerce",
        )

    if dt is None:
        raise ValueError(f"Cannot locate datetime columns in TM5: {p}")

    df = df_raw.copy()
    df["DateTime"] = dt

    # --- normalize OHLC column names ---
    rename_map = {}
    for col in df.columns:
        low = col.lower()
        if low == "open":
            rename_map[col] = "Open"
        elif low == "high":
            rename_map[col] = "High"
        elif low == "low":
            rename_map[col] = "Low"
        elif low == "close":
            rename_map[col] = "Close"
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    needed = ["DateTime", "Open", "High", "Low", "Close"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in TM5 {p}: {missing}")

    # drop junk and sort
    df = (
        df.dropna(subset=needed)
          .sort_values("DateTime")
          .reset_index(drop=True)
    )

    # normalized date column we use for day filtering
    df["Date"] = df["DateTime"].dt.normalize()

    return df


def _freq_pick(day, master: pd.DataFrame, lookback_years: int = LOOKBACK_YEARS,
               edge_pp: float = EDGE_PP, conf_floor: int = CONF_FLOOR, require_ot_align: bool = REQUIRE_OT_ALIGN):
    mrow = master.loc[master["Date"] == day]
    if mrow.empty:
        return "ABSTAIN", 0, "missing master row"
    def g(col):
        try: return str(mrow[col].iloc[0]).strip().upper()
        except Exception: return ""
    otoday = g("OpeningTrend"); ol_today = g("OpenLocation"); pdc_today = g("PrevDayContext")
    base = master[(master["Date"] < day) & (master["Date"] >= (pd.to_datetime(day) - pd.DateOffset(years=lookback_years)))].copy()
    def _match(df, use_ol, use_pdc):
        m = df[df["OpeningTrend"] == otoday] if "OpeningTrend" in df.columns else df
        if use_ol and ("OpenLocation" in m.columns) and ol_today: m = m[m["OpenLocation"] == ol_today]
        if use_pdc and ("PrevDayContext" in m.columns) and pdc_today: m = m[m["PrevDayContext"] == pdc_today]
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
    if len(hist) < (MIN2 if level == "L2" else MIN3): level, hist = "L1", _match(base, False, False)
    if len(hist) < (MIN1 if level == "L1" else (MIN2 if level == "L2" else MIN3)): level, hist = "L0", base
    pick, conf, b, r, n, gap = decide(hist)

    # Broaden if edge weak
    def try_level(df, lvl):
        p,c,B,R,N,G = decide(df)
        return (p,c,B,R,N,G,lvl,df)
    if not np.isnan(gap) and gap < edge_pp:
        if level == "L3":
            p2,c2,B2,R2,N2,G2,lv2,h2 = try_level(_match(base, True, False), "L2")
            if N2 >= MIN2 and (not np.isnan(G2) and G2 >= edge_pp): pick,conf,b,r,n,gap,level,hist = p2,c2,B2,R2,N2,G2,lv2,h2
        if (level in ("L3","L2")) and (gap < edge_pp):
            p1,c1,B1,R1,N1,G1,lv1,h1 = try_level(_match(base, False, False), "L1")
            if N1 >= MIN1 and (not np.isnan(G1) and G1 >= edge_pp): pick,conf,b,r,n,gap,level,hist = p1,c1,B1,R1,N1,G1,lv1,h1
        if (level in ("L3","L2","L1")) and (gap < edge_pp):
            p0,c0,B0,R0,N0,G0,lv0,h0 = try_level(base, "L0")
            if N0 >= MIN0 and (not np.isnan(G0) and G0 >= edge_pp): pick,conf,b,r,n,gap,level,hist = p0,c0,B0,R0,N0,G0,lv0,h0

    req = {"L3":MIN3,"L2":MIN2,"L1":MIN1,"L0":MIN0}[level]
    display_pick = pick if (n >= req and (not np.isnan(gap) and gap >= edge_pp) and conf >= conf_floor) else "ABSTAIN"
    if require_ot_align and display_pick != "ABSTAIN" and otoday in ("BULL","BEAR") and display_pick != otoday:
        display_pick = "ABSTAIN"
    reason = (f"{level} freq: OT={otoday or '-'}, OL={ol_today or '-'}, PDC={pdc_today or '-'} | "
              f"hist N={n}, conf={conf}% | gap>=edge? {gap if gap==gap else 'NA'}pp | "
              f"{'OT-align' if require_ot_align else ''}")
    return display_pick, conf, reason

def decide_for_day(df_tm5: pd.DataFrame, master: pd.DataFrame, day: pd.Timestamp):
    day = pd.to_datetime(day).normalize()
    dfd = df_tm5[df_tm5["Date"] == day].sort_values("DateTime")
    if dfd.empty:
        return None
    ot = compute_openingtrend_robust(dfd)
    prev = prev_trading_day_ohlc(df_tm5, day)
    ol  = compute_openlocation_from_df(dfd, prev)
    # ORB window for SL calculations
    w_orb = dfd[(dfd["_mins"] >= 9*60+15) & (dfd["_mins"] <= 9*60+35)]
    if w_orb.empty:
        return None
    orb_h, orb_l = float(w_orb["High"].max()), float(w_orb["Low"].min())

    # 09:40→ close window and entry at first open >=09:40
    w09 = dfd[(dfd["_mins"] >= 9*60+40) & (dfd["_mins"] <= 15*60+5)].sort_values("DateTime")
    if w09.empty:
        return None
    entry_px = float(w09["Open"].iloc[0])

    pick, conf, reason = _freq_pick(day, master)

    prev_h = float(prev["high"]) if prev else float("nan")
    prev_l = float(prev["low"]) if prev else float("nan")

    return {
        "Date": day,
        "OpeningTrend": ot,
        "OpenLocation": ol,
        "Pick": pick,
        "Confidence%": conf,
        "Reason": reason,
        "ORB_H": orb_h,
        "ORB_L": orb_l,
        "Prev_H": prev_h,
        "Prev_L": prev_l,
        "Entry": entry_px,
    }
