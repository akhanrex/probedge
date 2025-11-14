from fastapi import APIRouter, HTTPException, Query
import pandas as pd, numpy as np, math, json
from datetime import datetime as dt
from probedge.storage.resolver import locate_for_read
from probedge.infra.loaders import read_tm5_csv, by_day_map
from probedge.infra.constants import SESSION_START, ORB_END, T0, T1, CLOSE_PCT, CLOSE_FR_ORB
from probedge.infra.constants import LOOKBACK_YEARS
from probedge.infra.settings import SETTINGS
from probedge.decision.classifiers_robust import (
    slice_window, prev_trading_day_ohlc, compute_openingtrend_robust,
    compute_openlocation_from_df, compute_prevdaycontext_robust
)
from probedge.decision.freq_pick import freq_pick

router = APIRouter()

def _slice_min(df_day, m0, m1):
    if df_day is None or df_day.empty: return pd.DataFrame()
    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    return df_day.loc[m, ["DateTime","Open","High","Low","Close","Date"]]

def _is_close(a, b, entry_px, orb_rng) -> bool:
    thr = np.inf; parts = []
    if np.isfinite(entry_px) and entry_px > 0: parts.append(entry_px * CLOSE_PCT)
    if np.isfinite(orb_rng): parts.append(abs(orb_rng) * CLOSE_FR_ORB)
    if parts: thr = min(parts)
    return (np.isfinite(a) and np.isfinite(b)) and abs(a - b) <= thr

@router.get("/api/plan")
def api_plan(symbol: str = Query(...), day: str | None = Query(None, description="YYYY-MM-DD; default=latest day in tm5")):
    # Load intraday
    p_tm5 = locate_for_read("intraday", symbol)
    if not p_tm5.exists():
        raise HTTPException(404, detail=f"TM5 not found for {symbol}")
    tm5 = read_tm5_csv(p_tm5)

    # Resolve target day
    if day:
        d0 = pd.to_datetime(day, errors="coerce")
    else:
        d0 = tm5["Date"].max()
    if pd.isna(d0):
        raise HTTPException(400, detail="Invalid or missing day")
    day_norm = pd.to_datetime(d0).normalize()

    by_day = by_day_map(tm5)
    df_day = by_day.get(day_norm)
    if df_day is None or df_day.empty:
        raise HTTPException(404, detail=f"No intraday bars for {symbol} {day_norm.date()}")

    # Prev-day OHLC + tags
    prev_ohlc = prev_trading_day_ohlc(tm5, day_norm)
    ot  = compute_openingtrend_robust(df_day)
    ol  = compute_openlocation_from_df(df_day, prev_ohlc) if prev_ohlc else ""
    pdc = compute_prevdaycontext_robust(prev_ohlc["open"], prev_ohlc["high"], prev_ohlc["low"], prev_ohlc["close"]) if prev_ohlc else ""

    # Load master for freq pick
    p_master = locate_for_read("masters", symbol)
    if not p_master.exists():
        raise HTTPException(404, detail=f"MASTER not found for {symbol}")
    master = pd.read_csv(p_master)

    pick, conf_pct, reason, level, stats = freq_pick(day_norm, master)

    # If abstain → parity behavior: no plan
    if pick == "ABSTAIN":
        return {
            "symbol": symbol.upper(),
            "date": str(day_norm.date()),
            "tags": {"OpeningTrend": ot, "OpenLocation": ol, "PrevDayContext": pdc},
            "pick": "ABSTAIN",
            "confidence%": conf_pct,
            "reason": reason,
            "parity_mode": True,
        }

    long_side = (pick == "BULL")
    # 09:40 open (entry px)
    w09 = df_day[(df_day["_mins"] >= 9*60+40) & (df_day["_mins"] <= 15*60+5)]
    if w09.empty:
        raise HTTPException(500, detail="Missing 09:40→15:05 window")
    entry_px = float(w09["Open"].iloc[0])

    # ORB for SL calc
    w_orb = df_day[(df_day["_mins"] >= 9*60+15) & (df_day["_mins"] <= 9*60+35)]
    if w_orb.empty:
        raise HTTPException(500, detail="Missing ORB window")
    orb_h = float(w_orb["High"].max()); orb_l = float(w_orb["Low"].min())
    rng = max(0.0, orb_h - orb_l)
    dbl_h, dbl_l = (orb_h + rng, orb_l - rng)
    prev_h = float(prev_ohlc["high"]) if prev_ohlc else np.nan
    prev_l = float(prev_ohlc["low"])  if prev_ohlc else np.nan
    orb_rng = (orb_h - orb_l) if (np.isfinite(orb_h) and np.isfinite(orb_l)) else np.nan

    # Stops per Colab parity
    if ot == "BULL" and pick == "BULL":
        stop = prev_l if (np.isfinite(prev_l) and _is_close(orb_l, prev_l, entry_px, orb_rng)) else orb_l
    elif ot == "BULL" and pick == "BEAR":
        stop = dbl_h
    elif ot == "BEAR" and pick == "BEAR":
        stop = prev_h if (np.isfinite(prev_h) and _is_close(orb_h, prev_h, entry_px, orb_rng)) else orb_h
    elif ot == "BEAR" and pick == "BULL":
        stop = dbl_l
    elif ot == "TR" and pick == "BEAR":
        stop = dbl_h
    elif ot == "TR" and pick == "BULL":
        stop = dbl_l
    else:
        stop = dbl_l if long_side else dbl_h

    risk_per_share = (entry_px - stop) if long_side else (stop - entry_px)
    if (not np.isfinite(risk_per_share)) or risk_per_share <= 0:
        return {
            "symbol": symbol.upper(),
            "date": str(day_norm.date()),
            "tags": {"OpeningTrend": ot, "OpenLocation": ol, "PrevDayContext": pdc},
            "pick": pick, "confidence%": conf_pct,
            "skip": "bad SL/risk",
            "parity_mode": True,
        }

    # Phase B parity: use full daily risk as per-trade risk (matches Colab RISK_RS)
    per_trade_risk_rs = int(SETTINGS.risk_budget_rs)
    qty = int(math.floor(per_trade_risk_rs / risk_per_share))

    if qty <= 0:
        return {
            "symbol": symbol.upper(),
            "date": str(day_norm.date()),
            "tags": {"OpeningTrend": ot, "OpenLocation": ol, "PrevDayContext": pdc},
            "pick": pick, "confidence%": conf_pct,
            "skip": "qty=0",
            "parity_mode": True,
        }

    t1 = entry_px + risk_per_share if long_side else entry_px - risk_per_share
    t2 = entry_px + 2*risk_per_share if long_side else entry_px - 2*risk_per_share

    plan = {
        "symbol": symbol.upper(),
        "date": str(day_norm.date()),
        "tags": {"OpeningTrend": ot, "OpenLocation": ol, "PrevDayContext": pdc},
        "pick": pick, "confidence%": conf_pct, "reason": reason,
        "entry": round(entry_px, 4), "stop": round(float(stop), 4),
        "qty": int(qty), "risk_per_share": round(float(risk_per_share), 4),
        "target1": round(float(t1), 4), "target2": round(float(t2), 4),
        "per_trade_risk_rs_used": per_trade_risk_rs,
        "parity_mode": True
    }
    # JSON-safe
    return json.loads(pd.DataFrame([plan]).to_json(orient="records"))[0]
