import math
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Any, Dict, List, Optional


import numpy as np
import pandas as pd

from probedge.storage.resolver import locate_for_read
from probedge.infra.constants import CLOSE_PCT, CLOSE_FR_ORB
from probedge.infra.settings import SETTINGS
from ..infra.logger import get_logger
from ..infra.loaders import read_tm5_csv

logger = get_logger(__name__)

from probedge.decision.classifiers_robust import (
    prev_trading_day_ohlc,
    compute_openingtrend_robust,
    compute_openlocation_from_df,
    compute_prevdaycontext_robust,
)
from probedge.decision.freq_pick import freq_pick


def _is_close(a: float, b: float, entry_px: float, orb_rng: float) -> bool:
    """Colab parity: CLOSE_PCT + CLOSE_FR_ORB band."""
    thr = np.inf
    parts = []
    if np.isfinite(entry_px) and entry_px > 0:
        parts.append(entry_px * CLOSE_PCT)
    if np.isfinite(orb_rng):
        parts.append(abs(orb_rng) * CLOSE_FR_ORB)
    if parts:
        thr = min(parts)
    return (np.isfinite(a) and np.isfinite(b)) and abs(a - b) <= thr


def _effective_daily_risk_rs() -> int:
    """
    Daily risk budget:
      - MODE=test  -> 1000
      - else       -> SETTINGS.risk_budget_rs (e.g. 10000)
    """
    if getattr(SETTINGS, "mode", "paper") == "test":
        return 1000
    return int(getattr(SETTINGS, "risk_budget_rs", 10000))

def _load_tm5_flex(path: str) -> pd.DataFrame:
    """
    Single source of truth TM5 loader for the planner.

    Uses infra.loaders.read_tm5_csv so that /api/state, /api/plan/*,
    backtests etc all see identical intraday bars (DateTime, Date, OHLC).
    """
    logger.info("[_load_tm5_flex] reading tm5 via read_tm5_csv: %s", path)
    df = read_tm5_csv(path)

    # Safety: ensure we have a plain date column.
    if "Date" not in df.columns:
        df["Date"] = df["DateTime"].dt.date

    return df


def build_parity_plan(symbol: str, day_str: Optional[str] = None) -> Dict[str, Any]:
    """
    Core Colab-parity plan for a single symbol/day.
    - Uses FULL daily risk as per-trade risk (RISK_RS).
    - Does NOT do portfolio split; that is layered on top.
    - Returns a dict; never raises HTTPException.
    """
    sym_upper = symbol.upper()

    # ---------- Load intraday ----------
    p_tm5 = locate_for_read("intraday", sym_upper)
    if not p_tm5.exists():
        return {
            "symbol": sym_upper,
            "date": None,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "no_tm5",
            "error": f"TM5 not found for {sym_upper}",
            "parity_mode": True,
        }

    try:
        tm5 = _load_tm5_flex(p_tm5)
    except Exception as e:
        return {
            "symbol": sym_upper,
            "date": None,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "tm5_read_error",
            "error": f"Failed to read TM5: {e}",
            "parity_mode": True,
        }


    if tm5.empty:
        return {
            "symbol": sym_upper,
            "date": None,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "tm5_empty",
            "error": "TM5 data is empty",
            "parity_mode": True,
        }

    # ---------- Resolve day ----------
    # Use __date (python date) so we don't fight with timezone/normalize issues.
    if "__date" not in tm5.columns:
        tm5["__date"] = tm5["DateTime"].dt.date

    if day_str:
        d0 = pd.to_datetime(day_str, errors="coerce")
        if pd.isna(d0):
            return {
                "symbol": sym_upper,
                "date": None,
                "pick": "ABSTAIN",
                "confidence%": 0,
                "skip": "bad_day",
                "error": "Invalid or missing day",
                "parity_mode": True,
            }
        day_date = d0.date()
    else:
        # latest day present in this TM5
        day_date = tm5["__date"].max()

    day_norm = pd.to_datetime(day_date).normalize()

    df_day = tm5[tm5["__date"] == day_date].copy()
    if df_day.empty:
        return {
            "symbol": sym_upper,
            "date": str(day_date),
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "no_intraday_for_day",
            "error": f"No intraday bars for {sym_upper} {day_date}",
            "parity_mode": True,
        }

    # ---------- Prev-day OHLC + tags ----------
    prev_ohlc = prev_trading_day_ohlc(tm5, day_norm)
    ot = compute_openingtrend_robust(df_day)
    if prev_ohlc:
        # Use the same signatures as ops/rebuild_master_recent.py (classifiers)
        ol = compute_openlocation_from_df(df_day, prev_ohlc)
        pdc = compute_prevdaycontext_robust(prev_ohlc)
    else:
        ol = ""
        pdc = ""


    tags = {
        "OpeningTrend": ot,
        "OpenLocation": ol,
        "PrevDayContext": pdc,
    }



    # ---------- Load master for freq pick ----------
    p_master = locate_for_read("masters", sym_upper)
    if not p_master.exists():
        return {
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "no_master",
            "error": f"MASTER not found for {sym_upper}",
            "parity_mode": True,
        }

    try:
        master = pd.read_csv(p_master)
    except Exception as e:
        return {
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "master_read_error",
            "error": f"Failed to read MASTER: {e}",
            "parity_mode": True,
        }

    pick, conf_pct, reason, level, stats = freq_pick(day_norm, master)

    # Abstain → no trade, but still return tags + reason
    if pick == "ABSTAIN":
        return {
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "ABSTAIN",
            "parity_mode": True,
        }

    long_side = (pick == "BULL")

    # ---------- 09:40→15:05 window (entry) ----------
    w09 = df_day[(df_day["_mins"] >= 9 * 60 + 40) & (df_day["_mins"] <= 15 * 60 + 5)]
    if w09.empty:
        return {
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": pick,
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "missing_0940_1505_window",
            "parity_mode": True,
        }

    entry_px = float(w09["Open"].iloc[0])

    # ---------- ORB window (09:15→09:35) ----------
    w_orb = df_day[(df_day["_mins"] >= 9 * 60 + 15) & (df_day["_mins"] <= 9 * 60 + 35)]
    if w_orb.empty:
        return {
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": pick,
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "missing_orb_window",
            "parity_mode": True,
        }

    orb_h = float(w_orb["High"].max())
    orb_l = float(w_orb["Low"].min())
    rng = max(0.0, orb_h - orb_l)
    dbl_h, dbl_l = (orb_h + rng, orb_l - rng)
    prev_h = float(prev_ohlc["high"]) if prev_ohlc else np.nan
    prev_l = float(prev_ohlc["low"]) if prev_ohlc else np.nan
    orb_rng = (orb_h - orb_l) if (np.isfinite(orb_h) and np.isfinite(orb_l)) else np.nan

    # ---------- SL logic (Colab rules) ----------
    if ot == "BULL" and pick == "BULL":
        stop = (
            prev_l
            if (np.isfinite(prev_l) and _is_close(orb_l, prev_l, entry_px, orb_rng))
            else orb_l
        )
    elif ot == "BULL" and pick == "BEAR":
        stop = dbl_h
    elif ot == "BEAR" and pick == "BEAR":
        stop = (
            prev_h
            if (np.isfinite(prev_h) and _is_close(orb_h, prev_h, entry_px, orb_rng))
            else orb_h
        )
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
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": pick,
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "bad_SL_or_risk",
            "parity_mode": True,
        }

    # ---------- RISK_RS and quantity (parity mode) ----------
    daily_risk_rs = _effective_daily_risk_rs()
    per_trade_risk_rs = daily_risk_rs  # parity: full daily risk used per trade
    qty = int(math.floor(per_trade_risk_rs / risk_per_share))

    if qty <= 0:
        return {
            "symbol": sym_upper,
            "date": str(day_norm.date()),
            "tags": tags,
            "pick": pick,
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "qty=0",
            "parity_mode": True,
        }

    t1 = entry_px + risk_per_share if long_side else entry_px - risk_per_share
    t2 = entry_px + 2 * risk_per_share if long_side else entry_px - 2 * risk_per_share

    plan = {
        "symbol": sym_upper,
        "date": str(day_norm.date()),
        "tags": tags,
        "pick": pick,
        "confidence%": int(conf_pct),
        "reason": reason,
        "entry": round(float(entry_px), 4),
        "stop": round(float(stop), 4),
        "qty": int(qty),
        "risk_per_share": round(float(risk_per_share), 4),
        "target1": round(float(t1), 4),
        "target2": round(float(t2), 4),
        "per_trade_risk_rs_used": int(per_trade_risk_rs),
        "parity_mode": True,
    }
    return plan
