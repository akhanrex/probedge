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

# NOTE: we only need prev_trading_day_ohlc from the robust module now.
from probedge.decision.classifiers_robust import prev_trading_day_ohlc
from probedge.decision.freq_pick import freq_pick


def _is_close(a: float, b: float, entry_px: float, orb_rng: float) -> bool:
    """Colab parity: CLOSE_PCT + CLOSE_FR_ORB band."""
    thr = float("inf")
    parts: List[float] = []
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

    CTO FIX:
    - tags is always defined (no NameError in early returns)
    - if today's MASTER row isn't present at 09:40, compute tags from TM5 and
      pass them into the manual batch selector via freq_pick(tags_override=...)
    """
    sym_upper = symbol.upper()

    # ---------- Default tags (always defined; safe for ALL early returns) ----------
    tags: Dict[str, str] = {
        "OpeningTrend": "",
        "OpenLocation": "",
        "PrevDayContext": "",
    }

    # ---------- Load intraday ----------
    p_tm5 = locate_for_read("intraday", sym_upper)
    if not p_tm5.exists():
        return {
            "symbol": sym_upper,
            "date": None,
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "no_tm5",
            "error": f"TM5 not found for {sym_upper}",
            "parity_mode": True,
        }

    try:
        tm5 = _load_tm5_flex(str(p_tm5))
    except Exception as e:
        return {
            "symbol": sym_upper,
            "date": None,
            "tags": tags,
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
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "tm5_empty",
            "error": "TM5 data is empty",
            "parity_mode": True,
        }

    # ---------- Resolve day ----------
    if "__date" not in tm5.columns:
        tm5["__date"] = tm5["DateTime"].dt.date

    avail_days = sorted([d for d in pd.unique(tm5["__date"]) if pd.notna(d)])
    if not avail_days:
        return {
            "symbol": sym_upper,
            "date": day_str if day_str else None,
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "tm5_no_dates",
            "error": "TM5 has no usable dates",
            "parity_mode": True,
        }

    requested_day_norm = None
    if day_str:
        d0 = pd.to_datetime(day_str, errors="coerce")
        if pd.isna(d0):
            return {
                "symbol": sym_upper,
                "date": None,
                "tags": tags,
                "pick": "ABSTAIN",
                "confidence%": 0,
                "skip": "bad_day",
                "error": "Invalid or missing day",
                "parity_mode": True,
            }
        requested_day_norm = pd.Timestamp(d0).normalize()
        requested_day_str = requested_day_norm.date().isoformat()
    else:
        requested_day_str = None

    if requested_day_norm is None:
        effective_day_date = avail_days[-1]
        requested_day_str = effective_day_date.isoformat()
    else:
        req_date = requested_day_norm.date()
        if req_date in avail_days:
            effective_day_date = req_date
        else:
            prev = [d for d in avail_days if d <= req_date]
            effective_day_date = prev[-1] if prev else avail_days[0]

    effective_data_day_str = effective_day_date.isoformat()
    day_date = effective_day_date
    day_norm = pd.to_datetime(day_date).normalize()

    df_day = tm5[tm5["__date"] == day_date].copy()
    if df_day.empty:
        return {
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "no_intraday_for_effective_day",
            "error": f"No intraday bars for {sym_upper} effective_day={effective_data_day_str}",
            "parity_mode": True,
        }

    # ---------- Prev-day OHLC (used both for tags + SL logic) ----------
    prev_ohlc = None
    try:
        prev_ohlc = prev_trading_day_ohlc(tm5, day_norm)
    except Exception as e:
        logger.warning(
            "[build_parity_plan] prev_trading_day_ohlc failed for %s %s: %s",
            sym_upper,
            day_norm.date(),
            e,
        )

    prev_h = float(prev_ohlc["high"]) if prev_ohlc and "high" in prev_ohlc else np.nan
    prev_l = float(prev_ohlc["low"]) if prev_ohlc and "low" in prev_ohlc else np.nan

    # ---------- CTO FIX: compute tags from TM5 (fallback when MASTER row missing) ----------
    tags_override = dict(tags)

    def _try_import_tag_fns():
        # Try probedge.core.classifiers first (your single source of truth),
        # then probedge.decision.classifiers_robust if you kept the fns there.
        for modname in ("probedge.core.classifiers", "probedge.decision.classifiers_robust"):
            try:
                mod = __import__(modname, fromlist=["*"])
                pdc_fn = getattr(mod, "compute_prevdaycontext_robust", None)
                ol_fn  = getattr(mod, "compute_openlocation_from_df", None)
                ot_fn  = getattr(mod, "compute_openingtrend_robust", None)
                if pdc_fn or ol_fn or ot_fn:
                    return pdc_fn, ol_fn, ot_fn
            except Exception:
                continue
        return None, None, None

    pdc_fn, ol_fn, ot_fn = _try_import_tag_fns()

    try:
        if prev_ohlc is not None and pdc_fn:
            tags_override["PrevDayContext"] = str(pdc_fn(prev_ohlc) or "")
    except Exception:
        pass

    try:
        if prev_ohlc is not None and ol_fn:
            tags_override["OpenLocation"] = str(ol_fn(tm5, day_norm, prev_ohlc) or "")
    except Exception:
        pass

    # OpeningTrend function signatures can vary; try a few safe patterns
    if ot_fn:
        for args in (
            (tm5, day_norm, prev_ohlc),
            (tm5, day_norm),
            (df_day, prev_ohlc),
            (df_day,),
        ):
            try:
                v = ot_fn(*args)
                if v is not None:
                    tags_override["OpeningTrend"] = str(v or "")
                    break
            except TypeError:
                continue
            except Exception:
                continue

    # ---------- Load master for freq pick & tags ----------
    p_master = locate_for_read("masters", sym_upper)
    if not p_master.exists():
        return {
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
            "tags": tags_override,
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
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
            "tags": tags_override,
            "pick": "ABSTAIN",
            "confidence%": 0,
            "skip": "master_read_error",
            "error": f"Failed to read MASTER: {e}",
            "parity_mode": True,
        }

    # Override tags from MASTER row if present; else use computed tags_override
    try:
        if "Date" in master.columns:
            mdates = pd.to_datetime(master["Date"], errors="coerce").dt.normalize()
            row_today = master.loc[mdates == day_norm]
            if not row_today.empty:
                r = row_today.iloc[0]
                tags = {
                    "OpeningTrend": str(r.get("OpeningTrend", "") or ""),
                    "OpenLocation": str(r.get("OpenLocation", "") or ""),
                    "PrevDayContext": str(r.get("PrevDayContext", "") or ""),
                }
            else:
                tags = dict(tags_override)
        else:
            tags = dict(tags_override)
    except Exception as e:
        logger.warning(
            "[build_parity_plan] failed to override tags from MASTER for %s %s: %s",
            sym_upper,
            day_norm.date(),
            e,
        )
        tags = dict(tags_override)

    # ---------- Freq pick using MASTER, with fallback tags override ----------
    pick, conf_pct, reason, level, stats = freq_pick(day_norm, master, tags_override=tags)

    if pick == "ABSTAIN":
        return {
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
            "tags": tags,
            "pick": "ABSTAIN",
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "ABSTAIN",
            "parity_mode": True,
        }

    long_side = (pick == "BULL")
    ot = tags.get("OpeningTrend") or "TR"

    # ---------- 09:40→15:05 window (entry) ----------
    if "_mins" in df_day.columns:
        w09 = df_day[(df_day["_mins"] >= 9 * 60 + 40) & (df_day["_mins"] <= 15 * 60 + 5)]
    else:
        dt = pd.to_datetime(df_day["DateTime"])
        mins = dt.dt.hour * 60 + dt.dt.minute
        w09 = df_day[(mins >= 9 * 60 + 40) & (mins <= 15 * 60 + 5)]

    if w09.empty:
        return {
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
            "tags": tags,
            "pick": pick,
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "missing_0940_1505_window",
            "parity_mode": True,
        }

    entry_px = float(w09["Open"].iloc[0])

    # ---------- ORB window (09:15→09:35) ----------
    if "_mins" in df_day.columns:
        w_orb = df_day[(df_day["_mins"] >= 9 * 60 + 15) & (df_day["_mins"] <= 9 * 60 + 35)]
    else:
        dt = pd.to_datetime(df_day["DateTime"])
        mins = dt.dt.hour * 60 + dt.dt.minute
        w_orb = df_day[(mins >= 9 * 60 + 15) & (mins <= 9 * 60 + 35)]

    if w_orb.empty:
        return {
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
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
    orb_rng = (orb_h - orb_l) if (np.isfinite(orb_h) and np.isfinite(orb_l)) else np.nan

    # ---------- SL logic (Colab parity, using OT tag) ----------
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
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
            "tags": tags,
            "pick": pick,
            "confidence%": int(conf_pct),
            "reason": reason,
            "skip": "bad_SL_or_risk",
            "parity_mode": True,
        }

    daily_risk_rs = _effective_daily_risk_rs()
    per_trade_risk_rs = daily_risk_rs
    qty = int(math.floor(per_trade_risk_rs / risk_per_share))

    if qty <= 0:
        return {
            "symbol": sym_upper,
            "date": requested_day_str,
            "effective_data_day": effective_data_day_str,
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
        "date": requested_day_str,
        "effective_data_day": effective_data_day_str,
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
