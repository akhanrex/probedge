#!/usr/bin/env python3
"""
Leakage Audit Suite for ProbEdge

Goal: Detect the two big PnL-inflaters in backtests:
1) Lookahead in PLAN building (using data after 09:40 to decide pick/SL/etc)
2) Intrabar ambiguity / optimistic tie-break in EXEC (stop & target touched in same 5-min bar)

Run from repo root (same folder as config/, probedge/, apps/):
  source .venv/bin/activate
  python scripts/leakage_audit.py plan_lookahead --days 60
  python scripts/leakage_audit.py tie_bias      --days 120

Outputs:
  data/leakage/plan_lookahead_report.csv
  data/leakage/tie_bias_report.csv
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

IST_T0 = 9 * 60 + 40   # 09:40
IST_T1 = 15 * 60 + 5   # 15:05
ORB0  = 9 * 60 + 15    # 09:15
ORB1  = 9 * 60 + 35    # 09:35


def _ensure_repo_root() -> None:
    # config/frequency.yaml is loaded relative to CWD
    if not Path("config/frequency.yaml").exists():
        raise SystemExit("Run this from repo root (where config/frequency.yaml exists).")


def _effective_daily_risk_rs(settings) -> int:
    # mirrors plan_core._effective_daily_risk_rs()
    if getattr(settings, "mode", "paper") == "test":
        return 1000
    return int(getattr(settings, "risk_budget_rs", 10000))


def _read_intraday(sym: str) -> pd.DataFrame:
    from probedge.storage.resolver import locate_for_read
    from probedge.infra.loaders import read_tm5_csv

    p = locate_for_read("intraday", sym.upper())
    df = read_tm5_csv(str(p))
    if "Date" not in df.columns:
        df["Date"] = df["DateTime"].dt.date
    # ensure _mins and __date
    if "_mins" not in df.columns:
        df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute
    if "__date" not in df.columns:
        df["__date"] = df["DateTime"].dt.date
    return df


def _read_master(sym: str) -> pd.DataFrame:
    from probedge.storage.resolver import locate_for_read
    p = locate_for_read("masters", sym.upper())
    return pd.read_csv(p)


def _common_days_last_n(symbols: List[str], n: int) -> List[str]:
    days_sets = []
    for sym in symbols:
        df = _read_intraday(sym)
        days_sets.append(set(pd.Series(df["__date"]).dropna().astype(str).unique()))
    common = sorted(set.intersection(*days_sets))
    return common[-n:] if n > 0 else common


def _get_master_tags_for_day(master: pd.DataFrame, day: pd.Timestamp) -> Dict[str, str]:
    def norm(x) -> str:
        return str(x or "").strip().upper()

    out = {"OpeningTrend": "", "OpenLocation": "", "PrevDayContext": ""}
    if master is None or master.empty or "Date" not in master.columns:
        return out
    mdates = pd.to_datetime(master["Date"], errors="coerce").dt.normalize()
    row = master.loc[mdates == day]
    if row.empty:
        return out
    r = row.iloc[0]
    out["OpeningTrend"] = norm(r.get("OpeningTrend", ""))
    out["OpenLocation"] = norm(r.get("OpenLocation", ""))
    out["PrevDayContext"] = norm(r.get("PrevDayContext", ""))
    return out


def _prev_day_ohlc(tm5: pd.DataFrame, day: pd.Timestamp) -> Optional[Dict[str, float]]:
    # robust-enough: use previous available trading day in this symbol's file
    dcol = pd.to_datetime(tm5["__date"], errors="coerce").dt.normalize()
    uniq = sorted(set(dcol.dropna().unique()))
    prev = [d for d in uniq if d < day]
    if not prev:
        return None
    dprev = prev[-1]
    dfp = tm5.loc[dcol == dprev]
    if dfp.empty:
        return None
    return {
        "high": float(dfp["High"].max()),
        "low": float(dfp["Low"].min()),
    }


def _is_close(a: float, b: float, entry_px: float, orb_rng: float, close_pct: float, close_fr_orb: float) -> bool:
    thr = float("inf")
    parts: List[float] = []
    if np.isfinite(entry_px) and entry_px > 0:
        parts.append(entry_px * float(close_pct))
    if np.isfinite(orb_rng):
        parts.append(abs(orb_rng) * float(close_fr_orb))
    if parts:
        thr = min(parts)
    return (np.isfinite(a) and np.isfinite(b)) and abs(a - b) <= thr


def _earliest_touch_times(win: pd.DataFrame, long_side: bool, stop: float, t1: float, t2: float) -> Dict[str, Optional[pd.Timestamp]]:
    if win is None or win.empty:
        return {"stop": None, "t1": None, "t2": None}

    hi = win["High"].to_numpy(dtype=float)
    lo = win["Low"].to_numpy(dtype=float)
    ts = win["DateTime"].to_numpy()

    if long_side:
        cond_stop = lo <= stop
        cond_t1 = hi >= t1
        cond_t2 = hi >= t2
    else:
        cond_stop = hi >= stop
        cond_t1 = lo <= t1
        cond_t2 = lo <= t2

    i_stop = int(np.argmax(cond_stop)) if np.any(cond_stop) else -1
    i_t1 = int(np.argmax(cond_t1)) if np.any(cond_t1) else -1
    i_t2 = int(np.argmax(cond_t2)) if np.any(cond_t2) else -1

    return {
        "stop": pd.Timestamp(ts[i_stop]) if i_stop >= 0 else None,
        "t1": pd.Timestamp(ts[i_t1]) if i_t1 >= 0 else None,
        "t2": pd.Timestamp(ts[i_t2]) if i_t2 >= 0 else None,
    }


def plan_lookahead(days: int) -> Path:
    """
    For each symbol/day:
      - build plan normally
      - build plan again after *corrupting* data AFTER 09:40 for that day
    If plan changes => it was reading future bars => LEAK.
    """
    from probedge.infra.settings import SETTINGS
    from probedge.decision.plan_core import build_parity_plan
    from probedge.infra import loaders as loaders_mod

    symbols = [s.upper() for s in (getattr(SETTINGS, "symbols", []) or [])]
    if not symbols:
        raise SystemExit("No symbols found in SETTINGS.symbols")

    days_list = _common_days_last_n(symbols, days)

    # Save original function
    orig_read = loaders_mod.read_tm5_csv

    out_rows = []

    try:
        for day_str in days_list:
            day = pd.to_datetime(day_str).normalize()

            def patched_read_tm5_csv(path: str) -> pd.DataFrame:
                df = orig_read(path)
                if "DateTime" not in df.columns:
                    return df
                if "_mins" not in df.columns:
                    df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute
                d = df["DateTime"].dt.normalize()
                # corrupt all bars AFTER 09:40 for this day
                m = (d == day) & (df["_mins"] > IST_T0)
                if m.any():
                    # multiply OHLC to create a strong "future" perturbation
                    for c in ("Open", "High", "Low", "Close"):
                        if c in df.columns:
                            df.loc[m, c] = df.loc[m, c].astype(float) * 1.37
                return df

            # monkeypatch globally for this day
            loaders_mod.read_tm5_csv = patched_read_tm5_csv

            for sym in symbols:
                # normal plan (with original reader)
                loaders_mod.read_tm5_csv = orig_read
                p0 = build_parity_plan(sym, day_str=day_str)

                # patched plan (with corrupted future bars)
                loaders_mod.read_tm5_csv = patched_read_tm5_csv
                p1 = build_parity_plan(sym, day_str=day_str)

                # compare only decision-critical fields
                keys = ("pick", "entry", "stop", "risk_per_share", "target1", "target2", "confidence%", "reason")
                diff = {k: (p0.get(k), p1.get(k)) for k in keys if p0.get(k) != p1.get(k)}
                if diff:
                    out_rows.append({
                        "day": day_str,
                        "symbol": sym,
                        "diff": diff,
                        "plan_normal": {k: p0.get(k) for k in keys},
                        "plan_patched": {k: p1.get(k) for k in keys},
                    })

    finally:
        loaders_mod.read_tm5_csv = orig_read

    out_dir = Path("data/leakage")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / "plan_lookahead_report.csv"
    if out_rows:
        df = pd.DataFrame(out_rows)
        df.to_csv(out_fp, index=False)
    else:
        # write an empty but valid file
        pd.DataFrame(columns=["day","symbol","diff","plan_normal","plan_patched"]).to_csv(out_fp, index=False)

    return out_fp


def tie_bias(days: int) -> Path:
    """
    Detect 5-min intrabar ambiguity:
      if stop & T1 touch in the SAME 5-min candle, the current exec_adapter assumes T1 wins on a tie.
    This report lists those trades and quantifies how much PnL can flip if you assume STOP wins on tie.
    """
    from probedge.infra.settings import SETTINGS
    from probedge.infra.constants import CLOSE_PCT, CLOSE_FR_ORB
    from probedge.decision.freq_pick import freq_pick

    symbols = [s.upper() for s in (getattr(SETTINGS, "symbols", []) or [])]
    if not symbols:
        raise SystemExit("No symbols found in SETTINGS.symbols")

    days_list = _common_days_last_n(symbols, days)
    day_ts = [pd.to_datetime(d).normalize() for d in days_list]

    daily_risk = _effective_daily_risk_rs(SETTINGS)

    out_rows = []
    total_trades = 0

    # cache data per symbol
    tm5_cache: Dict[str, pd.DataFrame] = {}
    master_cache: Dict[str, pd.DataFrame] = {}
    daymap_cache: Dict[str, Dict[pd.Timestamp, pd.DataFrame]] = {}

    for sym in symbols:
        tm5 = _read_intraday(sym)
        master = _read_master(sym)
        tm5_cache[sym] = tm5
        master_cache[sym] = master

        # map day -> df_day
        dnorm = pd.to_datetime(tm5["__date"], errors="coerce").dt.normalize()
        mp: Dict[pd.Timestamp, pd.DataFrame] = {}
        for d in pd.unique(dnorm.dropna()):
            mp[pd.Timestamp(d).normalize()] = tm5.loc[dnorm == d].copy()
        daymap_cache[sym] = mp

    for day in day_ts:
        for sym in symbols:
            master = master_cache[sym]
            tm5 = tm5_cache[sym]
            df_day = daymap_cache[sym].get(day)

            if df_day is None or df_day.empty:
                continue

            tags = _get_master_tags_for_day(master, day)
            pick, conf_pct, reason, level, stats = freq_pick(day, master, tags_override=tags)
            pick = str(pick or "").upper()

            if pick not in ("BULL", "BEAR"):
                continue

            # Entry window 09:40->15:05, take first Open at 09:40
            w09 = df_day[(df_day["_mins"] >= IST_T0) & (df_day["_mins"] <= IST_T1)]
            if w09.empty:
                continue
            entry = float(w09["Open"].iloc[0])

            # ORB 09:15->09:35
            w_orb = df_day[(df_day["_mins"] >= ORB0) & (df_day["_mins"] <= ORB1)]
            if w_orb.empty:
                continue

            orb_h = float(w_orb["High"].max())
            orb_l = float(w_orb["Low"].min())
            rng = max(0.0, orb_h - orb_l)
            dbl_h, dbl_l = (orb_h + rng, orb_l - rng)
            orb_rng = (orb_h - orb_l) if (np.isfinite(orb_h) and np.isfinite(orb_l)) else np.nan

            prev = _prev_day_ohlc(tm5, day)
            prev_h = float(prev["high"]) if prev else np.nan
            prev_l = float(prev["low"]) if prev else np.nan

            ot = (tags.get("OpeningTrend") or "TR").upper()
            long_side = (pick == "BULL")

            # SL logic (matches plan_core)
            if ot == "BULL" and pick == "BULL":
                stop = prev_l if (np.isfinite(prev_l) and _is_close(orb_l, prev_l, entry, orb_rng, CLOSE_PCT, CLOSE_FR_ORB)) else orb_l
            elif ot == "BULL" and pick == "BEAR":
                stop = dbl_h
            elif ot == "BEAR" and pick == "BEAR":
                stop = prev_h if (np.isfinite(prev_h) and _is_close(orb_h, prev_h, entry, orb_rng, CLOSE_PCT, CLOSE_FR_ORB)) else orb_h
            elif ot == "BEAR" and pick == "BULL":
                stop = dbl_l
            elif ot == "TR" and pick == "BEAR":
                stop = dbl_h
            elif ot == "TR" and pick == "BULL":
                stop = dbl_l
            else:
                stop = dbl_l if long_side else dbl_h

            rps = (entry - stop) if long_side else (stop - entry)
            if (not np.isfinite(rps)) or rps <= 0:
                continue

            qty = int(np.floor(float(daily_risk) / float(rps)))
            if qty <= 0:
                continue

            t1 = entry + rps if long_side else entry - rps
            t2 = entry + 2 * rps if long_side else entry - 2 * rps

            touches = _earliest_touch_times(w09, long_side, float(stop), float(t1), float(t2))
            ts_stop, ts_t1, ts_t2 = touches["stop"], touches["t1"], touches["t2"]

            total_trades += 1

            tie_r1 = (ts_stop is not None) and (ts_t1 is not None) and (ts_stop == ts_t1)
            tie_r2 = (ts_stop is not None) and (ts_t2 is not None) and (ts_stop == ts_t2)
            if not (tie_r1 or tie_r2):
                continue

            # optimistic (current exec_adapter): target wins on tie
            pnl_r1 = qty * (t1 - entry) if long_side else qty * (entry - t1)
            pnl_r2 = qty * (t2 - entry) if long_side else qty * (entry - t2)
            sl_pnl = -qty * rps

            alt_pnl_r1 = sl_pnl if tie_r1 else pnl_r1
            alt_pnl_r2 = sl_pnl if tie_r2 else pnl_r2

            out_rows.append({
                "day": str(day.date()),
                "symbol": sym,
                "pick": pick,
                "ot": ot,
                "qty": qty,
                "entry": entry,
                "stop": float(stop),
                "t1": float(t1),
                "t2": float(t2),
                "ts_stop": str(ts_stop) if ts_stop is not None else "",
                "ts_t1": str(ts_t1) if ts_t1 is not None else "",
                "ts_t2": str(ts_t2) if ts_t2 is not None else "",
                "tie_r1": bool(tie_r1),
                "tie_r2": bool(tie_r2),
                "optimistic_pnl_r1": float(pnl_r1),
                "worstcase_pnl_r1": float(alt_pnl_r1),
                "delta_r1": float(alt_pnl_r1 - pnl_r1),
                "optimistic_pnl_r2": float(pnl_r2),
                "worstcase_pnl_r2": float(alt_pnl_r2),
                "delta_r2": float(alt_pnl_r2 - pnl_r2),
            })

    out_dir = Path("data/leakage")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / "tie_bias_report.csv"

    df = pd.DataFrame(out_rows).sort_values(["day","symbol"]).reset_index(drop=True)
    df.to_csv(out_fp, index=False)

    # print a quick summary to stdout
    print(f"Trades analysed: {total_trades}")
    print(f"Tie events found: {len(df)}")
    if len(df):
        print("Total delta (R1) if STOP wins ties:", float(df["delta_r1"].sum()))
        print("Total delta (R2) if STOP wins ties:", float(df["delta_r2"].sum()))
        print("Worst 10 by |delta_r2|:")
        print(df.assign(abs_delta=df["delta_r2"].abs()).sort_values("abs_delta", ascending=False).head(10)[
            ["day","symbol","pick","qty","delta_r1","delta_r2","ts_stop","ts_t1","ts_t2"]
        ].to_string(index=False))

    return out_fp


def main() -> None:
    _ensure_repo_root()

    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap1 = sub.add_parser("plan_lookahead", help="Detect lookahead in plan building by corrupting future bars.")
    ap1.add_argument("--days", type=int, default=60, help="How many last COMMON days across all symbols to test.")

    ap2 = sub.add_parser("tie_bias", help="Detect 5-min intrabar tie bias (optimistic target-on-tie).")
    ap2.add_argument("--days", type=int, default=120, help="How many last COMMON days across all symbols to test.")

    args = ap.parse_args()

    if args.cmd == "plan_lookahead":
        out = plan_lookahead(days=int(args.days))
        print(f"✅ Wrote: {out}")
    elif args.cmd == "tie_bias":
        out = tie_bias(days=int(args.days))
        print(f"✅ Wrote: {out}")
    else:
        raise SystemExit("Unknown command")

if __name__ == "__main__":
    main()
