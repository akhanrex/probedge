#!/usr/bin/env python
import argparse
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

from probedge.storage.resolver import locate_for_read
from probedge.infra.loaders import read_tm5_csv
from probedge.core import classifiers as C
from probedge.decision.plan_core import build_parity_plan


def _load_tm5(sym: str) -> pd.DataFrame:
    p = locate_for_read("intraday", sym)
    if not p.exists():
        raise FileNotFoundError(f"TM5 not found for {sym}: {p}")
    df = read_tm5_csv(p)
    # Ensure Date column
    if "Date" not in df.columns:
        df["Date"] = df["DateTime"].dt.date
    else:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df


def _load_master(sym: str) -> pd.DataFrame:
    p = locate_for_read("masters", sym)
    if not p.exists():
        raise FileNotFoundError(f"MASTER not found for {sym}: {p}")
    df = pd.read_csv(p)
    if "Date" not in df.columns:
        raise ValueError(f"MASTER for {sym} has no Date column: {p}")
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df


def _classifier_tags(tm5: pd.DataFrame, day_date) -> Dict[str, Any]:
    day_ts = pd.Timestamp(day_date)

    prev_ohlc = C.prev_trading_day_ohlc(tm5, day_ts)
    pdc = C.compute_prevdaycontext_robust(prev_ohlc)

    ol = C.compute_openlocation_from_df(tm5, day_ts, prev_ohlc)
    ot = C.compute_openingtrend_robust(tm5, day_ts)

    return {
        "PrevDayContext": pdc,
        "OpenLocation": ol,
        "OpeningTrend": ot,
        "prev_ohlc": prev_ohlc,
    }


def _fmt(d: Optional[Dict[str, Any]]) -> str:
    if not d:
        return "None"
    return ", ".join(f"{k}={v}" for k, v in d.items())


def main():
    ap = argparse.ArgumentParser(description="Debug tags + plan parity for one symbol/day")
    ap.add_argument("--symbol", "-s", required=True, help="Symbol, e.g. SBIN")
    ap.add_argument("--day", "-d", required=True, help="Day YYYY-MM-DD, e.g. 2025-12-01")
    args = ap.parse_args()

    sym = args.symbol.upper()
    day_str = args.day
    day_date = pd.to_datetime(day_str).date()

    print(f"=== DEBUG {sym} {day_str} ===")

    # ---------- TM5 + day slice ----------
    tm5 = _load_tm5(sym)
    df_day = tm5[tm5["Date"] == day_date].copy()

    if df_day.empty:
        print(f"[TM5] No intraday rows for {sym} on {day_str}")
    else:
        print(f"[TM5] Rows for {sym} on {day_str}: {len(df_day)}")
        print(df_day[["DateTime", "Open", "High", "Low", "Close"]].head(6).to_string(index=False))
        print("... ORB window (09:15–09:35 approx):")
        if "_mins" in df_day.columns:
            orb = df_day[(df_day["_mins"] >= 9*60+15) & (df_day["_mins"] <= 9*60+35)]
        else:
            # Derive minutes-from-midnight if not present
            dt = pd.to_datetime(df_day["DateTime"])
            mins = dt.dt.hour * 60 + dt.dt.minute
            orb = df_day[(mins >= 9*60+15) & (mins <= 9*60+35)]
        print(orb[["DateTime", "Open", "High", "Low", "Close"]].to_string(index=False))

    # ---------- MASTER row ----------
    print("\n=== MASTER row ===")
    try:
        master = _load_master(sym)
        row = master[master["Date"] == day_date]
        if row.empty:
            print(f"[MASTER] No row for {sym} on {day_str}")
        else:
            r = row.iloc[0].to_dict()
            print(f"[MASTER] {r}")
    except Exception as e:
        print(f"[MASTER] ERROR: {e}")
        master = None

    # ---------- Classifier tags directly from TM5 ----------
    print("\n=== Classifier tags (from probedge.core.classifiers) ===")
    try:
        class_tags = _classifier_tags(tm5, day_date)
        print(f"[C] PDC={class_tags['PrevDayContext']}, "
              f"OL={class_tags['OpenLocation']}, "
              f"OT={class_tags['OpeningTrend']}")
        print(f"[C] prev_ohlc: {_fmt(class_tags['prev_ohlc'])}")
    except Exception as e:
        print(f"[C] ERROR computing classifier tags: {e}")
        class_tags = None

    # ---------- Planner output ----------
    print("\n=== Planner (build_parity_plan) ===")
    try:
        plan = build_parity_plan(sym, day_str)
        print(f"[PLAN] skip={plan.get('skip')} pick={plan.get('pick')} "
              f"conf={plan.get('confidence%')}% reason={plan.get('reason')}")
        print(f"[PLAN] tags={plan.get('tags')}")
        print(f"[PLAN] entry={plan.get('entry')} stop={plan.get('stop')} "
              f"t1={plan.get('target1')} t2={plan.get('target2')} qty={plan.get('qty')}")
        print(f"[PLAN] per_trade_risk_rs_used={plan.get('per_trade_risk_rs_used')}")
    except Exception as e:
        print(f"[PLAN] ERROR computing plan: {e}")
        plan = None

    print("\n=== SUMMARY ===")
    print("Compare:")
    print(" - MASTER tags vs classifier tags vs plan['tags']")
    print(" - ORB window vs what you expect for OT")
    print(" - prev_ohlc vs what you expect for PDC/OL")
    print("If MASTER != classifier != plan, we know exactly which link is broken.")


if __name__ == "__main__":
    main()
