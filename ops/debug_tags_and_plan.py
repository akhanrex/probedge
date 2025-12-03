import argparse
import pandas as pd
from pathlib import Path

from probedge.storage.resolver import locate_for_read
from probedge.infra.loaders import read_tm5_csv
from probedge.core import classifiers as C
from probedge.decision.plan_core import build_parity_plan


def debug_symbol_day(symbol: str, day_str: str) -> None:
    sym = symbol.upper()
    day_ts = pd.to_datetime(day_str)
    day_date = day_ts.date()

    print(f"=== DEBUG {sym} {day_date} ===")

    # --- Load intraday (TM5) ---
    p_tm5 = locate_for_read("intraday", sym)
    if not p_tm5.exists():
        print(f"[TM5] intraday file not found for {sym}: {p_tm5}")
        return

    tm5 = read_tm5_csv(p_tm5)

    if "DateTime" not in tm5.columns:
        print("[TM5] no DateTime column; cannot proceed")
        return

    tm5["DateTime"] = pd.to_datetime(tm5["DateTime"], errors="coerce")
    tm5 = tm5.dropna(subset=["DateTime"])
    tm5["Date"] = tm5["DateTime"].dt.date

    # Slice day
    df_day = tm5[tm5["Date"] == day_date].copy()
    print(f"[TM5] Rows for {sym} on {day_date}: {len(df_day)}")
    if df_day.empty:
        print("[TM5] no intraday rows for this day; stop")
        return

    print(df_day[["DateTime", "Open", "High", "Low", "Close"]].head())
    print("... ORB window (09:15–09:35 approx):")

    # ORB window from intraday
    day_idx = df_day.set_index("DateTime").sort_index()
    try:
        orb = day_idx.between_time("09:15", "09:35")
    except TypeError:
        # if DateTime is tz-aware index, between_time still works; keep as safety
        orb = day_idx

    print(orb[["Open", "High", "Low", "Close"]].loc[:].head(25))

    # --- Prev trading day OHLC from intraday ---
    prev_days = sorted(d for d in tm5["Date"].unique() if d < day_date)
    if prev_days:
        prev_date = prev_days[-1]
        prev_df = tm5[tm5["Date"] == prev_date].copy()
        prev_open = float(prev_df["Open"].iloc[0])
        prev_high = float(prev_df["High"].max())
        prev_low = float(prev_df["Low"].min())
        prev_close = float(prev_df["Close"].iloc[-1])
        print(f"\n[PREV] nearest prior trading day in TM5: {prev_date}")
        print(
            f"[PREV] OHLC (computed from intraday): "
            f"O={prev_open}, H={prev_high}, L={prev_low}, C={prev_close}"
        )
    else:
        prev_date = None
        print("\n[PREV] no prior trading day found in TM5")

    # --- Classifier tags directly from core.classifiers ---
    print("\n=== Classifier tags (from probedge.core.classifiers) ===")
    try:
        prev_ohlc = C.prev_trading_day_ohlc(tm5, day_ts)
        print(f"[C] prev_trading_day_ohlc -> {prev_ohlc}")
        pdc = C.compute_prevdaycontext_robust(prev_ohlc)
        ol = C.compute_openlocation_from_df(tm5, day_ts, prev_ohlc)
        ot = C.compute_openingtrend_robust(tm5, day_ts)
        print(f"[C] PDC={pdc}, OL={ol}, OT={ot}")
    except Exception as e:
        print(f"[C] ERROR computing classifier tags: {e}")

    # --- MASTER row for this date ---
    from probedge.storage.resolver import locate_for_read as _loc_master
    import pandas as _pd

    p_master = _loc_master("masters", sym)
    master_row = None
    print("\n=== MASTER row ===")
    if not p_master.exists():
        print(f"[MASTER] file not found for {sym}: {p_master}")
    else:
        m = _pd.read_csv(p_master)
        if "Date" not in m.columns:
            print("[MASTER] no Date column; cannot match day")
        else:
            m["Date"] = _pd.to_datetime(m["Date"], errors="coerce").dt.date
            mm = m[m["Date"] == day_date]
            if mm.empty:
                print(f"[MASTER] no row for {day_date}")
            else:
                master_row = mm.iloc[0].to_dict()
                print(f"[MASTER] {master_row}")

    # --- Planner view ---
    print("\n=== Planner (build_parity_plan) ===")
    try:
        plan = build_parity_plan(sym, day_str=day_str)
        skip = plan.get("skip")
        print(
            f"[PLAN] skip={skip} pick={plan.get('pick')} "
            f"conf={plan.get('confidence%')}% reason={plan.get('reason')}"
        )
        print(f"[PLAN] tags={plan.get('tags')}")
        print(
            f"[PLAN] entry={plan.get('entry')} stop={plan.get('stop')} "
            f"t1={plan.get('target1')} t2={plan.get('target2')} qty={plan.get('qty')}"
        )
        print(f"[PLAN] per_trade_risk_rs_used={plan.get('per_trade_risk_rs_used')}")
    except Exception as e:
        print(f"[PLAN] ERROR building plan: {e}")

    print("\n=== SUMMARY ===")
    print("Compare:")
    print(" - PREV-day OHLC vs what you expect from chart")
    print(" - Classifier tags (C.*) vs MASTER OpeningTrend/OpenLocation/PrevDayContext")
    print(" - MASTER tags vs plan['tags']")
    print("If PREV-day OHLC itself looks wrong, this is a data issue;")
    print("if OHLC is correct but tags feel wrong, it's a classifier-rule issue.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="e.g. SBIN")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    debug_symbol_day(args.symbol, args.day)


if __name__ == "__main__":
    main()
