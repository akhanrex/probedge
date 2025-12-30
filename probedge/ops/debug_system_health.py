"""
ops/debug_system_health.py

End-to-end health check for Probedge:
- Settings / config
- Data (MASTER + TM5) for given days
- Planner (build_parity_plan)
- Optional API check for /api/health and /api/state

Usage (from repo root, venv activated):

  python ops/debug_system_health.py --days 2025-03-10 2025-06-10 2025-08-10 2025-10-10 2025-11-10 --risk 10000 --check-api

"""

import argparse
import datetime as dt
import json
import os
import sys
from typing import List, Dict, Any

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.storage import masters as masters_store
from probedge.decision.plan_core import build_parity_plan

try:
    # Only stdlib, no extra deps
    from urllib.request import urlopen
    from urllib.error import URLError
except ImportError:  # extremely old Python
    urlopen = None
    URLError = Exception


def _parse_days(raw_days: List[str]) -> List[str]:
    days: List[str] = []
    for d in raw_days:
        try:
            d0 = dt.datetime.fromisoformat(d).date()
            days.append(d0.isoformat())
        except Exception:
            print(f"[WARN] Skipping invalid day string: {d}", file=sys.stderr)
    return days


def print_header():
    print("=" * 80)
    print("Probedge – System Health Debug")
    print("=" * 80)
    print(f"MODE        : {SETTINGS.mode}")
    print(f"DATA_DIR    : {SETTINGS.data_dir}")
    print(f"SYMBOLS     : {', '.join(SETTINGS.symbols)}")
    print(f"STATE PATH  : {SETTINGS.paths.state}")
    print(f"MASTERS PATH: {SETTINGS.paths.masters}")
    print(f"INTRADAY    : {SETTINGS.paths.intraday}")
    print("-" * 80)


def check_data_for_symbol_day(sym: str, day: str) -> Dict[str, Any]:
    """Return a dict summarising MASTER/TM5/PLAN status for (sym, day)."""
    from probedge.storage.tm5 import _path_for as tm5_path_for  # local import

    result: Dict[str, Any] = {
        "symbol": sym,
        "day": day,
        "master_status": "UNKNOWN",
        "master_result": None,
        "tm5_status": "UNKNOWN",
        "tm5_bars": 0,
        "tm5_time_window": None,
        "plan_pick": None,
        "plan_conf": None,
        "plan_skip": None,
        "plan_error": None,
    }

    # --- MASTER ---
    mdf = masters_store.read(sym)
    if mdf.empty:
        result["master_status"] = "MISSING"
    else:
        try:
            dates = pd.to_datetime(mdf["Date"], errors="coerce").dt.date
            mask = dates == dt.date.fromisoformat(day)
            if not mask.any():
                result["master_status"] = "NO_ROW_FOR_DAY"
            else:
                row = mdf.loc[mask].iloc[0]
                result["master_status"] = "OK"
                result["master_result"] = str(row.get("Result", "") or "")
        except Exception as e:
            result["master_status"] = f"ERROR: {e}"

    # --- TM5 ---
    try:
        p_tm5 = tm5_path_for(sym)
        if not os.path.exists(p_tm5):
            result["tm5_status"] = "MISSING_FILE"
        else:
            tdf = pd.read_csv(p_tm5)
            # Try common column names: DateTime or date/Date
            dt_col = None
            for cand in ("DateTime", "date", "Date"):
                if cand in tdf.columns:
                    dt_col = cand
                    break
            if dt_col is None:
                result["tm5_status"] = "NO_DATETIME_COL"
            else:
                tdf["_dt"] = pd.to_datetime(tdf[dt_col], errors="coerce")
                tdf["_date"] = tdf["_dt"].dt.date
                d0 = dt.date.fromisoformat(day)
                ddf = tdf.loc[tdf["_date"] == d0]
                if ddf.empty:
                    result["tm5_status"] = "NO_BARS_FOR_DAY"
                else:
                    result["tm5_status"] = "OK"
                    result["tm5_bars"] = int(ddf.shape[0])
                    tmin = ddf["_dt"].min()
                    tmax = ddf["_dt"].max()
                    result["tm5_time_window"] = f"{tmin.time()} → {tmax.time()}"
    except Exception as e:
        result["tm5_status"] = f"ERROR: {e}"

    # --- PLAN ---
    try:
        plan = build_parity_plan(sym, day)
        result["plan_pick"] = plan.get("pick")
        result["plan_conf"] = plan.get("confidence%", 0)
        result["plan_skip"] = plan.get("skip")
        result["plan_error"] = plan.get("error")
    except Exception as e:
        result["plan_pick"] = None
        result["plan_conf"] = None
        result["plan_skip"] = None
        result["plan_error"] = f"EXCEPTION: {e}"

    return result


def print_day_summary(day: str, risk_rs: int):
    print("\n" + "#" * 80)
    print(f"# DAY {day} – risk={risk_rs}")
    print("#" * 80)

    rows: List[Dict[str, Any]] = []
    for sym in SETTINGS.symbols:
        rows.append(check_data_for_symbol_day(sym, day))

    # Pretty-print per symbol
    for r in rows:
        print(
            f"{r['symbol']:10s} | MASTER={r['master_status']:<16s} "
            f"Result={str(r['master_result'] or '-'):8s} | "
            f"TM5={r['tm5_status']:<14s} bars={r['tm5_bars']:3d} "
            f"({r['tm5_time_window'] or '-'}) | "
            f"PLAN={str(r['plan_pick'] or '-'):<8s} conf={str(r['plan_conf'] or '-'):>3s} "
            f"skip={str(r['plan_skip'] or '-'):20s} err={str(r['plan_error'] or '-'):s}"
        )


def check_api_for_day(day: str, risk_rs: int):
    if urlopen is None:
        print("[WARN] urllib not available; skipping API checks")
        return

    base = "http://127.0.0.1:9002"
    print("\n" + "-" * 80)
    print(f"API checks for day={day}, risk={risk_rs} (server must already be running)")
    print("-" * 80)

    # /api/health
    try:
        with urlopen(f"{base}/api/health", timeout=5) as r:
            body = r.read().decode("utf-8", errors="ignore")
            print(f"/api/health -> {r.status}, {len(body)} bytes")
            try:
                js = json.loads(body)
                print("  Parsed JSON:", js)
            except Exception:
                print("  (not JSON)")
    except URLError as e:
        print(f"/api/health FAILED: {e}")

    # /api/state
    try:
        url = f"{base}/api/state?day={day}&risk={risk_rs}"
        with urlopen(url, timeout=10) as r:
            body = r.read().decode("utf-8", errors="ignore")
            print(f"/api/state -> {r.status}, {len(body)} bytes")
            try:
                js = json.loads(body)
                print("  Parsed JSON keys:", list(js.keys()))
                pp = js.get("portfolio_plan") or js.get("plan") or {}
                plans = pp.get("plans") or []
                print(f"  Plans in response: {len(plans)}")
            except Exception:
                print("  (not JSON)")
    except URLError as e:
        print(f"/api/state FAILED: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--days",
        nargs="+",
        help=(
            "List of days YYYY-MM-DD to debug. "
            "Example: --days 2025-03-10 2025-06-10 2025-08-10 2025-10-10 2025-11-10"
        ),
    )
    ap.add_argument(
        "--risk",
        type=int,
        default=10000,
        help="Daily risk for context (not used directly in planner call).",
    )
    ap.add_argument(
        "--check-api",
        action="store_true",
        help="Also hit /api/health and /api/state on localhost:9002.",
    )
    args = ap.parse_args()

    raw_days = args.days or [
        # Default sample months for 2025
        "2025-03-10",
        "2025-06-10",
        "2025-08-10",
        "2025-10-10",
        "2025-11-10",
    ]
    days = _parse_days(raw_days)
    if not days:
        print("[FATAL] No valid days to check", file=sys.stderr)
        sys.exit(1)

    print_header()
    for day in days:
        print_day_summary(day, args.risk)
        if args.check_api:
            check_api_for_day(day, args.risk)


if __name__ == "__main__":
    main()
