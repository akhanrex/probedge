
from __future__ import annotations
import os, time
import pandas as pd
from pathlib import Path
from datetime import datetime as _dt
from probedge.infra.settings import SETTINGS
from probedge.storage.atomic_json import AtomicJSON
from probedge.decision.picker_batchv1 import read_tm5, decide_for_day
from probedge.decision.sl import compute_stop

HEARTBEAT_SEC = 1.0
RISK_RS = float(os.getenv("RISK_RS", "10000"))

def _fmt_path(template: str, sym: str) -> str:
    return (template or "").format(sym=sym, SYM=sym, symbol=sym)

def _load_master(path: str) -> pd.DataFrame:
    m = pd.read_csv(path)
    m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.normalize()
    for col in ["OpeningTrend","OpenLocation","PrevDayContext","FirstCandleType","RangeStatus","Result"]:
        if col in m.columns:
            m[col] = m[col].astype(str).str.strip().str.upper().replace({"NAN": ""})
    return m

def _today_norm():
    return pd.to_datetime(_dt.now().date())

def process_once(state_path: str):
    aj = AtomicJSON(state_path)
    state = aj.read(default={})
    now_iso = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
    state["heartbeat"] = now_iso

    ctrl = state.get("control") or {}
    action = (ctrl.get("action") or "").lower()
    sym = (ctrl.get("symbol") or "").strip().upper()
    strategy = (ctrl.get("strategy") or "batch_v1").strip().lower()

    if action == "arm" and sym and strategy == "batch_v1":
        try:
            tm5_path = _fmt_path(SETTINGS.paths.intraday, sym)
            master_path = _fmt_path(SETTINGS.paths.masters, sym)
            df_tm5 = read_tm5(tm5_path)
            m = _load_master(master_path)
            day = _today_norm()

            info = decide_for_day(df_tm5, m, day)
            if not info:
                state["plan"] = {"symbol": sym, "status": "no-data-for-today"}
            else:
                ot = info["OpeningTrend"]; pick = info["Pick"]
                orb_h, orb_l = info["ORB_H"], info["ORB_L"]
                prev_h, prev_l = info["Prev_H"], info["Prev_L"]
                entry = info["Entry"]
                stop = compute_stop(ot, pick, orb_h, orb_l, prev_h, prev_l, entry)
                long_side = (pick == "BULL")
                risk_per_sh = (entry - stop) if long_side else (stop - entry)
                qty = int(max(0, (RISK_RS // risk_per_sh))) if risk_per_sh > 0 else 0
                t1 = entry + risk_per_sh if long_side else entry - risk_per_sh
                t2 = entry + 2*risk_per_sh if long_side else entry - 2*risk_per_sh
                state["plan"] = {
                    "symbol": sym,
                    "date": str(info["Date"].date()),
                    "OpeningTrend": ot,
                    "Pick": pick,
                    "Confidence%": info["Confidence%"],
                    "Reason": info["Reason"],
                    "Entry": round(entry, 4),
                    "Stop": round(float(stop), 4),
                    "RiskPerShare": round(float(risk_per_sh), 4) if risk_per_sh == risk_per_sh else None,
                    "Qty": qty,
                    "Target1": round(float(t1), 4) if t1 == t1 else None,
                    "Target2": round(float(t2), 4) if t2 == t2 else None,
                    "ORB_H": round(float(orb_h), 4),
                    "ORB_L": round(float(orb_l), 4),
                    "Prev_H": round(float(prev_h), 4) if prev_h == prev_h else None,
                    "Prev_L": round(float(prev_l), 4) if prev_l == prev_l else None,
                }
            # mark processed
            state.setdefault("control", {})
            state["control"]["last_processed"] = now_iso
            state["control"]["status"] = "processed"
        except Exception as e:
            state.setdefault("errors", []).append(f"{now_iso} {sym} {e}")
            state.setdefault("control", {})
            state["control"]["status"] = "error"
    aj.write(state)

def main():
    state_path = SETTINGS.paths.state or "live_state.json"
    Path(state_path).touch(exist_ok=True)
    while True:
        process_once(state_path)
        time.sleep(HEARTBEAT_SEC)

if __name__ == "__main__":
    main()
