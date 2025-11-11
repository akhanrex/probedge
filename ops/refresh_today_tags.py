
# signature-agnostic, latest-date fallback refresher (v2025-11-11a)
import json
from pathlib import Path
from datetime import datetime, timezone
import importlib
import pandas as pd

from probedge.decision.tags_engine import _read_intraday

C = importlib.import_module("probedge.core.classifiers")
TODAY = pd.Timestamp.now(tz="Asia/Kolkata").normalize()

def unpack_prev(prev):
    O=H=L=C=None
    if isinstance(prev, (tuple, list)):
        if len(prev) == 4: O,H,L,C = prev
        elif len(prev) == 3: H,L,C = prev
    if H is None and hasattr(prev, "get"):
        H = prev.get("High") or prev.get("H") or prev.get("prev_H") or H
        L = prev.get("Low")  or prev.get("L") or prev.get("prev_L") or L
        C = prev.get("Close") or prev.get("C") or prev.get("prev_C") or C
        O = prev.get("Open") or prev.get("O") or prev.get("prev_O") or O
    if H is None and hasattr(prev, "__getitem__"):
        try:
            H = prev["High"]  if "High"  in prev else H
            L = prev["Low"]   if "Low"   in prev else L
            C = prev["Close"] if "Close" in prev else C
            O = prev["Open"]  if "Open"  in prev else O
        except Exception:
            pass
    return O,H,L,C

def safe_pdc(prev):
    O,H,L,C = unpack_prev(prev)
    try:
        return str(C.compute_prevdaycontext_robust(prev)).upper()
    except Exception:
        pass
    try:
        if H is not None and L is not None and C is not None:
            return str(C.compute_prevdaycontext_robust(H, L, C)).upper()
    except Exception:
        pass
    return "TR"

def safe_ol(df, d, prev):
    O,H,L,C = unpack_prev(prev)
    try:
        return str(C.compute_openlocation_from_df(df, d, prev)).upper()
    except Exception:
        pass
    try:
        args = [df, d]
        if O is not None: args.append(O)
        if H is not None: args.append(H)
        if L is not None: args.append(L)
        if C is not None: args.append(C)
        return str(C.compute_openlocation_from_df(*args)).upper()
    except Exception:
        pass
    return "OOH"

def safe_ot(df, d):
    try:
        return str(C.compute_openingtrend_robust(df, d)).upper()
    except Exception:
        return "TR"

def list_symbols_from_intraday():
    intradir = Path("data/intraday")
    return sorted([p.name.replace("_5minute.csv","")
                   for p in intradir.glob("*_5minute.csv") if p.is_file()])

def choose_date(df):
    ds = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    if (ds == TODAY).any():
        return TODAY
    return ds.max()

def safe_compute_tags(symbol: str):
    try:
        df = _read_intraday(symbol)
        if df is None or df.empty:
            return {"PDC":"GAP","OL":"GAP","OT":"GAP","_note":"no intraday"}
        d = choose_date(df)
        if pd.isna(d):
            return {"PDC":"GAP","OL":"GAP","OT":"GAP","_note":"no dates"}
        prev = C.prev_trading_day_ohlc(df, d)
        return {"PDC": safe_pdc(prev), "OL":  safe_ol(df, d, prev), "OT":  safe_ot(df, d)}
    except Exception as e:
        return {"PDC":"ERR","OL":"ERR","OT":"ERR","_err":str(e)}

def main():
    syms = list_symbols_from_intraday()
    tags = {s: safe_compute_tags(s) for s in syms}
    state = {
        "date": TODAY.strftime("%Y-%m-%d"),
        "symbols": syms,
        "status": "armed",
        "steps": [{"ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                   "step": "FORCE_TAGS"}],
        "tags": tags,
    }
    outp = Path("data/state"); outp.mkdir(parents=True, exist_ok=True)
    (outp / "tags_state.json").write_text(json.dumps(state, indent=2))
    (outp / "live_state.json").write_text(json.dumps(state, indent=2))
    print("OK →", state["date"], "symbols:", len(syms))

if __name__ == "__main__":
    main()
