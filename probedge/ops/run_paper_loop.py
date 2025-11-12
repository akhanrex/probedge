from __future__ import annotations
import time, csv
from datetime import datetime, timezone

try:
    from probedge.infra.settings import SETTINGS
except Exception:
    SETTINGS = None

from probedge.storage.atomic_json import save_json_atomic, load_json_safe

def _now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def _resolve_intraday_path(sym: str) -> str:
    pat = getattr(SETTINGS.paths, "intraday", "data/intraday/{sym}_5minute.csv")
    return str(pat).replace("{sym}", sym)

def _state_path() -> str:
    return getattr(SETTINGS.paths, "state", "live_state.json")

def _read_last_bar(csv_path: str):
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            last = None
            for row in csv.DictReader(f):
                last = row
            return last or {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def main():
    symbols = list(SETTINGS.symbols) if SETTINGS and getattr(SETTINGS, "symbols", None) else []
    if not symbols:
        print("No symbols configured; exiting.")
        return

    state_path = _state_path()
    print(f"[paper_loop] writing state to: {state_path}")
    while True:
        st = load_json_safe(state_path)
        st["heartbeat"] = _now_iso()
        st.setdefault("quotes", {})

        for sym in symbols:
            p = _resolve_intraday_path(sym)
            bar = _read_last_bar(p)
            if bar:
                try:
                    close_val = float(bar.get("Close") or bar.get("close") or 0)
                except Exception:
                    close_val = 0.0
                st["quotes"][sym] = {
                    "DateTime": bar.get("DateTime") or bar.get("Datetime") or bar.get("date") or "",
                    "Close": close_val,
                }

        save_json_atomic(state_path, st)
        time.sleep(1.0)

if __name__ == "__main__":
    main()
