"""
Agg5 (Phase-A heartbeat + optional quote seeding).

Goal (Phase A / paper):
- Keep /api/state responsive with a ticking clock_ist + last_agg5_ts
- Seed state["quotes"] from today's TM5 (intraday CSV) so UI has LTP even without live ticks
- NEVER clobber other state fields (PATCH-ONLY writes)

This is intentionally minimal. Live tick -> 5m aggregation can be layered later (Phase B).
"""

from __future__ import annotations

import time
from datetime import date as Date
from typing import Any, Dict, List, Optional

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.infra.clock_source import now_ist
from probedge.storage.atomic_json import AtomicJSON
from probedge.storage.resolver import locate_for_read

DEFAULT_SLEEP_SEC = 1.0


def _seed_quotes_from_tm5(symbols: List[str], day: str) -> Dict[str, Any]:
    quotes: Dict[str, Any] = {}
    for sym in symbols:
        try:
            p = locate_for_read("intraday", sym)
            # minimal columns; file can be large but universe is small
            df = pd.read_csv(p, usecols=["date", "time", "open", "high", "low", "close"])
            d = df[df["date"].astype(str).str.strip() == day]
            if len(d) == 0:
                continue
            last = d.iloc[-1]
            quotes[str(sym)] = {
                "ltp": float(last["close"]),
                "o": float(last["open"]),
                "h": float(last["high"]),
                "l": float(last["low"]),
                "ts_ist": now_ist().isoformat(),
            }
        except Exception:
            # best-effort seeding only
            continue
    return quotes


def run_agg(symbols: List[str], bar_seconds: int = 300, state_path: Optional[str] = None) -> None:
    # state path must be DATA_DIR isolated; SETTINGS.paths.state is absolute in our patched settings
    sp = state_path or SETTINGS.paths.state
    aj = AtomicJSON(sp)

    # Seed quotes once from existing TM5 so UI has an LTP immediately
    st = aj.read(default={}) or {}
    day = (st.get("date") or now_ist().date().isoformat())
    quotes = _seed_quotes_from_tm5(symbols, day)
    if quotes:
        aj.write({"quotes": quotes})

    # Heartbeat loop
    while True:
        aj.write(
            {
                "clock_ist": now_ist().isoformat(),
                "health": {"last_agg5_ts": time.time()},
            }
        )
        time.sleep(DEFAULT_SLEEP_SEC)
