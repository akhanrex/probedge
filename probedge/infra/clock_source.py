"""Clock source of truth for LIVE and SIM.

- LIVE: tz-aware IST wall clock
- SIM: if state has sim=True and sim_clock (ISO string), use that (assume IST if tz missing)

All callers should use get_now_ist(state) and never datetime.now() directly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def get_now_ist(state: Optional[Dict[str, Any]] = None) -> datetime:
    """Return a tz-aware datetime in IST."""
    if isinstance(state, dict) and bool(state.get("sim")):
        sc = state.get("sim_clock")
        if isinstance(sc, str) and sc.strip():
            try:
                dt = datetime.fromisoformat(sc)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=IST)
                return dt.astimezone(IST)
            except Exception:
                pass
    return datetime.now(tz=IST)

# Backward-compatible alias used by runtime scripts

def now_ist(state=None):
    """Return current IST datetime (tz-aware). Alias for get_now_ist()."""
    return get_now_ist(state)

