"""In-process idempotency helpers.

Phase B will replace this with a persistent store (redis/sqlite) so the
idempotency key survives restarts.

For Phase A paper / early live wiring, an in-memory set is enough to
prevent accidental double-sends inside one run.
"""

from __future__ import annotations

import time
import threading
from typing import Dict

_lock = threading.Lock()
_seq = 0
_seen: Dict[str, float] = {}


def next_client_order_id(prefix: str, symbol: str) -> str:
    """Generate a deterministic-ish client tag used as idempotency key."""
    global _seq
    with _lock:
        _seq += 1
        s = _seq
    date = time.strftime("%Y%m%d")
    sym = symbol.replace("-", "").upper()
    return f"{prefix}-{date}-{sym}-{s:06d}"


def ensure_once(key: str, *, ttl_s: int = 24 * 3600) -> bool:
    """Return True if this key hasn't been seen in the last ttl_s seconds."""
    now = time.time()
    if not key:
        return False
    with _lock:
        # GC old keys opportunistically
        if _seen:
            cutoff = now - float(ttl_s)
            for k in list(_seen.keys()):
                if _seen[k] < cutoff:
                    _seen.pop(k, None)

        if key in _seen:
            return False

        _seen[key] = now
        return True
