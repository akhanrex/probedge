from __future__ import annotations
from math import floor

def qty_from_risk(risk_rs: int, risk_per_share: float) -> int:
    if risk_per_share <= 0:
        return 0
    return max(0, floor(risk_rs / risk_per_share))

def ensure_targets(entry: float, stop: float, t1: float, t2: float):
    assert abs(t2 - entry) >= 2 * abs(entry - stop), "RR < 1:2 not allowed"
    return entry, stop, t1, t2
