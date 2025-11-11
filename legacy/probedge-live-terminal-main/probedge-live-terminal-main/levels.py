from __future__ import annotations
from typing import Tuple

def orb_from_first5(bar5_high: float, bar5_low: float) -> Tuple[float, float]:
    return (bar5_high, bar5_low)

def sl_targets_from_rules(direction: str, entry: float, orb_high: float, orb_low: float):
    # SL at opposite ORB side; T1=1R, T2=2R
    if direction == "BULL":
        stop = orb_low
    else:
        stop = orb_high
    risk_per_share = abs(entry - stop)
    t1 = entry + risk_per_share if direction == "BULL" else entry - risk_per_share
    t2 = entry + 2*risk_per_share if direction == "BULL" else entry - 2*risk_per_share
    return stop, t1, t2, risk_per_share
