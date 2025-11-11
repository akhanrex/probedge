from __future__ import annotations
from typing import Optional, Tuple, List

def arm(direction: str, highs: List[float], lows: List[float], eps_bps: float = 0.0) -> Optional[Tuple[float, str]]:
    # highs/lows should be bars 6..10 inclusive
    if direction not in ("BULL", "BEAR"):
        return None
    for i in range(len(highs)):
        if direction == "BULL":
            trigger = highs[i] * (1 + eps_bps)
        else:
            trigger = lows[i] * (1 - eps_bps)
        return (trigger, f"6TO10_BAR_{i+6}")
    return None
