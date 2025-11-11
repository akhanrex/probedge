from __future__ import annotations
from typing import Optional, Tuple

def arm(direction: str, bar5_high: float, bar5_low: float, eps_bps: float = 0.0) -> Optional[Tuple[float, str]]:
    if direction not in ("BULL", "BEAR"):
        return None
    if direction == "BULL":
        trigger = bar5_high * (1 + eps_bps)
    else:
        trigger = bar5_low * (1 - eps_bps)
    return trigger, "5TH_BAR"
