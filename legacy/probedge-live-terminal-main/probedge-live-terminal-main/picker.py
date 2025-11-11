from __future__ import annotations
from typing import Dict, Any, Tuple, Optional
from infra import config

"""
Frequency-based picker L3->L2->L1->L0 with gates.
Assumes `freq` is a dict with nested levels and counts by direction.
Structure example:
{
  "L3": {"BULL": 10, "BEAR": 5},
  "L2": {"BULL": 9, "BEAR": 6},
  "L1": {"BULL": 15, "BEAR": 12},
  "L0": {"BULL": 30, "BEAR": 28},
}
"""

def _choose_direction(counts: Dict[str, int]) -> Tuple[str, int]:
    bull = counts.get("BULL", 0)
    bear = counts.get("BEAR", 0)
    if bull == 0 and bear == 0:
        return "NONE", 0
    total = bull + bear
    if bull >= bear:
        conf = int(100 * bull / total)
        return "BULL", conf
    else:
        conf = int(100 * bear / total)
        return "BEAR", conf

def pick_with_gates(freq: Dict[str, Dict[str, int]], ot_align: Optional[str]) -> Tuple[str, int, str]:
    levels = [("L3", config.PICKER_MIN_L3), ("L2", config.PICKER_MIN_L2), ("L1", config.PICKER_MIN_L1), ("L0", config.PICKER_MIN_L0)]
    for lvl, min_count in levels:
        counts = freq.get(lvl, {})
        if (counts.get("BULL", 0) + counts.get("BEAR", 0)) < min_count:
            continue
        direction, conf = _choose_direction(counts)
        if direction == "NONE":
            continue
        if config.PICKER_REQUIRE_OT_ALIGN and ot_align and direction != ot_align:
            continue
        if conf < config.PICKER_CONF_MIN:
            continue
        return direction, conf, lvl
    return "NONE", 0, "NA"
