from typing import Dict, Tuple
import math

def orb_from_bars(bars_1_to_5: list[Dict]) -> Tuple[float, float, float]:
    """
    ORB over bars 1..5 (paper mode: first 5 fast-bars).
    Returns (orb_high, orb_low, orb_range).
    """
    if len(bars_1_to_5) < 5:
        return (float("nan"), float("nan"), float("nan"))
    highs = [float(b["High"]) for b in bars_1_to_5[:5]]
    lows  = [float(b["Low"])  for b in bars_1_to_5[:5]]
    oh, ol = max(highs), min(lows)
    return oh, ol, max(0.0, oh - ol)

def double_range(orb_h: float, orb_l: float) -> Tuple[float, float]:
    rng = max(0.0, orb_h - orb_l)
    return orb_h + rng, orb_l - rng

def sl_targets_from_rules(ot: str, pick: str, entry_px: float,
                          orb_h: float, orb_l: float,
                          prev_h: float | None, prev_l: float | None):
    """
    Mirrors your SL rules:
      - OT=BULL & Pick=BULL -> SL = ORB Low or PrevDay Low if 'close'
      - OT=BULL & Pick=BEAR -> SL = dbl_h
      - OT=BEAR & Pick=BEAR -> SL = ORB High or PrevDay High if 'close'
      - OT=BEAR & Pick=BULL -> SL = dbl_l
      - OT=TR: use double-range side
    Targets: T1/T2 = ±1R / ±2R
    """
    def is_close(a: float, b: float, entry: float, orb_rng: float) -> bool:
        if not (math.isfinite(a) and math.isfinite(b)): return False
        parts = []
        if math.isfinite(entry) and entry > 0: parts.append(entry * 0.0025)   # 0.25%
        if math.isfinite(orb_rng):           parts.append(abs(orb_rng) * 0.20) # 20% of ORB
        thr = min(parts) if parts else float("inf")
        return abs(a - b) <= thr

    dbl_h, dbl_l = double_range(orb_h, orb_l)
    long_side = (pick == "BULL")

    # Stop
    if ot == "BULL" and pick == "BULL":
        if prev_l is not None and is_close(orb_l, prev_l, entry_px, orb_h - orb_l):
            stop = prev_l
        else:
            stop = orb_l
    elif ot == "BULL" and pick == "BEAR":
        stop = dbl_h
    elif ot == "BEAR" and pick == "BEAR":
        if prev_h is not None and is_close(orb_h, prev_h, entry_px, orb_h - orb_l):
            stop = prev_h
        else:
            stop = orb_h
    elif ot == "BEAR" and pick == "BULL":
        stop = dbl_l
    elif ot == "TR" and pick == "BULL":
        stop = dbl_l
    elif ot == "TR" and pick == "BEAR":
        stop = dbl_h
    else:
        stop = dbl_l if long_side else dbl_h

    # Risk/Targets
    if long_side:
        rps = entry_px - stop
        t1 = entry_px + rps
        t2 = entry_px + 2 * rps
    else:
        rps = stop - entry_px
        t1 = entry_px - rps
        t2 = entry_px - 2 * rps

    return stop, rps, t1, t2
