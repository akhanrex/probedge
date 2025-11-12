
from __future__ import annotations

DEFAULT_CLOSE_PCT = 0.0025      # 0.25% of entry
DEFAULT_CLOSE_FR_ORB = 0.20     # 20% of ORB range

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return float("nan")

def is_close(a, b, entry_px, orb_range, close_pct: float = DEFAULT_CLOSE_PCT, close_fr_orb: float = DEFAULT_CLOSE_FR_ORB) -> bool:
    """Return True if |a-b| <= min(entry*close_pct, orb_range*close_fr_orb)."""
    a = _safe_float(a); b = _safe_float(b)
    thr_parts = []
    if entry_px and entry_px > 0:
        thr_parts.append(entry_px * close_pct)
    if orb_range and orb_range > 0:
        thr_parts.append(abs(orb_range) * close_fr_orb)
    thr = min(thr_parts) if thr_parts else float("inf")
    if not (a == a and b == b):  # NaN check
        return False
    return abs(a - b) <= thr

def compute_stop(opening_trend: str, pick: str, orb_h, orb_l, prev_h, prev_l, entry_px,
                 close_pct: float = DEFAULT_CLOSE_PCT, close_fr_orb: float = DEFAULT_CLOSE_FR_ORB) -> float:
    """Implements the exact SL policy Aamir specified, in formulas.

    Cases:
      1) OT=BULL, Pick=BULL → SL = range low (ORB_L); if prev_day_low is "close" to ORB_L, use prev_day_low instead
      2) OT=BULL, Pick=BEAR → SL = double-range high = ORB_H + (ORB_H-ORB_L)
      3) OT=BEAR, Pick=BEAR → SL = range high (ORB_H); if prev_day_high "close" to ORB_H, use prev_day_high
      4) OT=BEAR, Pick=BULL → SL = double-range low = ORB_L - (ORB_H-ORB_L)
      5) OT=TR,   Pick=BEAR → SL = double-range high
      6) OT=TR,   Pick=BULL → SL = double-range low
    """
    ot = (opening_trend or "").upper()
    side = (pick or "").upper()
    orb_h = _safe_float(orb_h); orb_l = _safe_float(orb_l)
    prev_h = _safe_float(prev_h); prev_l = _safe_float(prev_l)
    orb_rng = max(0.0, orb_h - orb_l)
    dbl_h = orb_h + orb_rng
    dbl_l = orb_l - orb_rng

    near_prev_l = is_close(orb_l, prev_l, entry_px, orb_rng, close_pct, close_fr_orb)
    near_prev_h = is_close(orb_h, prev_h, entry_px, orb_rng, close_pct, close_fr_orb)

    if ot == "BULL" and side == "BULL":
        return prev_l if (prev_l == prev_l and near_prev_l) else orb_l
    if ot == "BULL" and side == "BEAR":
        return dbl_h
    if ot == "BEAR" and side == "BEAR":
        return prev_h if (prev_h == prev_h and near_prev_h) else orb_h
    if ot == "BEAR" and side == "BULL":
        return dbl_l
    if ot == "TR" and side == "BEAR":
        return dbl_h
    if ot == "TR" and side == "BULL":
        return dbl_l
    # Fallback: opposing double
    return dbl_l if side == "BULL" else dbl_h
