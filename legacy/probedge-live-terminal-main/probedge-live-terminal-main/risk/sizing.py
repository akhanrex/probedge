import math

def qty_from_risk(risk_rs: float, entry: float, stop: float) -> int:
    rps = abs(entry - stop)
    if rps <= 0: return 0
    return max(0, math.floor(risk_rs / rps))
