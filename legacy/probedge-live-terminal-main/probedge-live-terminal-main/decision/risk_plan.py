# decision/risk_plan.py
def rr_targets(entry_ref: float, stop: float, rr: float = 2.0):
    """
    Pure 1:R → T1=1R, T2=RR from entry_ref relative to stop.
    Works for long & short automatically based on entry vs stop.
    """
    if entry_ref is None or stop is None:
        return None, None
    direction = 1.0 if entry_ref > stop else -1.0  # long if stop below entry
    r = abs(entry_ref - stop)
    t1 = entry_ref + direction * r * 1.0
    t2 = entry_ref + direction * r * rr
    return round(t1, 2), round(t2, 2)

def ensure_targets(entry_ref: float, stop: float, t1, t2, rr: float = 2.0):
    """
    If t1/t2 are missing or NaN, compute via rr_targets.
    """
    def _is_num(x): 
        try: return x == x and x is not None
        except: return False
    if not _is_num(t1) or not _is_num(t2):
        return rr_targets(entry_ref, stop, rr)
    return round(float(t1),2), round(float(t2),2)
