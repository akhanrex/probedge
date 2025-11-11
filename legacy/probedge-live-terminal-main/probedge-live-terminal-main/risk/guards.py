def orb_ok(orb_rng: float, entry_px: float, min_pct=0.0025, max_pct=0.025):
if entry_px <= 0: return False
pct = orb_rng / entry_px
return (pct >= min_pct) and (pct <= max_pct)
