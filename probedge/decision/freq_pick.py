import pandas as pd

# DO NOT change manual terminal code. Live imports it as the source of truth.
from apps.api.routes._freq_select import apply_lookback, select_hist_batch_parity

def _norm(x) -> str:
    return str(x or "").strip().upper()

def freq_pick(day, master: pd.DataFrame, tags_override: dict | None = None):
    """
    Live PICK must be identical to manual terminal PICK.
    Wrapper over apps/api/routes/_freq_select.py (batch-parity engine).

    tags_override:
      Optional {OpeningTrend, OpenLocation, PrevDayContext} to use when today's
      MASTER row isn't present yet at 09:40.
      If not provided, behavior remains strict (ABSTAIN on missing master row).

    Returns: (display_pick, conf_pct, reason, level, stats_dict)
    """
    if master is None or master.empty:
        return "ABSTAIN", 0, "no master", "L0", {}

    day = pd.to_datetime(day, errors="coerce")
    if pd.isna(day):
        return "ABSTAIN", 0, "bad day", "L0", {}
    day = pd.Timestamp(day).normalize()

    if "Date" not in master.columns:
        return "ABSTAIN", 0, "master missing Date", "L0", {}

    # Try to read tags from today's master row if available
    ot = ol = pdc = ""
    m_date = pd.to_datetime(master["Date"], errors="coerce").dt.normalize()
    mrow = master.loc[m_date == day]

    if not mrow.empty:
        r0 = mrow.iloc[0]  # <-- positional, safe
        ot  = _norm(r0.get("OpeningTrend", ""))
        ol  = _norm(r0.get("OpenLocation", ""))
        pdc = _norm(r0.get("PrevDayContext", ""))
    else:
        # No row for today → allow override (computed from TM5 at 09:40)
        if not tags_override:
            return "ABSTAIN", 0, "missing master row", "L0", {}
        ot  = _norm(tags_override.get("OpeningTrend", ""))
        ol  = _norm(tags_override.get("OpenLocation", ""))
        pdc = _norm(tags_override.get("PrevDayContext", ""))

    # Apply manual/batch lookback window and normalization
    base, _ = apply_lookback(master, asof=str(day.date()))

    # Run exact manual/batch selector
    _hist_bb, meta = select_hist_batch_parity(base, ot=ot, ol=ol, pdc=pdc)

    pick = meta.get("pick") or "ABSTAIN"
    conf = int(meta.get("conf_pct") or 0)
    level = meta.get("level") or "L0"
    reason = meta.get("reason") or ""

    stats = {
        "level": level,
        "B": int(meta.get("bull_n") or 0),
        "R": int(meta.get("bear_n") or 0),
        "N": int(meta.get("total") or 0),
        "gap_pp": float(meta.get("gap_pp") or 0.0),
        "conf%": conf,
    }
    return pick, conf, reason, level, stats
