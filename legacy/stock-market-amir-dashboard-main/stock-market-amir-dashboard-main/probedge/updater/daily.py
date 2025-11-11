# probedge/updater/daily.py
from __future__ import annotations
from typing import Optional
from datetime import datetime, time as dtime

try:
    from kiteconnect import KiteConnect  # type: ignore
except Exception:
    KiteConnect = None  # type: ignore

# Delegate to the weekly updater's incremental function (central place that writes master)
try:
    from probedge.updater.weekly import update_master_if_needed as _weekly_update_if_needed
except Exception:
    _weekly_update_if_needed = None

def update_master_if_needed(kite: Optional["KiteConnect"], master_path: str, *, symbol: str) -> int:
    """
    Return number of rows appended to master. No-op if not connected.
    Delegates to the weekly updater's incremental function if available.
    """
    if _weekly_update_if_needed and kite:
        try:
            return int(_weekly_update_if_needed(kite, master_path, symbol=symbol) or 0)
        except Exception:
            pass
    return 0

# ---- IST time helper (prefer app helper if available) ----
try:
    from app.config import now_ist as _now_ist
except Exception:
    _now_ist = None

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None

def _now_ist_fallback() -> datetime:
    if _now_ist:
        return _now_ist()
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Kolkata"))
    return datetime.utcnow()

def append_today_if_connected_and_closed(
    kite: Optional["KiteConnect"], master_path: str, *, symbol: str, now: Optional[datetime] = None
) -> int:
    """
    Opportunistically append 'today' after market close (>= 15:35 IST).
    Safe no-op if kite is None or market not closed.
    """
    now = now or _now_ist_fallback()
    if not (dtime(15, 35) <= now.time() <= dtime(23, 59, 59)):
        return 0
    if not kite:
        return 0
    return update_master_if_needed(kite, master_path, symbol=symbol)
