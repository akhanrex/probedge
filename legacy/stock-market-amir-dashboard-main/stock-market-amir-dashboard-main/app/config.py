# app/config.py
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st


def get_secret_first(*names: str, default=None):
    """Try Streamlit secrets, then env vars (case-insensitive), else default."""
    try:
        for n in names:
            v = st.secrets.get(n)
            if v:
                return str(v)
    except Exception:
        pass
    for n in names:
        v = os.getenv(n) or os.getenv(n.upper())
        if v:
            return str(v)
    return default


# --- Core app constants (exactly as in app.py, unchanged) ---
APP_PASSWORD = get_secret_first("PROBEDGE_PASSWORD", "APP_PASSWORD", default="probedge")
LOGO_URL = get_secret_first("PROBEDGE_LOGO_URL", default=None)
TM_ICON_URL = get_secret_first("TATAMOTORS_ICON_URL", "TM_ICON_URL", default=None)
AE_ICON_URL = get_secret_first("ADANIENT_ICON_URL", "AE_ICON_URL", default=None)
HALF_LIFE_DAYS = float(get_secret_first("PROBEDGE_HALF_LIFE_DAYS", default="365"))
# --- UI feature flags (global) ---
SHOW_FINAL_SIGNAL = False   # hide + do not compute Final Signal A/B everywhere


# Zerodha (Kite) creds
API_KEY = get_secret_first("kite_api_key")
API_SECRET = get_secret_first("kite_api_secret")

YEARS_BACK = int(get_secret_first("PROBEDGE_YEARS_BACK", default="15"))
FORCE_REBUILD_ON_SPAN = bool(
    int(get_secret_first("PROBEDGE_FORCE_REBUILD", default="0"))
)

# Timezone helpers
IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    return datetime.now(IST)
