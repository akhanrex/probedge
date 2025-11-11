# app/views/auth.py — Auth & Kite-connect views

import streamlit as st

from app.config import (
    APP_PASSWORD,
    API_KEY,
    API_SECRET,
)
from app.ui import show_logo

# Optional: Kite SDK
try:
    from kiteconnect import KiteConnect
except Exception:
    KiteConnect = None

# Optional: login callback helper (safe to import; used in safe_bootstrap, not here)
try:
    from kite_login import handle_callback  # noqa: F401
except Exception:
    pass


def render_connect_kite():
    """Small card that shows the 'Connect Zerodha (Kite)' button when creds exist."""
    show_logo(centered=True)
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        st.markdown("To enable live data, connect Zerodha (optional).")
        if KiteConnect is None or not API_KEY:
            st.error(
                "Kite API not configured; you can still use the app without live data."
            )
            st.caption(
                "Set KITE_API_KEY / KITE_API_SECRET in secrets/env to enable live."
            )
            return
        try:
            kc = KiteConnect(api_key=API_KEY)
            auth_url = kc.login_url()
            st.link_button("Connect Zerodha (Kite)", auth_url, use_container_width=True)
        except Exception as e:
            st.error(f"Could not generate Zerodha auth URL: {e}")


def render_login():
    """Simple password gate used by safe_bootstrap."""
    show_logo(centered=True)
    _, mid, _ = st.columns([1, 2, 1])
    with mid:
        pwd = st.text_input(
            "Password", type="password", placeholder="Enter password", key="pwd_input"
        )
        if st.button("Login", use_container_width=True, key="login_btn"):
            if (pwd or "") == (APP_PASSWORD or ""):
                st.session_state.logged_in = True
                st.success("Logged in")
                st.rerun()
            else:
                st.error("Invalid password")
