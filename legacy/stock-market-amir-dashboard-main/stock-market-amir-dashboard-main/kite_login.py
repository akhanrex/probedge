# kite_login.py — minimal Zerodha OAuth for Streamlit
import os
import streamlit as st

try:
    from kiteconnect import KiteConnect
except Exception:
    KiteConnect = None


def _get_secret_first(*names, default=None):
    # prefer st.secrets, then env
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


API_KEY = _get_secret_first("kite_api_key", "KITE_API_KEY")
API_SECRET = _get_secret_first("kite_api_secret", "KITE_API_SECRET")
# Optional: pre-provisioned token to skip interactive login (demo mode)
BOOT_TOKEN = _get_secret_first("kite_access_token", "KITE_ACCESS_TOKEN", default=None)


def login_button(label: str = "Connect to login", width_px: int = 220) -> None:
    """Render a compact, centered connect button without full-width stretching."""
    if "access_token" in st.session_state:
        st.success("Zerodha already connected ✅")
        return

    if BOOT_TOKEN:
        st.session_state["access_token"] = BOOT_TOKEN
        st.info("Using pre-set Kite access token from secrets.")
        return

    if KiteConnect is None:
        st.error("kiteconnect package not installed. Add 'kiteconnect' to requirements.txt.")
        return
    if not API_KEY:
        st.error("Kite API key not configured in secrets.")
        return

    try:
        kite = KiteConnect(api_key=API_KEY)
        url = kite.login_url()  # Zerodha dev console redirect URL
        # Small, centered anchor styled as a button
        st.markdown(
            f"""
            <div style="display:flex;justify-content:center;">
              <a href="{url}" target="_self"
                 style="display:inline-block;padding:10px 16px;border-radius:8px;
                        background:#2563eb;color:#ffffff;text-decoration:none;
                        font-weight:600;width:{width_px}px;text-align:center;">
                {label}
              </a>
            </div>
            """,
            unsafe_allow_html=True,
        )
    except Exception as e:
        st.error(f"Failed to get login URL: {e}")
        
# at top of kite_login.py
from typing import Optional


def handle_callback(request_token: str | None = None):
    ...
    req = (request_token or "").strip()
    if not req:
        try:
            qp = getattr(st, "query_params", {}) or {}
            req = qp.get("request_token")
            if isinstance(req, list):
                req = req[0]
            if not req:
                qp2 = st.experimental_get_query_params()
                if "request_token" in qp2 and qp2["request_token"]:
                    req = qp2["request_token"][0]
        except Exception:
            req = None

    if not req:
        st.error("Missing 'request_token' in callback URL.")
        return

    # st.caption(...) that was indented under the 'return' can be removed.

    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(req, api_secret=API_SECRET)
        st.session_state["access_token"] = data["access_token"]
        try:
            st.query_params.clear()
        except Exception:
            try:
                st.experimental_set_query_params()
            except Exception:
                pass
        st.success("Zerodha connected 🎉")
        try:
            st.divider()
            st.caption("Kite Status")
            st.write("Connected:", "✅")
        except Exception:
            pass

        (st.rerun if hasattr(st, "rerun") else st.experimental_rerun)()
    except Exception as e:
        st.error(f"Kite login failed: {e}")
