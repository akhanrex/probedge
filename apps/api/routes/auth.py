# apps/api/routes/auth.py

from datetime import date
import json
from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import RedirectResponse
from kiteconnect import KiteConnect

from probedge.infra.settings import SETTINGS

router = APIRouter(prefix="/api/auth", tags=["auth"])

# ---- Session file: single source of truth ----

# Absolute path to kite_session.json (from .env / settings)
SESSION_FILE: Path = (
    SETTINGS.kite_session_file
    if getattr(SETTINGS, "kite_session_file", None)
    else (SETTINGS.data_dir / "data/state/kite_session.json")
)


def _load_session() -> dict | None:
    """Load stored Kite session from disk, or None."""
    if not SESSION_FILE.exists():
        return None
    try:
        with SESSION_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception:
        return None


def _save_session(sess: dict) -> None:
    """Persist Kite session to disk."""
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    # we also stamp the session_day for convenience
    sess = dict(sess)
    sess.setdefault("session_day", date.today().isoformat())
    with SESSION_FILE.open("w", encoding="utf-8") as f:
        json.dump(sess, f, indent=2, default=str)


def _has_valid_token_today() -> tuple[bool, dict | None]:
    """Return (is_valid_for_today, session_dict_or_None)."""
    sess = _load_session()
    if not sess:
        return False, None

    day = str(sess.get("session_day") or "")
    today = date.today().isoformat()
    return (day == today), sess


def _make_kite_for_login() -> KiteConnect:
    """Create a Kite client for login/generate_session."""
    api_key = SETTINGS.kite_api_key
    api_secret = SETTINGS.kite_api_secret
    if not api_key or not api_secret:
        raise RuntimeError("Kite API key/secret not configured in .env")

    kite = KiteConnect(api_key=api_key)
    # redirect_url is configured in Kite developer console; we keep a copy in .env for clarity
    if getattr(SETTINGS, "kite_redirect_url", None):
        kite.set_session_hook(None)  # just to be explicit; not strictly needed
    return kite


def _make_kite_for_historical(access_token: str) -> KiteConnect:
    """Helper if you ever want to reuse this from scripts."""
    api_key = SETTINGS.kite_api_key
    if not api_key:
        raise RuntimeError("Kite API key not configured in .env")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


# ---- Public API ----

@router.get("/status")
def auth_status():
    ok, sess = _has_valid_token_today()
    return {
        "today": date.today().isoformat(),
        "has_valid_token_today": ok,
        "session_day": (sess.get("session_day") if sess else None),
        "session_user_id": (sess.get("user_id") if sess else None),
        "data_dir": str(SETTINGS.data_dir),
    }


@router.get("/login_url")
def login_url():
    """
    Return the Zerodha Connect login URL.
    Even if you are already logged in on the browser, this will redirect
    via Kite and then back to our /api/auth/callback with a request_token.
    """
    api_key = SETTINGS.kite_api_key
    api_secret = SETTINGS.kite_api_secret
    redirect_url = getattr(SETTINGS, "kite_redirect_url", None)

    if not api_key or not api_secret:
        raise RuntimeError("Kite API key/secret not configured in .env")

    kite = KiteConnect(api_key=api_key)

    # IMPORTANT: The redirect URL must match what you configured in Kite console.
    # In your .env: KITE_REDIRECT_URL=http://127.0.0.1:9002/api/auth/callback
    if redirect_url:
        kite.redirect_url = redirect_url

    url = kite.login_url()
    return {"login_url": url}


@router.get("/callback", include_in_schema=False)
def kite_callback(request_token: str = Query(...)):
    """
    Zerodha will redirect here with ?request_token=...
    We exchange it for an access_token and persist the session to disk.
    """
    api_key = SETTINGS.kite_api_key
    api_secret = SETTINGS.kite_api_secret
    if not api_key or not api_secret:
        # In practice this should not happen if login_url worked.
        raise RuntimeError("Kite API key/secret not configured in .env")

    kite = KiteConnect(api_key=api_key)

    # Exchange request_token -> access_token + user data
    data = kite.generate_session(request_token, api_secret=api_secret)
    # Typical response includes: access_token, user_id, public_token, etc.

    # Stamp session_day and save to file
    data["session_day"] = date.today().isoformat()
    _save_session(data)

    # After storing session, push user to live terminal
    return RedirectResponse(url="/")

