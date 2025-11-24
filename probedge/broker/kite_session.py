# probedge/broker/kite_session.py

from __future__ import annotations

import json
import os
from pathlib import Path
import datetime as dt
from typing import Optional, Dict, Any

from kiteconnect import KiteConnect

# Try to import SETTINGS, but don't depend on it
try:
    from probedge.infra.settings import SETTINGS
except Exception:
    SETTINGS = None  # type: ignore

from probedge.infra.logger import get_logger

log = get_logger(__name__)


def _from_settings_or_env(env_key: str, attr_name: str, default: str = "") -> str:
    """
    Helper: prefer SETTINGS.attr_name if present, else environment variable.
    This lets us work even if infra.settings is not updated.
    """
    if SETTINGS is not None and hasattr(SETTINGS, attr_name):
        val = getattr(SETTINGS, attr_name)
        if isinstance(val, str) and val:
            return val
    return os.getenv(env_key, default)


API_KEY: str = _from_settings_or_env("KITE_API_KEY", "kite_api_key", "")
API_SECRET: str = _from_settings_or_env("KITE_API_SECRET", "kite_api_secret", "")
SESSION_FILE: str = _from_settings_or_env(
    "KITE_SESSION_FILE",
    "kite_session_file",
    "data/state/kite_session.json",
)

SESSION_PATH = Path(SESSION_FILE)


class NotAuthenticated(Exception):
    pass


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _new_kite() -> KiteConnect:
    if not API_KEY:
        raise RuntimeError("KITE_API_KEY is not set (check your .env)")
    return KiteConnect(api_key=API_KEY)


def get_login_url() -> str:
    """
    Return the Kite login URL.
    In UI/CLI you open this in browser.
    """
    kite = _new_kite()
    url = kite.login_url()
    log.info("[kite_session] login_url = %s", url)
    return url


def save_session(session: Dict[str, Any]) -> None:
    _ensure_dir(SESSION_PATH)
    SESSION_PATH.write_text(json.dumps(session, indent=2, default=str))
    log.info("[kite_session] session saved to %s", SESSION_PATH)


def load_session() -> Optional[Dict[str, Any]]:
    if not SESSION_PATH.exists():
        return None
    try:
        return json.loads(SESSION_PATH.read_text())
    except Exception as e:
        log.warning("[kite_session] failed to load session: %s", e)
        return None


def handle_callback(request_token: str) -> Dict[str, Any]:
    """
    Core logic: exchange request_token -> access_token and save it.
    Can be called from an HTTP callback OR from a CLI script.
    """
    if not API_SECRET:
        raise RuntimeError("KITE_API_SECRET is not set (check your .env)")

    kite = _new_kite()
    data = kite.generate_session(request_token, API_SECRET)
    # data contains: access_token, public_token, user_id, etc.

    session = {
        "api_key": API_KEY,
        "access_token": data["access_token"],
        "public_token": data.get("public_token"),
        "user_id": data["user_id"],
        "login_time": dt.datetime.now().isoformat(),
    }
    save_session(session)
    return session


def get_authorized_kite() -> KiteConnect:
    """
    For live engine: returns KiteConnect with access_token set.
    Raises NotAuthenticated if no session on disk.
    """
    sess = load_session()
    if not sess or "access_token" not in sess:
        raise NotAuthenticated("No Kite session on disk; login first")

    kite = _new_kite()
    kite.set_access_token(sess["access_token"])
    return kite


def kite_status() -> Dict[str, Any]:
    """
    Small status dict: used by UI or CLI to know if we are logged in.
    """
    sess = load_session()
    if not sess:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "user_id": sess.get("user_id"),
        "login_time": sess.get("login_time"),
    }
