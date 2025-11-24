# probedge/broker/kite_session.py

from __future__ import annotations

import json
import os
from pathlib import Path
import datetime as dt
from typing import Optional, Dict, Any

# Try to load .env so KITE_* are available in os.environ
try:
    from dotenv import load_dotenv
    load_dotenv()  # looks for .env in current/parent dirs
except Exception:
    pass

from kiteconnect import KiteConnect

# SETTINGS is optional; we just use it if available
try:
    from probedge.infra.settings import SETTINGS  # type: ignore
except Exception:
    SETTINGS = None  # type: ignore

from probedge.infra.logger import get_logger

log = get_logger(__name__)


def _get_val(env_name: str, settings_attr: str | None = None, default: str = "") -> str:
    """
    Priority:
      1) SETTINGS.<settings_attr> if present and non-empty
      2) Environment variable ENV_NAME (from os.environ / .env)
      3) default
    """
    if SETTINGS is not None and settings_attr and hasattr(SETTINGS, settings_attr):
        val = getattr(SETTINGS, settings_attr)
        if isinstance(val, str) and val:
            return val

    return os.getenv(env_name, default)


API_KEY: str = _get_val("KITE_API_KEY", "kite_api_key", "")
API_SECRET: str = _get_val("KITE_API_SECRET", "kite_api_secret", "")
SESSION_FILE: str = _get_val(
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
        # We log a warning first to avoid silent confusion
        log.error(
            "[kite_session] KITE_API_KEY not found. "
            "Check your .env or infra.settings.SETTINGS."
        )
        raise RuntimeError("KITE_API_KEY is not set (check your .env)")
    return KiteConnect(api_key=API_KEY)


def get_login_url() -> str:
    """
    Return the Kite login URL.
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
    Exchange request_token -> access_token and save it.
    """
    if not API_SECRET:
        log.error("[kite_session] KITE_API_SECRET not found. Check your .env.")
        raise RuntimeError("KITE_API_SECRET is not set (check your .env)")

    kite = _new_kite()
    data = kite.generate_session(request_token, API_SECRET)

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
    Returns KiteConnect with access_token set.
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
    Small status dict so UI/CLI can know if we are logged in.
    """
    sess = load_session()
    if not sess:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "user_id": sess.get("user_id"),
        "login_time": sess.get("login_time"),
    }
