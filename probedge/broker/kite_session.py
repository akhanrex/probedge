# probedge/broker/kite_session.py

from __future__ import annotations
import json
from pathlib import Path
import datetime as dt

from kiteconnect import KiteConnect

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger

log = get_logger(__name__)

SESSION_PATH = Path(SETTINGS.kite.session_file)


class NotAuthenticated(Exception):
    pass


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _new_kite() -> KiteConnect:
    return KiteConnect(api_key=SETTINGS.kite.api_key)


def get_login_url() -> str:
    """
    Called by backend when UI wants the Kite login URL.
    """
    kite = _new_kite()
    url = kite.login_url()
    log.info("[kite_session] login_url = %s", url)
    return url


def save_session(session: dict) -> None:
    _ensure_dir(SESSION_PATH)
    SESSION_PATH.write_text(json.dumps(session, indent=2, default=str))
    log.info("[kite_session] session saved to %s", SESSION_PATH)


def load_session() -> dict | None:
    if not SESSION_PATH.exists():
        return None
    try:
        return json.loads(SESSION_PATH.read_text())
    except Exception as e:
        log.warning("[kite_session] failed to load session: %s", e)
        return None


def handle_callback(request_token: str) -> dict:
    """
    Called from /api/kite/callback.
    Exchanges request_token -> access_token and persists it.
    """
    kite = _new_kite()
    data = kite.generate_session(request_token, SETTINGS.kite.api_secret)
    # data contains: access_token, public_token, user_id, etc.

    session = {
        "api_key": SETTINGS.kite.api_key,
        "access_token": data["access_token"],
        "public_token": data.get("public_token"),
        "user_id": data["user_id"],
        "login_time": dt.datetime.now().isoformat(),
        # optional: expiry; Kite sessions are per-day
    }
    save_session(session)
    return session


def get_authorized_kite() -> KiteConnect:
    """
    Used by live engine / tick / OMS.
    Raises NotAuthenticated if no valid session.
    """
    sess = load_session()
    if not sess or "access_token" not in sess:
        raise NotAuthenticated("No Kite session on disk")

    kite = _new_kite()
    kite.set_access_token(sess["access_token"])
    return kite


def kite_status() -> dict:
    sess = load_session()
    if not sess:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "user_id": sess.get("user_id"),
        "login_time": sess.get("login_time"),
    }
