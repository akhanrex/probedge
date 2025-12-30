from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from probedge.infra.settings import SETTINGS


@dataclass
class KiteSession:
    access_token: str
    valid_for_day: str  # "YYYY-MM-DD"
    user_id: Optional[str] = None

    @property
    def as_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "valid_for_day": self.valid_for_day,
            "user_id": self.user_id,
        }


def _session_path() -> Path:
    """
    data/secrets/kite_session.json under the repo data_dir.
    """
    base = SETTINGS.data_dir
    secrets_dir = base / "data" / "secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)
    return secrets_dir / "kite_session.json"


def load_session() -> Optional[KiteSession]:
    """
    Return current KiteSession if file exists and is valid JSON, else None.
    """
    p = _session_path()
    if not p.exists():
        return None
    try:
        raw = json.loads(p.read_text())
    except Exception:
        return None

    access_token = raw.get("access_token")
    valid_for_day = raw.get("valid_for_day")
    user_id = raw.get("user_id")
    if not access_token or not valid_for_day:
        return None

    return KiteSession(
        access_token=str(access_token),
        valid_for_day=str(valid_for_day),
        user_id=str(user_id) if user_id is not None else None,
    )


def save_session(access_token: str, valid_for_day: str, user_id: Optional[str] = None) -> KiteSession:
    """
    Overwrite the session file with a new token valid for the given day.
    """
    sess = KiteSession(
        access_token=str(access_token),
        valid_for_day=str(valid_for_day),
        user_id=str(user_id) if user_id is not None else None,
    )
    p = _session_path()
    p.write_text(json.dumps(sess.as_dict, indent=2))
    return sess


def get_access_token_for_today(today: Optional[date] = None) -> Optional[str]:
    """
    Returns the access token if it is valid for 'today', else None.
    """
    today = today or date.today()
    today_str = today.isoformat()

    sess = load_session()
    if not sess:
        return None

    if sess.valid_for_day != today_str:
        return None

    return sess.access_token
