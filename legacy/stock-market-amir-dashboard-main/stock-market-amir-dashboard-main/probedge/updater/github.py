# github_sync.py
import base64
import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests
import streamlit as st


def _gh_headers():
    token = st.secrets.get("github_token")
    if not token:
        raise RuntimeError("Missing github_token in secrets.")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }


def _gh_repo_parts():
    repo = st.secrets.get("github_repo")
    branch = st.secrets.get("github_branch", "main")
    if not repo:
        raise RuntimeError("Missing github_repo in secrets.")
    owner, name = repo.split("/", 1)
    return owner, name, branch


def _get_file_info(path: str) -> Tuple[Optional[str], Optional[bytes]]:
    """Return (sha, raw_bytes) for file at path on GitHub, or (None, None) if not found."""
    owner, name, branch = _gh_repo_parts()
    url = f"https://api.github.com/repos/{owner}/{name}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=_gh_headers())
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    data = r.json()
    sha = data.get("sha")
    content_b64 = data.get("content", "")
    if content_b64:
        raw = base64.b64decode(content_b64)
    else:
        raw = None
    return sha, raw


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def push_if_changed(local_bytes: bytes, dest_path: str, commit_message: str) -> bool:
    """
    Uploads to GitHub only if content changed.
    Returns True if pushed, False if skipped (no change).
    """
    prev_sha, prev_bytes = _get_file_info(dest_path)
    new_hash = _sha256(local_bytes)
    old_hash = _sha256(prev_bytes) if prev_bytes is not None else None
    if old_hash == new_hash:
        return False  # no change

    owner, name, branch = _gh_repo_parts()
    url = f"https://api.github.com/repos/{owner}/{name}/contents/{dest_path}"
    payload = {
        "message": commit_message,
        "content": base64.b64encode(local_bytes).decode("utf-8"),
        "branch": branch,
    }
    if prev_sha:
        payload["sha"] = prev_sha

    r = requests.put(url, headers=_gh_headers(), data=json.dumps(payload))
    r.raise_for_status()
    return True


def now_ist():
    # IST = UTC+5:30
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


def debounce_ok(key: str, minutes: int = 10) -> bool:
    """Allow action if last attempt older than `minutes` (stored in st.session_state)."""
    last = st.session_state.get(key)
    tnow = time.time()
    if not last or (tnow - last) > minutes * 60:
        st.session_state[key] = tnow
        return True
    return False


def once_per_day_ok(key: str) -> bool:
    """True if we haven't executed yet today (based on IST day stamp)."""
    stamp = st.session_state.get(key)
    today = now_ist().date().isoformat()
    if stamp == today:
        return False
    st.session_state[key] = today
    return True


# ---- appended from github_utils.py ----
import os
import base64
import requests
import streamlit as st


def push_to_github(file_path: str, commit_message: str):
    """Push a local file to GitHub repo using REST API + Streamlit secrets."""
    token = st.secrets["github_token"]
    repo = st.secrets["github_repo"]
    branch = st.secrets.get("github_branch", "main")

    # Read file content
    with open(file_path, "rb") as f:
        content = f.read()
    encoded = base64.b64encode(content).decode()

    filename = os.path.basename(file_path)
    url = f"https://api.github.com/repos/{repo}/contents/{filename}"

    # Get SHA if file already exists
    r = requests.get(
        url, headers={"Authorization": f"token {token}"}, params={"ref": branch}
    )
    sha = r.json().get("sha") if r.status_code == 200 else None

    data = {
        "message": commit_message,
        "content": encoded,
        "branch": branch,
    }
    if sha:
        data["sha"] = sha

    r = requests.put(url, json=data, headers={"Authorization": f"token {token}"})
    if r.status_code in (200, 201):
        st.success(f"✅ {filename} pushed to GitHub {branch}.")
    else:
        st.error(f"❌ Push failed: {r.status_code} → {r.text}")
