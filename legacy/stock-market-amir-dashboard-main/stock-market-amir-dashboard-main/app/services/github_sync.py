# app/services/github_sync.py — GitHub push/upload helpers

from __future__ import annotations

import os
import base64
import hashlib
import json
import inspect
from typing import Tuple

import streamlit as st
from app.config import get_secret_first

# requests is optional on some hosts; we guard its usage
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

# Optional: repo helper (if present in your codebase)
try:
    from probedge.updater.github import push_to_github  # type: ignore
except Exception:
    push_to_github = None  # type: ignore


def _get_github_secrets() -> Tuple[dict, list]:
    """
    Read GitHub config from Streamlit secrets or environment.
    Returns (config_dict, missing_keys_list). 'token' and 'repo' are required.
    """
    gh = {
        "token": get_secret_first("github_token", "GITHUB_TOKEN", default=None),
        "repo": get_secret_first("github_repo", "GITHUB_REPO", default=None),
        "branch": get_secret_first("github_branch", "GITHUB_BRANCH", default="main"),
        "user": get_secret_first("github_user", "GITHUB_USER", default=None),
        "email": get_secret_first("github_email", "GITHUB_EMAIL", default=None),
    }
    missing = [k for k in ("token", "repo") if not gh.get(k)]
    return gh, missing


def _push_git_compat(*, repo_root: str, commit_message: str, paths: list[str]) -> bool:
    """
    Best-effort compatibility wrapper for a repo-level push helper.
    Returns True if the helper reports success, otherwise False.
    Safe to use on Streamlit Cloud (will just return False).
    """
    if not push_to_github:
        return False

    try:
        sig = inspect.signature(push_to_github)
        params = list(sig.parameters.values())
        names = [p.name for p in params]

        # Most flexible: keyword form
        if {"repo_root", "commit_message", "paths"}.issubset(set(names)):
            return bool(
                push_to_github(
                    repo_root=repo_root, commit_message=commit_message, paths=paths
                )
            )

        # Try positional 3-arg variants
        if len(params) >= 3:
            third = names[2].lower()
            third_arg = paths if ("paths" in third) else (paths[0] if paths else "")
            try:
                return bool(push_to_github(repo_root, commit_message, third_arg))
            except TypeError:
                pass

        # Try positional 2-arg variants
        if len(params) == 2:
            p0, p1 = names[0].lower(), names[1].lower()
            if any(k in p0 for k in ("message", "commit_message", "msg")):
                second = paths if ("paths" in p1) else (paths[0] if paths else "")
                return bool(push_to_github(commit_message, second))
            if any(k in p1 for k in ("message", "commit_message", "msg")):
                first = paths if ("paths" in p0) else (paths[0] if paths else "")
                return bool(push_to_github(first, commit_message))
            try:
                return bool(push_to_github(commit_message, paths))
            except Exception:
                return bool(push_to_github(paths[0] if paths else "", commit_message))

        # Unknown signature
        return False

    except Exception as e:
        try:
            st.warning(f"Git push helper incompatible: {e}")
        except Exception:
            pass
        return False


def _push_via_github_api(
    *,
    repo: str,
    token: str,
    path_in_repo: str,
    local_file: str,
    branch: str = "main",
    message: str = "",
) -> bool:
    """
    Uploads/updates a file using GitHub Contents API.
    Returns True on success. If 'requests' isn't available or inputs are invalid, returns False.
    """
    if not repo or not token or not path_in_repo or not local_file:
        return False
    if not os.path.exists(local_file):
        return False
    if requests is None:
        try:
            st.warning("GitHub push: 'requests' not installed; skipping upload.")
        except Exception:
            pass
        return False

    # Normalize path for GitHub
    path_in_repo = path_in_repo.replace("\\", "/").lstrip("/")

    # Read local file
    try:
        with open(local_file, "rb") as f:
            raw_bytes = f.read()
    except Exception:
        return False

    encoded = base64.b64encode(raw_bytes).decode("utf-8")

    # Build request details
    try:
        owner, name = repo.split("/", 1)
    except ValueError:
        return False

    base_url = f"https://api.github.com/repos/{owner}/{name}/contents/{path_in_repo}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }

    # Check if file exists to obtain its SHA (required for update)
    sha = None
    r_get = requests.get(base_url, headers=headers, params={"ref": branch})
    if r_get.status_code == 200:
        try:
            remote = r_get.json()
            sha = remote.get("sha")
            # Optional optimization: skip if content identical
            remote_b64 = remote.get("content")
            if remote_b64:
                try:
                    # GitHub returns content with newlines; strip them before decode
                    import re as _re
                    remote_bytes = base64.b64decode(_re.sub(r"\s+", "", remote_b64))
                    if hashlib.sha256(remote_bytes).hexdigest() == hashlib.sha256(raw_bytes).hexdigest():
                        return True  # no change; treat as success
                except Exception:
                    pass
        except Exception:
            sha = None
    elif r_get.status_code not in (200, 404):
        # Unexpected error from GET
        return False

    payload = {
        "message": message or f"Update {path_in_repo}",
        "content": encoded,
        "branch": branch,
    }
    if sha:
        payload["sha"] = sha

    r_put = requests.put(base_url, headers=headers, data=json.dumps(payload))
    return r_put.status_code in (200, 201)
