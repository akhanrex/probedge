# ops_kite_auth_cli.py
#
# CLI helper to fetch today's KITE_ACCESS_TOKEN and update .env automatically.
#
# Usage (daily before market):
#   cd /path/to/probedge
#   source .venv/bin/activate
#   python ops_kite_auth_cli.py
#
# It will:
#   1) Read KITE_API_KEY / KITE_API_SECRET from .env
#   2) Print Kite login URL -> you open in browser and login
#   3) Ask you to paste the request_token from redirect URL
#   4) Call generate_session(...) to get access_token
#   5) Update KITE_ACCESS_TOKEN=... inside .env
#   6) Optionally write full session JSON to KITE_SESSION_FILE

from __future__ import annotations

import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from kiteconnect import KiteConnect


REPO_ROOT = Path(__file__).resolve().parents[0]
ENV_PATH = REPO_ROOT / ".env"


def update_env_access_token(env_path: Path, access_token: str) -> None:
    """Replace or append KITE_ACCESS_TOKEN=... in .env."""
    if env_path.exists():
        text = env_path.read_text()
    else:
        text = ""

    line = f"KITE_ACCESS_TOKEN={access_token}"

    if "KITE_ACCESS_TOKEN=" in text:
        # replace existing line
        text = re.sub(r"^KITE_ACCESS_TOKEN=.*$", line, text, flags=re.MULTILINE)
    else:
        # append new line
        if not text.endswith("\n"):
            text += "\n"
        text += line + "\n"

    env_path.write_text(text)
    print(f"[auth] Updated {env_path} with new KITE_ACCESS_TOKEN.")


def save_session_json(session_file: Path, session_data: dict) -> None:
    """Save session JSON, converting datetimes to strings."""
    session_file.parent.mkdir(parents=True, exist_ok=True)
    with session_file.open("w") as f:
        json.dump(session_data, f, indent=2, sort_keys=True, default=str)
    print(f"[auth] Saved full session to {session_file}")



def main() -> None:
    print("[auth] Loading .env from", ENV_PATH)
    load_dotenv(dotenv_path=ENV_PATH, override=False)

    api_key = os.getenv("KITE_API_KEY", "").strip()
    api_secret = os.getenv("KITE_API_SECRET", "").strip()
    session_file = os.getenv("KITE_SESSION_FILE", "").strip()

    if not api_key or not api_secret:
        raise SystemExit(
            "Set KITE_API_KEY and KITE_API_SECRET in .env before running this."
        )

    kite = KiteConnect(api_key=api_key)
    login_url = kite.login_url()
    print("\n1) Open this URL in your browser and complete login:\n")
    print(login_url)
    print("\n2) After login, Zerodha will redirect you to a URL like:")
    print("   https://your-redirect-url/?request_token=XXXX&action=login&status=success")
    print("   Copy the 'request_token' value (XXXX).\n")

    request_token = input("3) Paste request_token here: ").strip()
    if not request_token:
        raise SystemExit("No request_token provided.")

    print("\n[auth] Generating session with Kite...")
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    print("[auth] Got access_token:", access_token)

    # Update .env
    update_env_access_token(ENV_PATH, access_token)

    # Save full session if configured
    if session_file:
        save_session_json(Path(session_file), data)

    print("\n[auth] Done. You can now start agg5 / live feed using this token.")


if __name__ == "__main__":
    main()

