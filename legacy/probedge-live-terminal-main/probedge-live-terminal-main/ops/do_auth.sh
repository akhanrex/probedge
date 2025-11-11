#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate || true
echo "[auth] starting local callback server on 127.0.0.1:8999 ..."
python -m ops.auth_server &
PID=$!
sleep 1
echo "[auth] opening Kite login ..."
curl -s "http://127.0.0.1:8999/api/auth/start" >/dev/null || true
echo "[auth] After you login and approve, this window will say 'saved'."
wait $PID || true
