#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Activate venv if present
source .venv/bin/activate || true
# Ensure MODE=live; server reads .env for the rest
export MODE=live
echo "[run_live] MODE=$MODE BAR_SECONDS=${BAR_SECONDS:-300}"
python -m ws.server
