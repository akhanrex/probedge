#!/usr/bin/env bash
set -euo pipefail

# cd to repo root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( dirname "$SCRIPT_DIR" )"
cd "$ROOT_DIR"

# load .env (if present)
if [[ -f ".env" ]]; then
  set -a
  source .env
  set +a
fi

# ensure data dir exists
: "${DATA_DIR:=./data}"
mkdir -p "$DATA_DIR"

# make sure PYTHONPATH is safe under `set -u`
PYTHONPATH="${PYTHONPATH:-}"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

echo "[run_local] MODE=${MODE:-paper} SYMBOLS=${SYMBOLS:-TATAMOTORS,LT,SBIN} BAR_SECONDS=${BAR_SECONDS:-10}"
exec python -m ws.server
