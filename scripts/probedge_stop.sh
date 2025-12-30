#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

source .venv/bin/activate

PID_DIR=".pids"
mkdir -p "${PID_DIR}"

RESET_STATE="${RESET_STATE:-0}"   # use: RESET_STATE=1 ./scripts/probedge_stop.sh

echo "[Probedge] Stopping Phase A + intraday_paper + API..."

kill_pidfile () {
  local f="$1"
  if [[ -f "$f" ]]; then
    local pid
    pid="$(cat "$f" || true)"
    if [[ -n "${pid:-}" ]]; then
      echo "[Probedge] Killing PID $pid from $f"
      kill -TERM "$pid" 2>/dev/null || true
      sleep 0.3
      kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$f"
  fi
}

# Kill known pidfiles (if present)
kill_pidfile "${PID_DIR}/intraday_paper.pid"
kill_pidfile "${PID_DIR}/phase_a.pid"
kill_pidfile "${PID_DIR}/api.pid"

# Kill by patterns as safety net
pkill -9 -f "apps\.runtime\.run_phase_a" 2>/dev/null || true
pkill -9 -f "apps\.runtime\.intraday_paper" 2>/dev/null || true
pkill -9 -f "uvicorn.*apps\.api\.main:app" 2>/dev/null || true

# Kill anything bound to ports (macOS)
for port in 9002 9102; do
  pids="$(lsof -ti tcp:$port 2>/dev/null || true)"
  if [[ -n "$pids" ]]; then
    echo "[Probedge] Killing tcp:$port pids=$pids"
    kill -9 $pids 2>/dev/null || true
  fi
done

if [[ "${RESET_STATE}" == "1" ]]; then
  echo "[Probedge] RESET_STATE=1 -> wiping live_state.json"
  python - <<'PY'
from probedge.infra.settings import SETTINGS
from pathlib import Path
import json
p = Path(SETTINGS.paths.state)
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps({}, indent=2))
print("HARD_RESET_OK:", p)
PY
fi

# Confirm
alive="$(pgrep -fl "apps\.runtime\.run_phase_a|apps\.runtime\.intraday_paper|uvicorn.*apps\.api\.main:app" || true)"
if [[ -n "$alive" ]]; then
  echo "[Probedge] WARN: still alive:"
  echo "$alive"
else
  echo "[Probedge] Stop done."
fi
