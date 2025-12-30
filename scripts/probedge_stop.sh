#!/usr/bin/env bash
set -euo pipefail

echo "[Probedge] Stopping Phase A + API..."

# Kill the known process patterns hard (they sometimes ignore TERM)
pkill -9 -f "apps.runtime.run_phase_a" 2>/dev/null || true
pkill -9 -f "uvicorn apps.api.main:app" 2>/dev/null || true

# Kill anything bound to ports (macOS)
for port in 9002 9102; do
  pids="$(lsof -ti tcp:$port 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    echo "[Probedge] Killing tcp:$port pids=$pids"
    kill -9 $pids 2>/dev/null || true
  fi
done

# Confirm
alive="$(pgrep -fl "apps.runtime.run_phase_a|uvicorn apps.api.main:app" || true)"
if [ -n "$alive" ]; then
  echo "[Probedge] WARN: still alive:"
  echo "$alive"
else
  echo "[Probedge] Stop done."
fi
