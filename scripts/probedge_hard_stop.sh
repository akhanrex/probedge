#!/usr/bin/env bash
set -euo pipefail

pkill -TERM -f "python -m apps.runtime.run_phase_a" 2>/dev/null || true
pkill -TERM -f "uvicorn apps.api.main:app" 2>/dev/null || true

for port in 9002 9102; do
  pids=$(lsof -ti tcp:$port 2>/dev/null || true)
  if [ -n "${pids:-}" ]; then
    kill -TERM $pids 2>/dev/null || true
  fi
done

sleep 1

pkill -KILL -f "python -m apps.runtime.run_phase_a" 2>/dev/null || true
pkill -KILL -f "uvicorn apps.api.main:app" 2>/dev/null || true

for port in 9002 9102; do
  pids=$(lsof -ti tcp:$port 2>/dev/null || true)
  if [ -n "${pids:-}" ]; then
    kill -KILL $pids 2>/dev/null || true
  fi
done

echo "OK: hard stop done"
