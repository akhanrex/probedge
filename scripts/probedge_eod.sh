#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source .venv/bin/activate

DAY="${1:-$(date +%F)}"

echo "[probedge_eod] Running paper_exec_from_journal for day=${DAY}"
if ! python -m apps.runtime.paper_exec_from_journal --day="${DAY}"; then
  echo "[probedge_eod] WARNING: paper_exec_from_journal failed for day=${DAY}. Check logs, continuing to fills_to_daily." >&2
fi

echo "[probedge_eod] Rebuilding fills_daily summary"
if ! python -m apps.runtime.fills_to_daily; then
  echo "[probedge_eod] ERROR: fills_to_daily failed." >&2
  exit 1
fi

echo "[probedge_eod] Done."
