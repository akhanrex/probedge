#!/usr/bin/env bash
set -euo pipefail

echo "[Probedge] Day start script"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

echo "[Probedge] Activating venv..."
if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
else
  echo "[Probedge] ERROR: .venv not found."
  exit 1
fi

mkdir -p logs .pids

export ENABLE_AGG5="${ENABLE_AGG5:-true}"
export PB_ENABLE_KITE_TICKS="${PB_ENABLE_KITE_TICKS:-1}"
export PYTHONUNBUFFERED=1

RESET_STATE="${RESET_STATE:-0}"   # debug only

# --- sanity: jq required ---
if ! command -v jq >/dev/null 2>&1; then
  echo "[Probedge] ERROR: jq is required but not installed."
  exit 1
fi

MODE=$(python - << 'PY'
from probedge.infra.settings import SETTINGS
print(SETTINGS.mode)
PY
)

DATA_DIR=$(python - << 'PY'
from probedge.infra.settings import SETTINGS
print(SETTINGS.data_dir)
PY
)

echo "[Probedge] MODE=${MODE}"
echo "[Probedge] DATA_DIR=${DATA_DIR}"

wait_port_free () {
  local port="$1"
  for i in {1..30}; do
    if ! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

# --- clean up any old processes first ---
echo "[Probedge] Cleaning old processes..."
RESET_STATE=0 "${SCRIPT_DIR}/probedge_stop.sh" || true

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

# --- STEP 0: ensure valid Kite session for today ---
echo "[Probedge] Step 0/4 – Ensuring Kite session for today..."

# start API just for auth/login
uvicorn apps.api.main:app --host 127.0.0.1 --port 9002 > logs/probedge_api_login.log 2>&1 &
UVICORN_LOGIN_PID=$!
echo "${UVICORN_LOGIN_PID}" > .pids/api.pid

# cleanup login API if script exits early
cleanup() {
  kill "${UVICORN_LOGIN_PID}" 2>/dev/null || true
}
trap cleanup EXIT

sleep 1
if ! wait_port_free 9002; then
  # port is in use -> still OK, it means uvicorn is up
  true
fi

AUTH_STATUS=$(curl -s 'http://127.0.0.1:9002/api/auth/status' || echo '')
HAS_VALID=$(echo "${AUTH_STATUS}" | jq -r '.has_valid_token_today // empty')

if [[ "${HAS_VALID}" != "true" ]]; then
  echo "[Probedge] No valid Kite session for today."
  echo "[Probedge] Open in browser:  http://127.0.0.1:9002/login"
  echo "[Probedge] Waiting for Kite session to become valid (Ctrl+C to abort)..."

  for i in {1..60}; do
    sleep 3
    AUTH_STATUS=$(curl -s 'http://127.0.0.1:9002/api/auth/status' || echo '')
    HAS_VALID=$(echo "${AUTH_STATUS}" | jq -r '.has_valid_token_today // empty')
    if [[ "${HAS_VALID}" == "true" ]]; then
      echo "[Probedge] Kite session is now valid for today."
      break
    fi
    if (( i % 10 == 0 )); then
      echo "[Probedge] Still waiting for Kite login..."
    fi
  done

  if [[ "${HAS_VALID}" != "true" ]]; then
    echo "[Probedge] ERROR: Kite session not valid after waiting. Aborting."
    exit 1
  fi
else
  echo "[Probedge] Existing valid Kite session found for today."
fi

# stop login-only API instance; we will start a fresh one for the day
kill "${UVICORN_LOGIN_PID}" 2>/dev/null || true
rm -f .pids/api.pid
sleep 1
trap - EXIT

# --- STEP 0.5: rebuild tokens for current universe ---
echo "[Probedge] Step 0.5/4 – Building tokens_5min.csv..."
python -m apps.runtime.build_tokens_5min >> logs/probedge_data.log 2>&1

# --- STEP 1: refresh intraday 5-min from Kite ---
echo "[Probedge] Step 1/4 – Refreshing intraday 5-min data (last 5 days)..."
python -m apps.runtime.rebuild_intraday_5min_from_kite --days 5 >> logs/probedge_data.log 2>&1

# --- STEP 2: normalize intraday files ---
echo "[Probedge] Step 2/4 – Normalizing intraday files..."
python -m apps.runtime.normalize_intraday_5min_files >> logs/probedge_data.log 2>&1

# --- STEP 3: rebuild masters from intraday ---
echo "[Probedge] Step 3/4 – Rebuilding masters (tags) from intraday..."
python -m apps.runtime.rebuild_masters_from_intraday >> logs/probedge_data.log 2>&1

# --- STEP 4: data QC ---
echo "[Probedge] Step 4/4 – Running data QC..."
python -m apps.runtime.data_qc >> logs/probedge_data.log 2>&1
echo "[Probedge] Data QC OK."

# --- start Phase A runtime (agg5 + 09:40 plan snapshot writer etc.) ---
echo "[Probedge] Starting Phase A runtime..."
python -m apps.runtime.run_phase_a > logs/probedge_phase_a.log 2>&1 &
PHASE_A_PID=$!
echo "${PHASE_A_PID}" > .pids/phase_a.pid
echo "[Probedge] Phase A PID ${PHASE_A_PID} (logs/probedge_phase_a.log)"

# --- start API server for the trading day ---
echo "[Probedge] Starting API server on http://127.0.0.1:9002 ..."
uvicorn apps.api.main:app --host 127.0.0.1 --port 9002 > logs/probedge_api.log 2>&1 &
API_PID=$!
echo "${API_PID}" > .pids/api.pid
echo "[Probedge] API PID ${API_PID} (logs/probedge_api.log)"

# --- start intraday paper execution engine (separate process) ---
echo "[Probedge] Starting intraday paper engine..."
python -u -c "from apps.runtime.intraday_paper import run_intraday_paper_loop; run_intraday_paper_loop(2.0)" \
  > logs/probedge_intraday_paper.log 2>&1 &
PAPER_PID=$!
echo "${PAPER_PID}" > .pids/intraday_paper.pid
echo "[Probedge] intraday_paper PID ${PAPER_PID} (logs/probedge_intraday_paper.log)"

echo
echo "[Probedge] Logs:"
echo "  tail -f logs/probedge_data.log"
echo "  tail -f logs/probedge_phase_a.log"
echo "  tail -f logs/probedge_api.log"
echo "  tail -f logs/probedge_intraday_paper.log"
echo
echo "[Probedge] Open UI: http://127.0.0.1:9002/"
