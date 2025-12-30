#!/usr/bin/env bash
set -e

echo "[Probedge] Day start script"

# --- locate repo root and activate venv ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

echo "[Probedge] Activating venv..."
if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
else
  echo "[Probedge] ERROR: .venv not found. Create venv first."
  exit 1
fi

mkdir -p logs

export ENABLE_AGG5=false

# --- sanity: jq required ---
if ! command -v jq >/dev/null 2>&1; then
  echo "[Probedge] ERROR: jq is required but not installed."
  exit 1
fi

# --- print settings (mode + data_dir) ---
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

# --- clean up any old processes first ---
echo "[Probedge] Cleaning old Phase A + API processes..."
pkill -f "python -m apps.runtime.run_phase_a" || true
pkill -f "uvicorn apps.api.main:app" || true

# --- STEP 0: ensure valid Kite session for today ---
echo "[Probedge] Step 0/4 – Ensuring Kite session for today..."

# start API just for auth/login
uvicorn apps.api.main:app --host 0.0.0.0 --port 9002 > logs/probedge_api_login.log 2>&1 &
UVICORN_LOGIN_PID=$!
sleep 2

AUTH_STATUS=$(curl -s 'http://127.0.0.1:9002/api/auth/status' || echo '')
HAS_VALID=$(echo "${AUTH_STATUS}" | jq -r '.has_valid_token_today // empty')

if [[ "${HAS_VALID}" != "true" ]]; then
  echo "[Probedge] No valid Kite session for today."
  echo "[Probedge] 1) In browser, open:  http://127.0.0.1:9002/login"
  echo "[Probedge] 2) Enter PIN, click 'Connect to Kite', complete Zerodha login."
  echo "[Probedge] 3) Come back here; script will auto-detect when login is done."
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
    kill "${UVICORN_LOGIN_PID}" || true
    exit 1
  fi
else
  echo "[Probedge] Existing valid Kite session found for today."
fi

# stop login-only API instance; we will start a fresh one for the day
kill "${UVICORN_LOGIN_PID}" || true
sleep 1


# --- STEP 0.5: rebuild tokens for current universe ---
# This makes universe reshuffles a 1-knob change (frequency.yaml).
echo "[Probedge] Step 0.5/4 – Building tokens_5min.csv for current universe..."
if python -m apps.runtime.build_tokens_5min >> logs/probedge_data.log 2>&1; then
  echo "[Probedge] Tokens build done."
else
  echo "[Probedge] ERROR during tokens build. See logs/probedge_data.log"
  exit 1
fi


# --- STEP 1: refresh intraday 5-min from Kite ---
echo "[Probedge] Step 1/4 – Refreshing intraday 5-min data (last 5 days) from Kite..."
if python -m apps.runtime.rebuild_intraday_5min_from_kite --days 5 >> logs/probedge_data.log 2>&1; then
  echo "[Probedge] Intraday refresh done."
else
  echo "[Probedge] ERROR during intraday refresh. See logs/probedge_data.log"
  exit 1
fi


# --- STEP 2: normalize intraday files ---
echo "[Probedge] Step 2/4 – Normalizing intraday files..."
if python -m apps.runtime.normalize_intraday_5min_files >> logs/probedge_data.log 2>&1; then
  echo "[Probedge] Intraday normalization done."
else
  echo "[Probedge] ERROR during intraday normalization. See logs/probedge_data.log"
  exit 1
fi

# --- STEP 3: rebuild masters from intraday ---
echo "[Probedge] Step 3/4 – Rebuilding masters (tags) from intraday..."
if python -m apps.runtime.rebuild_masters_from_intraday >> logs/probedge_data.log 2>&1; then
  echo "[Probedge] Masters rebuild done."
else
  echo "[Probedge] ERROR during masters rebuild. See logs/probedge_data.log"
  exit 1
fi

# --- STEP 4: data QC ---
echo "[Probedge] Step 4/4 – Running data QC..."
if python -m apps.runtime.data_qc >> logs/probedge_data.log 2>&1; then
  echo "[Probedge] Data QC OK."
else
  echo "[Probedge] ERROR during data QC. See logs/probedge_data.log"
  exit 1
fi

# --- start Phase A spine (agg5 + planner + intraday paper) ---
echo "[Probedge] Starting Phase A spine (agg5 + planner + intraday paper)..."
python -m apps.runtime.run_phase_a > logs/probedge_phase_a.log 2>&1 &
PHASE_A_PID=$!
echo "[Probedge] Phase A started (PID ${PHASE_A_PID}), log: logs/probedge_phase_a.log"

# --- start API server for the trading day ---
echo "[Probedge] Starting API server on http://127.0.0.1:9002 ..."
uvicorn apps.api.main:app --host 0.0.0.0 --port 9002 > logs/probedge_api.log 2>&1 &
API_PID=$!
echo "[Probedge] API started (PID ${API_PID}), log: logs/probedge_api.log"

echo
echo "[Probedge] Logs:"
echo "  tail -f logs/probedge_data.log"
echo "  tail -f logs/probedge_phase_a.log"
echo "  tail -f logs/probedge_api.log"
echo
echo "[Probedge] Now open:  http://127.0.0.1:9002/login"

