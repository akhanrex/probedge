#!/usr/bin/env bash
set -e

# Always run from repo root
cd "$(dirname "$0")/.."

echo "=== Probedge – Daily Paper Ops ==="

# 1) Activate venv
if [ -f ".venv/bin/activate" ]; then
  echo "[step 1] Activating virtualenv"
  # shellcheck disable=SC1091
  source .venv/bin/activate
else
  echo "[error] .venv not found. Run: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

# Risk per day (default 10000 if not passed)
RISK_RS="${1:-10000}"
TODAY="$(date +%Y-%m-%d)"

echo "[info] Today = ${TODAY}, Daily risk = ₹${RISK_RS}"

echo
echo "=== step 2 – (Optional) refresh Kite token if expired ==="
echo "If you already refreshed token today, you can skip with Ctrl+C now."
echo "Otherwise, press Enter to run auth..."
read -r _

python ops_kite_auth_cli.py || {
  echo "[warn] Kite auth failed – if token is still valid from earlier, you can ignore."
}

echo
echo "=== step 3 – Backfill last 3 days intraday from Kite ==="
python ops/backfill_intraday_kite.py --days 3

echo
echo "=== step 4 – Rebuild recent MASTER files (last ~120 days) ==="
python ops/rebuild_master_recent.py

echo
echo "=== step 5 – System health check for today (${TODAY}) ==="
python ops/debug_system_health.py --days "${TODAY}" --risk "${RISK_RS}"

echo
echo "=== step 6 – Build plan for today (${TODAY}) in paper mode ==="
python -m apps.runtime.daily_timeline --day "${TODAY}" --risk "${RISK_RS}"

echo
echo "=== DONE – Backend ready for today in paper mode ==="
echo "Now make sure uvicorn FastAPI server is running on :9002,"
echo "then open the Live Terminal in your browser."

