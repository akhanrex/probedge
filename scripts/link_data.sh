#!/usr/bin/env bash
set -e

# ==== EDIT THESE TWO PATHS TO MATCH YOUR REAL REPOS ====
LIVE_REPO_ROOT="$HOME/Trading/probedge-live-terminal-main"      # intraday CSVs
WEBAPP_REPO_ROOT="$HOME/Trading/stock-market-amir-dashboard"    # MASTER CSVs
# =======================================================

MONO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "$MONO_ROOT/data/intraday"
mkdir -p "$MONO_ROOT/data/masters"
mkdir -p "$MONO_ROOT/data/state"
mkdir -p "$MONO_ROOT/data/journal"

# Universe knob (single source of truth)
# Reads config/frequency.yaml via SETTINGS.symbols.
mapfile -t SYMS < <(python - <<'PY'
from probedge.infra.settings import SETTINGS
for s in SETTINGS.symbols:
    print(s)
PY
)

echo "Linking intraday + master files into monorepo data/ ..."

for S in "${SYMS[@]}"; do
  # ---- intraday ----
  CAND_INTRA=(
    "$LIVE_REPO_ROOT/data/intraday/${S}_5minute.csv"
    "$LIVE_REPO_ROOT/data/intraday/${S}_5MINUTE.csv"
    "$LIVE_REPO_ROOT/data/${S}/${S}_5minute.csv"
  )

  SRC_INTRA=""
  for P in "${CAND_INTRA[@]}"; do
    if [ -f "$P" ]; then
      SRC_INTRA="$P"
      break
    fi
  done

  if [ -z "$SRC_INTRA" ]; then
    echo "WARN: no intraday file found for $S"
  else
    ln -sf "$SRC_INTRA" "$MONO_ROOT/data/intraday/${S}_5minute.csv"
    echo "OK intraday: $S -> $(realpath "$SRC_INTRA")"
  fi

  # ---- master ----
  CAND_MASTER=(
    "$WEBAPP_REPO_ROOT/data/masters/${S}_5MINUTE_MASTER.csv"
    "$WEBAPP_REPO_ROOT/data/masters/${S}_MASTER.csv"
    "$WEBAPP_REPO_ROOT/data/${S}/${S}_5MINUTE_MASTER.csv"
  )

  SRC_MASTER=""
  for P in "${CAND_MASTER[@]}"; do
    if [ -f "$P" ]; then
      SRC_MASTER="$P"
      break
    fi
  done

  if [ -z "$SRC_MASTER" ]; then
    echo "WARN: no MASTER file found for $S"
  else
    ln -sf "$SRC_MASTER" "$MONO_ROOT/data/masters/${S}_5MINUTE_MASTER.csv"
    echo "OK master: $S -> $(realpath "$SRC_MASTER")"
  fi
done

echo "Done."
