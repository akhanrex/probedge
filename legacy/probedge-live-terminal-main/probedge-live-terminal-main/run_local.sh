#!/usr/bin/env bash
set -euo pipefail
export MODE=paper
export SYMBOLS="TATAMOTORS,LT,SBIN"
export HOST=127.0.0.1
export PORT=9002
python -m ws.server
