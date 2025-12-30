#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
source .venv/bin/activate
export MODE=live
uvicorn apps.api.main:app --host 127.0.0.1 --port 8000 --reload
