# ProbEdge Terminal — Production Cutover v1 (Bundle)

This bundle contains **drop‑in files** you can unzip at the **repo root**.
It adds:
- `/api/state/stream` (SSE live state)
- `/api/arm` and `/api/stop` controls
- Paper loop writer for `live_state.json`
- Minimal Terminal UI (SSE + controls)

## Files

```
apps/api/routes/control.py
apps/api/routes/state_stream.py
apps/api/static/terminal.html
apps/api/static/terminal.js
probedge/storage/atomic_json.py
probedge/ops/run_paper_loop.py
tests/test_controls_and_sse.py
README_PRODUCTION_CUTOVER.md
```

## Install

1) Unzip at the **repo root** (same folder where `apps/` and `probedge/` live).

2) Ensure your `config/frequency.yaml` already has:
```yaml
paths:
  intraday: "data/intraday/{sym}_5minute.csv"
  masters: "data/masters/{sym}_5MINUTE_MASTER.csv"
  journal: "legacy/stock-market-amir-dashboard-main/stock-market-amir-dashboard-main/data/journal/Journal.csv"
  state: "live_state.json"
```

3) Include routers in `apps/api/main.py` (add once if not already present):
```python
from apps.api.routes import control as control_route
from apps.api.routes import state_stream as state_stream_route

app.include_router(control_route.router)
app.include_router(state_stream_route.router)
```

4) Run dev:
```bash
export MODE=paper
PYTHONPATH=$(pwd) uvicorn apps.api.main:app --host 127.0.0.1 --port 9002 --reload
# In another tab:
PYTHONPATH=$(pwd) python -m probedge.ops.run_paper_loop
```

5) Check:
```
curl -s http://127.0.0.1:9002/api/health
curl -s http://127.0.0.1:9002/api/settings | jq .
curl -s http://127.0.0.1:9002/api/state | jq .
curl -s http://127.0.0.1:9002/api/state/stream   # (open in browser to see SSE)
```

6) UI:
Open `http://127.0.0.1:9002/terminal.html` — Arm/Stop updates appear under **Controls** and quotes update via **SSE** heartbeat.

## Notes
- The paper loop writes only `heartbeat` + last `Close` per symbol from your 5‑minute CSVs. Your live/OMS logic can gradually replace this loop while keeping the same `live_state.json` contract.
- All writes to state are atomic (`*.tmp` + `os.replace`).
