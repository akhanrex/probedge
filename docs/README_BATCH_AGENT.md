
# ProbEdge — Batch-v1 Strategy + SL Integration

This bundle wires the *batch-colab logic* and the exact **SL rules** into your live terminal.

## What you get
- `probedge/decision/sl.py` — single source of truth for SL calculation (matches your description exactly).
- `probedge/decision/picker_batchv1.py` — robust 5-min reader + OpeningTrend + freq-based picker using the existing master CSVs.
- `probedge/ops/batch_agent.py` — a small loop that listens to `/api/arm` (via `live_state.json`) and computes the full plan (Entry, SL, T1/T2, Qty) for the selected symbol.
- `apps/api/static/terminal_v2.html|js` — a cleaner terminal view that shows the plan.

## How to run
1. Start API (as you already do):
   ```bash
   export MODE=paper
   PYTHONPATH=$(pwd) uvicorn apps.api.main:app --host 127.0.0.1 --port 9002 --reload
   ```
2. Start the batch agent in a second tab:
   ```bash
   PYTHONPATH=$(pwd) python -m probedge.ops.batch_agent
   ```
3. Open the new UI:
   - `http://127.0.0.1:9002/terminal_v2.html`
   - Pick a symbol → **Arm**. Within ~1s the plan appears (reads TM5 + Master and applies the SL engine).

## Config knobs
- Risk per trade: env var `RISK_RS` (default ₹10,000)
- Paths and symbols: from your existing `config/frequency.yaml` (uses `paths.intraday`, `paths.masters`, `paths.state`).
- SL thresholds ("closeness"): see `DEFAULT_CLOSE_PCT=0.0025` and `DEFAULT_CLOSE_FR_ORB=0.20` in `sl.py`.

## Notes
- Entry is the **09:40 bar open**, matching the batch backtest. Targets are 1R / 2R.
- No OMS fills are sent yet; the agent only computes the plan and updates `live_state.json`. Hooking to OMS is a small next step.
