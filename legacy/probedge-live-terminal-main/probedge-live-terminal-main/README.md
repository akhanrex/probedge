# Probedge Locked Build (CTO-aligned)

Clock → Tags → Picker → Plan → OMS → WS/UI. Hard IST cutovers; picker-led; tick-based entry.

## What changed
- Hard time locks: 09:25 PDC, 09:30 OL, 09:39:50 OT, 09:40 arm, 15:05 exit.
- Picker L3→L0 with gates, OT alignment and confidence ≥ 55.
- Entry default: 5th-bar break. Optional: 6–10 prev-bar break.
- LIMIT-only OMS (paper simulation); live broker stub.
- WS always emits state when ticks arrive.

## Run (paper)
```bash
bash ops/run_local.sh
```

Send ticks via WebSocket `/ws/ticks` payload:
```json
{
  "symbol": "TATAMOTORS",
  "ltp": 885.5,
  "ctx": {
    "bar5_high": 890.0,
    "bar5_low": 880.0,
    "freq": {"L3":{"BULL":9,"BEAR":6},"L2":{"BULL":8,"BEAR":6},"L1":{"BULL":10,"BEAR":9},"L0":{"BULL":20,"BEAR":18}}
  }
}
```

## Requirements
- Python 3.10+
- `pip install -r requirements.txt`

Notes:
- `probedge.core.classifiers` must exist (we do not overwrite your core file).
- Ensure `./data` and master frequency builder feed `ctx.freq` as shown.
