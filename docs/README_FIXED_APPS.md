# Fixed Probedge API (apps/)

Drop this `apps` folder into your project root (same level where you run: `uvicorn apps.api.main:app --reload`).

## Configure
Set environment variables (or rely on defaults):
- `MODE` = paper|live (default paper)
- `BAR_SECONDS` = 300
- `DATA_DIR` = path to your data directory (default `data`)
- `SYMBOLS` = comma-separated symbols (default the 10-stock basket)

The API will try multiple path patterns for tm5 and master files, for compatibility with older layouts.

## Run
```bash
uvicorn apps.api.main:app --reload --port 9002
# then try:
curl 'http://127.0.0.1:9002/api/health'
curl 'http://127.0.0.1:9002/api/config'
curl 'http://127.0.0.1:9002/api/tm5?symbol=TATAMOTORS'
curl 'http://127.0.0.1:9002/api/matches?symbol=HAL&ot=BULL&ol=OAR&pdc=TR'
curl 'http://127.0.0.1:9002/api/plan?symbol=JSWENERGY'
```
