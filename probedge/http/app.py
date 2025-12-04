# probedge/http/app.py

from pathlib import Path
import json
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from probedge.http import api_state   # import the router we just created

app = FastAPI()
STATE_PATH = Path("data/state/live_state.json")

# 1) API routes
app.include_router(api_state.router)

# 2) Static + live.html

# Assume your repo structure is:
#   /probedge
#     /webui
#       live.html
#       /css/terminal.css
#       /js/live.js
REPO_ROOT = Path(__file__).resolve().parents[2]
WEBUI_DIR = REPO_ROOT / "webui"

# --- NEW: mount CSS and JS exactly where live.html expects them ---

# CSS under /css → webui/css
app.mount(
    "/css",
    StaticFiles(directory=WEBUI_DIR / "css"),
    name="css",
)

# JS under /js → webui/js
app.mount(
    "/js",
    StaticFiles(directory=WEBUI_DIR / "js"),
    name="js",
)

# Optional: keep /static if something else uses it
app.mount(
    "/static",
    StaticFiles(directory=WEBUI_DIR),
    name="static",
)

# Serve the live grid HTML at "/"
@app.get("/")
def live_root():
    return FileResponse(WEBUI_DIR / "live.html")


@app.get("/api/health")
def api_health():
    """
    Simple health stub for the WebUI header.
    """
    if not STATE_PATH.exists():
        return {
            "system_status": "WARN",
            "status": "WARN",
            "ok": False,
            "reason": "state file not ready; run daily_timeline / live agg",
            "last_state_ts": None,
        }

    mtime = datetime.fromtimestamp(STATE_PATH.stat().st_mtime).isoformat()
    return {
        "system_status": "OK",
        "status": "OK",
        "ok": True,
        "reason": "state file present (compat stub)",
        "last_state_ts": mtime,
    }


@app.get("/api/state_raw")
def api_state_raw():
    """
    Compatibility endpoint for the terminal UI.

    - Reads data/state/live_state.json
    - If it finds 'portfolio_plan', it flattens it so the top-level
      has: date, mode, daily_risk_rs, active_trades, risk_per_trade_rs,
      total_planned_risk_rs, plans, etc.
    - Also keeps 'symbols', 'sim', 'sim_clock' if present.
    """
    if not STATE_PATH.exists():
        return {"error": "state file not ready"}

    with STATE_PATH.open() as f:
        data = json.load(f)

    # New-style state: { "mode": ..., "portfolio_plan": {...}, "symbols": {...}, "sim": ... }
    if isinstance(data, dict) and isinstance(data.get("portfolio_plan"), dict):
        flat = {k: v for k, v in data.items() if k != "portfolio_plan"}
        flat.update(data["portfolio_plan"])
        return flat

    # Old-style state or anything else -> just return as-is
    return data
