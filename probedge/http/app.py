# probedge/http/app.py

from pathlib import Path
import json
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from probedge.http import api_state  # existing router

app = FastAPI()
STATE_PATH = Path("data/state/live_state.json")

# === 1) API routes (existing) ===
app.include_router(api_state.router)

# === 2) Static + live.html ===

# Repo layout:
#   /probedge (project root)
#     /webui
#       live.html
#       /js/live.js
#       /css/terminal.css
REPO_ROOT = Path(__file__).resolve().parents[2]
WEBUI_DIR = REPO_ROOT / "webui"

# Serve static assets in the simplest, most compatible way:
# - /static/...  -> entire webui tree
# - /js/...      -> webui/js
# - /css/...     -> webui/css
app.mount("/static", StaticFiles(directory=WEBUI_DIR), name="static")
app.mount("/js", StaticFiles(directory=WEBUI_DIR / "js"), name="js")
app.mount("/css", StaticFiles(directory=WEBUI_DIR / "css"), name="css")

# Serve the live terminal HTML at "/"
@app.get("/")
def live_root():
    """
    Serve the Probedge live terminal.
    """
    return FileResponse(WEBUI_DIR / "live.html")


# === 3) Health endpoint – make it very generic/compatible ===
@app.get("/api/health")
def api_health():
    """
    Health stub for the WebUI header.

    We return multiple keys so old/new UIs can all understand:
    - system_status: "OK"/"WARN"
    - status:        same as system_status
    - ok:            bool
    - reason:        text
    - last_state_ts: ISO timestamp of live_state.json mtime (if present)
    """
    if not STATE_PATH.exists():
        return {
            "system_status": "WARN",
            "status": "WARN",
            "ok": False,
            "reason": "state file not ready; run timeline / playback",
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


# === 4) State endpoint – ultra-compatible bridge for the terminal UI ===
@app.get("/api/state_raw")
def api_state_raw():
    """
    Compatibility endpoint for the terminal UI.

    We try to support both shapes:

    A) New-style state (what apps.runtime.daily_timeline writes):
       {
         "mode": "paper",
         "portfolio_plan": {
            "date": "...",
            "mode": "...",
            "daily_risk_rs": ...,
            "active_trades": ...,
            "risk_per_trade_rs": ...,
            "total_planned_risk_rs": ...,
            "plans": [...]
         },
         "symbols": {...},
         "sim": true/false,
         "sim_clock": "...",
         "sim_day": "..."
       }

    B) Old-style state (already flattened).

    This endpoint will ALWAYS return:
      top-level:
        - mode
        - date
        - daily_risk_rs
        - active_trades
        - risk_per_trade_rs
        - total_planned_risk_rs
        - plans
        - symbols (if any)
        - sim, sim_clock, sim_day (if any)
      AND a nested "portfolio_plan" with the same plan fields for UIs that
      still look under portfolio_plan.
    """
    if not STATE_PATH.exists():
        return {"error": "state file not ready"}

    with STATE_PATH.open() as f:
        data = json.load(f)

    # If we already got a flat, old-style shape, normalize into a common structure.
    if "portfolio_plan" not in data and "plans" in data:
        # Treat `data` as already flat
        flat = dict(data)
        plan_fields = {
            "date": flat.get("date"),
            "mode": flat.get("mode"),
            "daily_risk_rs": flat.get("daily_risk_rs"),
            "active_trades": flat.get("active_trades"),
            "risk_per_trade_rs": flat.get("risk_per_trade_rs"),
            "total_planned_risk_rs": flat.get("total_planned_risk_rs"),
            "plans": flat.get("plans", []),
        }
        flat["portfolio_plan"] = plan_fields
        return flat

    # New-style: { mode, portfolio_plan:{...}, symbols..., sim... }
    if isinstance(data, dict) and isinstance(data.get("portfolio_plan"), dict):
        pp = data["portfolio_plan"]

        flat = {
            # top-level meta
            "mode": data.get("mode", pp.get("mode", "paper")),
            "sim": data.get("sim"),
            "sim_clock": data.get("sim_clock"),
            "sim_day": data.get("sim_day"),
            "symbols": data.get("symbols", {}),

            # flattened plan
            "date": pp.get("date"),
            "daily_risk_rs": pp.get("daily_risk_rs"),
            "active_trades": pp.get("active_trades"),
            "risk_per_trade_rs": pp.get("risk_per_trade_rs"),
            "total_planned_risk_rs": pp.get("total_planned_risk_rs"),
            "plans": pp.get("plans", []),
        }

        # Also keep a nested portfolio_plan for old JS
        flat["portfolio_plan"] = {
            "date": flat["date"],
            "mode": flat["mode"],
            "daily_risk_rs": flat["daily_risk_rs"],
            "active_trades": flat["active_trades"],
            "risk_per_trade_rs": flat["risk_per_trade_rs"],
            "total_planned_risk_rs": flat["total_planned_risk_rs"],
            "plans": flat["plans"],
        }

        return flat

    # Fallback: just return the raw data
    return data
