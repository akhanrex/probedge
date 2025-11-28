# probedge/http/app.py

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from probedge.http import api_state   # import the router we just created

app = FastAPI()

# 1) API routes
app.include_router(api_state.router)

# 2) Static + live.html

# Assume your repo structure is:
#   /probedge-main
#     /webui
#       live.html
#       /js/live.js
#       /css/terminal.css
REPO_ROOT = Path(__file__).resolve().parents[2]
WEBUI_DIR = REPO_ROOT / "webui"

# Serve JS/CSS under /static
app.mount("/static", StaticFiles(directory=WEBUI_DIR), name="static")

# Serve the live grid HTML at "/"
@app.get("/")
def live_root():
    return FileResponse(WEBUI_DIR / "live.html")
