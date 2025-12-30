# apps/api/main.py
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from probedge.infra.settings import SETTINGS

from .routes.health import router as health_router
from .routes.config import router as config_router
from .routes.tm5 import router as tm5_router
from .routes.matches import router as matches_router
from .routes.plan import router as plan_router
from .routes.state import router as state_router
from apps.api.routes import auth as auth_routes
from .routes.risk import router as risk_router
from .routes.freq3 import router as freq3_router
from .routes.superpath import router as superpath_router
from .routes.journal import router as journal_router
from .routes.plan_snapshot import router as plan_snapshot_router


app = FastAPI(title="Probedge API")



# --- CORS ---
origins = SETTINGS.allowed_origins or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_routes.router)

# --- Paths ---
this_dir = Path(__file__).resolve().parent
api_static_dir = this_dir / "static"

# repo root: .../probedge/probedge (the directory that contains `apps/`, `probedge/`, `webui/`)
repo_root = this_dir.parents[1]
webui_dir = repo_root / "webui"
manual_dir = webui_dir / "manual"



# --- Static mounts ---
# Legacy debug assets
app.mount("/static", StaticFiles(directory=api_static_dir), name="static")

# New web UI assets (HTML/JS/CSS)
app.mount("/webui-static", StaticFiles(directory=webui_dir), name="webui-static")

# Legacy manual terminal expects assets under /webui/...
app.mount("/webui", StaticFiles(directory=webui_dir), name="webui")



# --- HTML entrypoints ---

@app.get("/", include_in_schema=False)
async def root():
    """Default entry – auth-aware login page."""
    return FileResponse(webui_dir / "login.html")


@app.get("/login", include_in_schema=False)
async def login_page():
    """Explicit login URL alias (same as /)."""
    return FileResponse(webui_dir / "login.html")




@app.get("/live", include_in_schema=False)
async def live_page():
    """Alias path for the live terminal UI."""
    return FileResponse(webui_dir / "live.html")

@app.get("/manual", include_in_schema=False)
async def manual_terminal_page():
    """Legacy manual frequency terminal UI."""
    return FileResponse(webui_dir / "pages" / "terminal.html")

@app.get("/debug", include_in_schema=False)
async def debug_page():
    """Legacy debug console."""
    return FileResponse(api_static_dir / "terminal_debug.html")


# --- API routers ---
# Routers already declare their own /api/... prefixes.
app.include_router(health_router)
app.include_router(config_router)
app.include_router(tm5_router)
app.include_router(matches_router)
app.include_router(plan_router)
app.include_router(state_router)
app.include_router(plan_snapshot_router)
app.include_router(risk_router)
app.include_router(journal_router)
app.include_router(superpath_router)
app.include_router(freq3_router)
