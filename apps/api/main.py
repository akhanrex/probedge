from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os

# -------- app base --------
app = FastAPI(title="ProbEdge API", version="0.1.0")

# CORS (env ALLOWED_ORIGINS or "*")
origins_env = os.environ.get("ALLOWED_ORIGINS", "*")
allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health (keep it inline)
@app.get("/api/health")
def health():
    return {"ok": True}

# -------- routers --------
# Required
from apps.api.routes import tm5 as tm5_route
from apps.api.routes import config as config_route
from apps.api.routes import matches as matches_route
from apps.api.routes import settings_debug as settings_debug_route
from apps.api.routes import journal as journal_route

# Optional (guarded)
try:
    from apps.api.routes import state_file as state_route
    app.include_router(state_route.router)
except Exception:
    pass

# Include ours last (last wins)
app.include_router(config_route.router)
app.include_router(tm5_route.router)
app.include_router(matches_route.router)
app.include_router(settings_debug_route.router)
app.include_router(journal_route.router)

# ---- static (index.html / terminal.html)
app.mount("/", StaticFiles(directory="apps/api/static", html=True))
