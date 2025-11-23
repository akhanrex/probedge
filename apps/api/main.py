# apps/api/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from probedge.infra.settings import SETTINGS

# Routers
from apps.api.routes.health import router as health_router
from apps.api.routes.config import router as config_router
from apps.api.routes.tm5 import router as tm5_router
from apps.api.routes.matches import router as matches_router
from apps.api.routes.plan import router as plan_router
from apps.api.routes.state import router as state_router


def create_app() -> FastAPI:
    app = FastAPI(title="ProbEdge API", version="0.1.0")

    # ---- CORS ----
    origins = SETTINGS.allowed_origins or ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Static files (terminal UI) ----
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

        @app.get("/", include_in_schema=False)
        async def root():
            # Serve the new debug terminal page
            return FileResponse(static_dir / "terminal_debug.html")

    # ---- REST routes ----
    app.include_router(health_router)
    app.include_router(config_router)
    app.include_router(tm5_router)
    app.include_router(matches_router)
    app.include_router(plan_router)
    app.include_router(state_router)

    return app


app = create_app()
