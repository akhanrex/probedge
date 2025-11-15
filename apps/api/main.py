from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from probedge.infra.settings import SETTINGS

# Import routers directly from each module
from apps.api.routes.config import router as config_router
from apps.api.routes.tm5 import router as tm5_router
from apps.api.routes.matches import router as matches_router
from apps.api.routes.plan import router as plan_router
from apps.api.routes.state import router as state_router


def create_app() -> FastAPI:
    app = FastAPI()

    # CORS setup
    app.add_middleware(
        CORSMiddleware,
        allow_origins=getattr(SETTINGS, "allowed_origins", ["*"]),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST routes
    app.include_router(config_router)
    app.include_router(tm5_router)
    app.include_router(matches_router)
    app.include_router(plan_router)
    app.include_router(state_router)

    return app


app = create_app()
