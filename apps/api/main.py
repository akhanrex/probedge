import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from probedge.infra.settings import SETTINGS
from apps.api.routes import config as config_route
from apps.api.routes import tm5 as tm5_route
from apps.api.routes import matches as matches_route
from apps.api.routes import plan as plan_route
# (Other routes like journal/state/plan will be added in later phases)

app = FastAPI(title="ProbEdge API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if SETTINGS.allowed_origins == ["*"] else SETTINGS.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(config_route.router)
app.include_router(tm5_route.router)
app.include_router(matches_route.router)
app.include_router(plan_route.router)

if __name__ == "__main__":
    uvicorn.run("apps.api.main:app", host="0.0.0.0", port=9002, reload=True)
