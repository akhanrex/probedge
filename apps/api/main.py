from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Route modules
from apps.api.routes import config as config_route
from apps.api.routes import tm5 as tm5_route
from apps.api.routes import matches as matches_route
from apps.api.routes import journal as journal_route
from apps.api.routes import state_file as state_file_route
from apps.api.routes import settings_debug as settings_debug_route
from apps.api.routes import plan as plan_route  # /api/plan

app = FastAPI(title="ProbEdge API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

@app.get("/api/health")
def health():
    return {"ok": True}

# Register routers
app.include_router(config_route.router)
app.include_router(tm5_route.router)
app.include_router(matches_route.router)
app.include_router(journal_route.router)
app.include_router(state_file_route.router)
app.include_router(settings_debug_route.router)
app.include_router(plan_route.router)  # ensure /api/plan is live
