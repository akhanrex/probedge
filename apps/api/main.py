from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apps.api.routes import config, tm5, matches, journal, plan, state, superpath

app = FastAPI(title="Probedge API", version="0.1.0")

# CORS (allow localhost by default)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(config.router)
app.include_router(tm5.router)
app.include_router(matches.router)
app.include_router(journal.router)
app.include_router(plan.router)
app.include_router(state.router)
app.include_router(superpath.router)

@app.get("/api/health")
def health():
    return {"ok": True}
