from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import config, tm5, matches, state, journal, kill
from .ws import live as wslive
app = FastAPI(title="Probedge API")
app.include_router(config.router)
app.include_router(tm5.router)
app.include_router(matches.router)
app.include_router(state.router)
app.include_router(journal.router)
app.include_router(kill.router)
app.include_router(wslive.router)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])
@app.get("/api/health")
def health(): return {"ok": True}


# --- Live 5-min aggregator autostart ---
import asyncio
from probedge.infra.settings import SETTINGS
from probedge.realtime.agg5 import run_agg

@app.on_event("startup")
async def _agg_start():
    if SETTINGS.mode == "live":
        app.state.agg_task = asyncio.create_task(run_agg(SETTINGS.symbols))

@app.on_event("shutdown")
async def _agg_stop():
    t = getattr(app.state, "agg_task", None)
    if t:
        t.cancel()
