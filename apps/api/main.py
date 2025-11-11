from fastapi import FastAPI
from apps.api.routes import tm5 as tm5_route
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from .routes import config, journal, kill, matches, state, tm5
from .routes import state_file
from .ws import live as wslive
app = FastAPI(title="Probedge API")
app.include_router(config.router)
app.include_router(tm5.router)
app.include_router(matches.router)
app.include_router(state_file.router)
# # app.include_router(state.router)  # disabled  # disabled in favor of file-backed state
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


from probedge.decision.timeline import run_timeline
@app.on_event("startup")
async def _agg_start():
    if SETTINGS.mode == "live":
        app.state.agg_task = asyncio.create_task(run_agg(SETTINGS.symbols))
        app.state.timeline_task = asyncio.create_task(run_timeline())

@app.on_event("shutdown")
async def _agg_stop():
    t = getattr(app.state, "agg_task", None)
    if t:
        t.cancel()

app.mount("/ui", StaticFiles(directory="apps/api/static", html=True), name="ui")

app.include_router(tm5_route.router)
