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
