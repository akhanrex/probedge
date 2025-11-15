# apps/api/routes/health.py

from fastapi import APIRouter
from probedge.infra.settings import SETTINGS

router = APIRouter()

@router.get("/api/health")
def health():
    """
    Simple healthcheck + minimal config snapshot.
    Use this to quickly see if the backend is alive and reading settings.
    """
    return {
        "status": "ok",
        "mode": SETTINGS.mode,
        "symbols": SETTINGS.symbols,
    }
