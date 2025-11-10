from fastapi import APIRouter
from probedge.infra.settings import SETTINGS
router = APIRouter(prefix="/api", tags=["config"])
@router.get("/config")
def get_config():
    return {"mode": SETTINGS.mode,"bar_seconds": SETTINGS.bar_seconds,"symbols": SETTINGS.symbols,"paths": SETTINGS.paths.model_dump()}
