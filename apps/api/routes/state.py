from fastapi import APIRouter
from probedge.infra.settings import SETTINGS
router = APIRouter(prefix="/api", tags=["state"])
@router.get("/state")
def get_state():
    return {"symbols": SETTINGS.symbols, "status": "idle", "risk_rs": SETTINGS.risk_rs, "mode": SETTINGS.mode}
