# apps/api/routes/health.py

from fastapi import APIRouter
from probedge.infra.settings import SETTINGS
from probedge.infra.health import assess_health

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health_check():
    """
    Health endpoint used by UI and automation.
    Combines:
      - application mode
      - configured symbols
      - health assessment from heartbeats
    """
    health = assess_health()

    return {
        "mode": SETTINGS.mode,
        "symbols": SETTINGS.symbols,
        "system_status": health.system_status,  # "OK", "WARN", "DOWN"
        "reason": health.reason,
        "last_agg5_ts": health.last_agg5_ts,
        "last_batch_ts": health.last_batch_ts,
    }
