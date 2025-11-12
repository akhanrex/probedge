
from fastapi import APIRouter
from probedge.infra.settings import SETTINGS

router = APIRouter()

@router.get("/api/settings")
def settings_dump():
    return {
        "mode": SETTINGS.mode,
        "bar_seconds": SETTINGS.bar_seconds,
        "data_dir": SETTINGS.data_dir,
        "paths": {
            "intraday": SETTINGS.paths.intraday,
            "masters":  SETTINGS.paths.masters,
            "ticks":    SETTINGS.paths.ticks,
            "journal":  SETTINGS.paths.journal,
            "state":    SETTINGS.paths.state,
        },
        "symbols": SETTINGS.symbols,
    }
