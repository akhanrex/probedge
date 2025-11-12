from fastapi import APIRouter
from probedge.infra.settings import SETTINGS

router = APIRouter()

@router.get("/api/config")
def get_config():
    return {
        "mode": SETTINGS.mode,
        "symbols": SETTINGS.symbols,
        "paths": {
            "intraday": SETTINGS.paths.intraday,
            "masters": SETTINGS.paths.masters,
            "journal": SETTINGS.paths.journal,
            "state": SETTINGS.paths.state,
        },
        "risk_budget_rs": SETTINGS.risk_budget_rs,  # test=1k, else default=10k
        "allowed_origins": SETTINGS.allowed_origins,
    }
