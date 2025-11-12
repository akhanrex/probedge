from __future__ import annotations
from fastapi import APIRouter
from apps.infra.settings import SETTINGS

router = APIRouter(prefix="/api", tags=["config"])

@router.get("/config")
def get_config():
    return {
        "mode": SETTINGS.mode,
        "bar_seconds": SETTINGS.bar_seconds,
        "symbols": SETTINGS.symbols,
        "paths": {
            "intraday_patterns": SETTINGS.paths.intraday_patterns,
            "master_patterns": SETTINGS.paths.master_patterns,
            "journal_csv": SETTINGS.paths.journal_csv,
            "state_json": SETTINGS.paths.state_json,
        },
        "data_dir": SETTINGS.data_dir,
    }
