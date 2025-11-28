# probedge/http/api_state.py

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException

from probedge.infra.settings import SETTINGS

router = APIRouter()

def _live_state_path() -> Path:
    # SETTINGS.paths.state is already "data/state/live_state.json"
    p = Path(SETTINGS.paths.state)
    return p

@router.get("/api/state")
async def get_state():
    """
    Return the latest live_state.json (fake-live or real-live).
    """
    path = _live_state_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="live_state.json not found")

    try:
        with path.open("r", encoding="utf-8") as f:
            state = json.load(f)
    except json.JSONDecodeError:
        # In case we read while the writer is mid-write
        raise HTTPException(status_code=503, detail="live_state.json not ready")

    return state
