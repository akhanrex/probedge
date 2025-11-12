from fastapi import APIRouter, HTTPException
from pathlib import Path
import json

router = APIRouter()

@router.get("/api/state")
def api_state():
    f = Path("data/state/live_state.json")
    if not f.exists():
        raise HTTPException(404, "state not found")
    return json.loads(f.read_text())
