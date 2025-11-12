
from fastapi import APIRouter, HTTPException
from probedge.infra.settings import SETTINGS
import json

router = APIRouter()

@router.get("/api/state")
def get_state():
    path = SETTINGS.paths.state
    try:
        with open(path) as f:
            data = json.load(f)
        return {"source": path, "data": data}
    except FileNotFoundError:
        # Return empty structure if file doesn't exist yet
        return {"source": path, "data": {}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"state error: {e}")
