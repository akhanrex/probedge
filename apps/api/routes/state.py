from __future__ import annotations
from fastapi import APIRouter, HTTPException
from apps.storage.tm5 import read_state_json

router = APIRouter(prefix="/api", tags=["state"])

@router.get("/state")
def get_state():
    try:
        s = read_state_json()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return s
