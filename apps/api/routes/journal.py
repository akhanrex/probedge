from __future__ import annotations
from fastapi import APIRouter, HTTPException
from apps.storage.tm5 import read_journal

router = APIRouter(prefix="/api", tags=["journal"])

@router.get("/journal/daily")
def get_journal_daily():
    try:
        df = read_journal()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    rows = df.to_dict(orient="records")
    return {"count": len(rows), "rows": rows}
