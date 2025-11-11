
from fastapi import APIRouter
from pathlib import Path
import json, datetime as dt

router = APIRouter()
STATE_MAIN = Path("data/state/live_state.json")   # runtime
STATE_TAGS = Path("data/state/tags_state.json")   # refresher

def _read_json(p: Path):
    try:
        if p.exists():
            return json.loads(p.read_text())
    except Exception:
        pass
    return {}

def _today():
    return dt.datetime.now().astimezone().strftime("%Y-%m-%d")

@router.get("/api/state")
async def api_state():
    runtime = _read_json(STATE_MAIN)
    tagsdoc = _read_json(STATE_TAGS)

    runtime.setdefault("date", _today())
    runtime.setdefault("symbols", [])
    runtime.setdefault("status", "boot")
    runtime.setdefault("steps", [])
    runtime.setdefault("tags", {})

    if tagsdoc.get("tags"):
        runtime["tags"] = tagsdoc["tags"]
    if tagsdoc.get("symbols"):
        runtime["symbols"] = tagsdoc["symbols"]
    if (not runtime.get("date")) and tagsdoc.get("date"):
        runtime["date"] = tagsdoc["date"]

    return runtime
