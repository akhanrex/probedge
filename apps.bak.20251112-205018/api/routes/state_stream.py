from fastapi import APIRouter
from fastapi.responses import StreamingResponse, Response
import os, json, time
from pathlib import Path

try:
    # Prefer the unified settings loader if present
    from probedge.infra.settings import SETTINGS
except Exception:
    SETTINGS = None

router = APIRouter()

def _load_json_safe(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def _json_dumps(obj) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return json.dumps({"error": "serialization"})

def _resolve_state_path() -> str:
    if SETTINGS and getattr(SETTINGS, "paths", None) and getattr(SETTINGS.paths, "state", None):
        return SETTINGS.paths.state
    return "live_state.json"

@router.get("/api/state/stream")
def stream_state():
    """SSE stream of the live_state.json."""
    state_path = _resolve_state_path()
    path = Path(state_path)

    def event_gen():
        last_mtime = -1.0
        last_payload = None
        first = True
        while True:
            try:
                if path.exists():
                    mtime = path.stat().st_mtime
                    if first or mtime != last_mtime:
                        data = _load_json_safe(str(path))
                        payload = _json_dumps(data)
                        if first or payload != last_payload:
                            yield f"data: {payload}\n\n"
                            last_payload = payload
                            last_mtime = mtime
                            first = False
                else:
                    if first:
                        yield "data: {}\n\n"
                        first = False
                time.sleep(1.0)
            except GeneratorExit:
                break
            except Exception:
                time.sleep(1.0)
                continue

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    return StreamingResponse(event_gen(), media_type="text/event-stream", headers=headers)

@router.get("/api/ping-sse")
def ping_sse():
    return Response("ok", media_type="text/plain")
