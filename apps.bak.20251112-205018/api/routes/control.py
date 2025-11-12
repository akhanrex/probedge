
from fastapi import APIRouter
import os, json
from probedge.infra.settings import SETTINGS

router = APIRouter()
STATE_PATH = SETTINGS.paths.state

def _read_state():
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_state(st):
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

@router.post("/api/arm")
def arm(payload: dict):
    st = _read_state()
    st.update({"armed": True})
    st.update(payload or {})
    _write_state(st)
    return {"ok": True, "state": st}

@router.post("/api/stop")
def stop():
    st = _read_state()
    st["armed"] = False
    _write_state(st)
    return {"ok": True, "state": st}
