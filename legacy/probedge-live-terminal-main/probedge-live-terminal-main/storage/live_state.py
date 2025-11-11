# storage/live_state.py
import os, json, tempfile, time

DATA_DIR = os.getenv("DATA_DIR", "./data")
STATE_PATH = os.path.join(DATA_DIR, "live_state.json")

os.makedirs(DATA_DIR, exist_ok=True)

def save_state(state: dict):
    # add a heartbeat
    state = dict(state)
    state["_saved_at"] = time.time()
    d = os.path.dirname(STATE_PATH) or "."
    os.makedirs(d, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, suffix=".tmp") as tmp:
        json.dump(state, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, STATE_PATH)

# add at bottom (keep existing save_state as-is)
import json, os

PATH = os.getenv("LIVE_STATE_PATH", os.path.join(os.getenv("DATA_DIR","./data"), "live_state.json"))

def load_state():
    try:
        if os.path.exists(PATH):
            with open(PATH, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}
