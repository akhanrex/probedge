import os, json
from infra.config import DATA_DIR


def path(symbol): return os.path.join(DATA_DIR, f"{symbol}_live_state.json")


def read(symbol):
p = path(symbol)
return json.load(open(p)) if os.path.exists(p) else {}


def write(symbol, state:dict):
os.makedirs(DATA_DIR, exist_ok=True)
with open(path(symbol), "w") as f:
json.dump(state, f, indent=2, default=str)
