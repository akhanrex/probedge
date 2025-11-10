import os

def ensure_dir(d: str):
    os.makedirs(d, exist_ok=True)
