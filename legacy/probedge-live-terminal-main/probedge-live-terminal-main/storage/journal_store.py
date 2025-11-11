# storage/journal_store.py
import os, csv, time
from typing import Dict
import pandas as pd  # <-- needed for load_journal()

DATA_DIR = os.getenv("DATA_DIR", "./data")
JOURNAL_PATH = os.path.join(DATA_DIR, "journal.csv")

HEADERS = [
    "ts","date","symbol","pick","entry","exit","qty","result","sl","t1","t2","pnl","notes"
]

os.makedirs(DATA_DIR, exist_ok=True)

def _ensure_header():
    if not os.path.exists(JOURNAL_PATH):
        with open(JOURNAL_PATH, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(HEADERS)

def load_journal() -> pd.DataFrame:
    _ensure_header()
    try:
        return pd.read_csv(JOURNAL_PATH)
    except Exception:
        return pd.DataFrame(columns=HEADERS)

def append_trade(row: Dict):
    """
    row keys (flexible): symbol, result, entry, exit, qty, pick, sl, t1, t2, pnl, notes, ts
    """
    _ensure_header()
    ts = row.get("ts") or time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    out = {
        "ts": ts,
        "date": date,
        "symbol": row.get("symbol"),
        "pick": row.get("pick"),
        "entry": row.get("entry"),
        "exit": row.get("exit"),
        "qty": row.get("qty"),
        "result": row.get("result"),
        "sl": row.get("sl"),
        "t1": row.get("t1"),
        "t2": row.get("t2"),
        "pnl": row.get("pnl"),
        "notes": row.get("notes",""),
    }
    with open(JOURNAL_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writerow(out)
