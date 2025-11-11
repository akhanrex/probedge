from __future__ import annotations
import csv, os, datetime as dt
from typing import Dict, Any

def _journal_path(base_dir: str, date: dt.date) -> str:
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, f"journal_{date.strftime('%Y%m%d')}.csv")

def append_trade(base_dir: str, row: Dict[str, Any]) -> None:
    path = _journal_path(base_dir, dt.date.today())
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if not exists:
            w.writeheader()
        w.writerow(row)

def load_journal(base_dir: str):
    path = _journal_path(base_dir, dt.date.today())
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))
