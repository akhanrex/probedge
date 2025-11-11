# tools/sync_any_master.py
from pathlib import Path
import sys

repo = Path(__file__).resolve().parents[1] if (Path(__file__).parent.name == "tools") else Path(__file__).resolve().parent
sys.path.insert(0, str(repo))

from app.intraday_utils import sync_master_full_from_5m

TARGETS = [
    ("lt",   "data/masters/LT_Master.csv"),
    ("sbin", "data/masters/SBIN_Master.csv"),
]

for key, mpath in TARGETS:
    stats = sync_master_full_from_5m(key, mpath)
    print(f"{key.upper():<5} → added={stats['rows_added']}, updated={stats['rows_updated']}, wrote: {stats['path']}")
