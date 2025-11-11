from pathlib import Path
import os

DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data")).resolve()
MASTERS_DIR = DATA_ROOT / "masters"
LATEST_DIR = DATA_ROOT / "latest"
JOURNAL_DIR = DATA_ROOT / "journal"
CONFIG_DIR = Path("./config").resolve()
