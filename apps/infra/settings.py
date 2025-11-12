from __future__ import annotations
from pydantic import BaseModel
from typing import List, Optional
import os

DEFAULT_SYMBOLS = [
    "TATAMOTORS","ETERNAL","JIOFIN","HAL","LICI",
    "BAJFINANCE","JSWENERGY","RECLTD","BAJAJHFL","SWIGGY"
]

class Paths(BaseModel):
    # Flexible path templates. We'll try these in order.
    intraday_patterns: List[str] = [
        "{DATA_DIR}/intraday/{sym}_5minute.csv",
        "{DATA_DIR}/intraday/{sym}_5MINUTE.csv",
        "{DATA_DIR}/tm5/{sym}_5minute.csv",
        "{DATA_DIR}/{sym}/tm5min.csv",
    ]
    master_patterns: List[str] = [
        "{DATA_DIR}/masters/{sym}_5MINUTE_MASTER_INDICATORS.csv",
        "{DATA_DIR}/master/{sym}_Master.csv",
        "{DATA_DIR}/masters/{sym}_Master.csv",
    ]
    journal_csv: str = "{DATA_DIR}/journal.csv"
    state_json: str = "{DATA_DIR}/live_state.json"

class Settings(BaseModel):
    mode: str = os.getenv("MODE", "paper")
    bar_seconds: int = int(os.getenv("BAR_SECONDS", "300"))
    data_dir: str = os.getenv("DATA_DIR", "data")
    symbols: List[str] = (
        [s.strip().upper() for s in os.getenv("SYMBOLS","").split(",") if s.strip()]
        or DEFAULT_SYMBOLS
    )
    paths: Paths = Paths()

SETTINGS = Settings()
