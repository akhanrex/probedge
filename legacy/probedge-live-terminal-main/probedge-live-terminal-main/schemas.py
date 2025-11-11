from __future__ import annotations
from typing import TypedDict, Dict, Any

class WSState(TypedDict, total=False):
    symbol: str
    ltp: float
    tags: Dict[str, Any]
    plan: Dict[str, Any]
    pnl: Dict[str, float]
