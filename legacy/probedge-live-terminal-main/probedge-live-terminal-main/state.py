from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Literal

Tag = Literal["BULL", "BEAR", "NEUTRAL", "NONE"]

@dataclass
class TagsState:
    pdc: Optional[Tag] = None
    ol: Optional[Tag] = None
    ot: Optional[Tag] = None
    first_candle_type: Optional[str] = None
    range_status: Optional[str] = None
    locked_pdc: bool = False
    locked_ol: bool = False
    locked_ot: bool = False

@dataclass
class PlanState:
    mode: str = "5TH_BAR"
    direction: Optional[Tag] = None
    confidence: Optional[int] = None
    level: Optional[str] = None
    entry_ref: Optional[float] = None
    trigger: Optional[float] = None
    stop: Optional[float] = None
    t1: Optional[float] = None
    t2: Optional[float] = None
    qty: Optional[int] = None
    status: str = "IDLE"  # IDLE/ARMED/ORDER_SENT/LIVE/FLAT/MISSED/ABSTAINED

@dataclass
class SymbolState:
    symbol: str = ""
    ltp: float = 0.0
    tags: TagsState = field(default_factory=TagsState)
    plan: PlanState = field(default_factory=PlanState)
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    has_position: bool = False

@dataclass
class AppState:
    risk_rs: int = 1000
    entry_mode: str = "5TH_BAR"
    mode: str = "paper"
    symbols: Dict[str, SymbolState] = field(default_factory=dict)
    killswitch: bool = False
