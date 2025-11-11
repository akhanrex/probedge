from __future__ import annotations
from typing import Dict, Any, Optional
from dataclasses import dataclass
from probedge.core import classifiers  # Use your existing, unchanged classifiers

@dataclass
class TagsOut:
    PDC: Optional[str] = None
    OL: Optional[str] = None
    OT: Optional[str] = None
    FirstCandleType: Optional[str] = None
    RANGE_STATUS: Optional[str] = None

def compute_tags_5(df_like) -> TagsOut:
    prev = classifiers.prev_trading_day_ohlc(df_like)
    pdc = classifiers.compute_prevdaycontext_robust(df_like, prev)
    ol = classifiers.compute_openlocation(df_like) if hasattr(classifiers, "compute_openlocation") else classifiers.compute_openlocation_from_df(df_like)
    ot = classifiers.compute_openingtrend_robust(df_like)
    fct = classifiers.compute_first_candletype(df_like)
    rng = classifiers.compute_rangestatus(df_like)
    return TagsOut(PDC=pdc, OL=ol, OT=ot, FirstCandleType=fct, RANGE_STATUS=rng)
