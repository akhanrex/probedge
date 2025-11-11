from __future__ import annotations
from typing import Optional

class OMS:
    def __init__(self):
        self._live = {}

    def place_entry(self, sym: str, direction: str, price: float, qty: int) -> None:
        self._live[sym] = {
            "side": "BUY" if direction == "BULL" else "SELL",
            "entry_px": price,
            "qty": qty,
            "filled": False,
            "t1_hit": False,
            "t2_hit": False,
            "stop_hit": False,
        }

    def sync(self, sym: str, ltp: float, plan) -> Optional[str]:
        o = self._live.get(sym)
        if not o:
            return None
        if not o["filled"]:
            crossed = (ltp >= plan.trigger) if plan.direction == "BULL" else (ltp <= plan.trigger)
            if crossed:
                o["filled"] = True
                return "LIVE"
            else:
                return "ORDER_SENT"
        else:
            if plan.direction == "BULL":
                if ltp <= plan.stop and not o["stop_hit"]:
                    o["stop_hit"] = True
                    return "FLAT"
                if ltp >= plan.t2 and not o["t2_hit"]:
                    o["t2_hit"] = True
                    return "FLAT"
                if ltp >= plan.t1 and not o["t1_hit"]:
                    o["t1_hit"] = True
                    return "LIVE"
            else:
                if ltp >= plan.stop and not o["stop_hit"]:
                    o["stop_hit"] = True
                    return "FLAT"
                if ltp <= plan.t2 and not o["t2_hit"]:
                    o["t2_hit"] = True
                    return "FLAT"
                if ltp <= plan.t1 and not o["t1_hit"]:
                    o["t1_hit"] = True
                    return "LIVE"
        return None

    def force_exit(self, sym: str, ltp: float, plan) -> None:
        if sym in self._live:
            self._live.pop(sym, None)
