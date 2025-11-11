import math, time
from dataclasses import dataclass, field
from typing import Optional, Dict

@dataclass
class Bar:
    start_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: int = 0

class BarAggregator:
    """
    Paper mode: '5-min' bars close every BAR_SECONDS (e.g., 10s).
    Live mode: set BAR_SECONDS=300.
    """
    def __init__(self, symbol: str, bar_seconds: int = 300):
        self.symbol = symbol
        self.bar_seconds = max(1, int(bar_seconds))
        self.cur: Optional[Bar] = None

    def _bucket_start(self, ts: float) -> float:
        return math.floor(ts / self.bar_seconds) * self.bar_seconds

    def on_tick(self, ts: float, price: float) -> Optional[Dict]:
        """
        Feed every tick. Returns a CLOSED bar dict when a bar rolls over, else None.
        """
        if not (isinstance(ts, (int, float)) and isinstance(price, (int, float))):
            return None
        bstart = self._bucket_start(ts)

        # First tick ever
        if self.cur is None:
            self.cur = Bar(start_ts=bstart, open=price, high=price, low=price, close=price, volume=1)
            return None

        # Same bucket -> update
        if bstart == self.cur.start_ts:
            self.cur.high = max(self.cur.high, price)
            self.cur.low  = min(self.cur.low,  price)
            self.cur.close = price
            self.cur.volume += 1
            return None

        # New bucket -> close previous, start new
        closed = self._emit_closed()
        self.cur = Bar(start_ts=bstart, open=price, high=price, low=price, close=price, volume=1)
        return closed

    def _emit_closed(self) -> Dict:
        b = self.cur
        # Minimal schema for now; UI doesn’t need this yet, but storage does.
        return {
            "symbol": self.symbol,
            "start_ts": b.start_ts,
            "end_ts": b.start_ts + self.bar_seconds,
            "Open": float(b.open),
            "High": float(b.high),
            "Low": float(b.low),
            "Close": float(b.close),
            "Volume": int(b.volume),
        }
