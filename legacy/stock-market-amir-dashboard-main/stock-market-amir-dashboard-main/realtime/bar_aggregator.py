# realtime/bar_aggregator.py
from __future__ import annotations
import math, time, datetime as dt
from typing import Dict, Optional

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

def _floor_5m(ts_ms: int) -> dt.datetime:
    t = dt.datetime.fromtimestamp(ts_ms/1000.0, IST)
    minute = (t.minute // 5) * 5
    return t.replace(minute=minute, second=0, microsecond=0)

def _next_5m_floor(t: dt.datetime) -> dt.datetime:
    add = 5 - (t.minute % 5)
    nt = t + dt.timedelta(minutes=add)
    return nt.replace(second=0, microsecond=0)

class Candle5m:
    __slots__ = ("start","end","o","h","l","c","v")
    def __init__(self, start: dt.datetime, end: dt.datetime, px: float, vol: float=0.0):
        self.start = start; self.end = end
        self.o = px; self.h = px; self.l = px; self.c = px; self.v = float(vol)
    def add(self, px: float, vol: float=0.0):
        if px is None: return
        if px > self.h: self.h = px
        if px < self.l: self.l = px
        self.c = px
        self.v += float(vol)

class BarAggregator5m:
    """
    Build rolling 5m candles from ticks/deltas. Call .push(last, ts_ms, vol_delta)
    Emits complete candles via the on_close callback.
    """
    def __init__(self, on_close):
        self.on_close = on_close
        self.cur: Optional[Candle5m] = None

    def push(self, last: float, ts_ms: int, vol_delta: float=0.0):
        if last is None: return
        start = _floor_5m(ts_ms)
        if (self.cur is None) or (start != self.cur.start):
            # close prev
            if self.cur is not None:
                try: self.on_close(self.cur)
                except Exception: pass
            # open new
            self.cur = Candle5m(start=start, end=_next_5m_floor(start), px=float(last), vol=float(vol_delta))
        else:
            self.cur.add(float(last), float(vol_delta))
