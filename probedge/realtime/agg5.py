import asyncio, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple
from probedge.infra.settings import SETTINGS
from probedge.realtime.kite_live import live_tick_stream
from probedge.infra.health import record_agg5_heartbeat

IST = timezone(timedelta(hours=5, minutes=30))

def floor_5min(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0, minute=(dt.minute//5)*5)

class BarWriter:
    def __init__(self, sym: str):
        # paths are attribute-based, not dict-like
        p = SETTINGS.paths.intraday.format(sym=sym)
        self.path = Path(p)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self):
        if not self.path.exists():
            with self.path.open("w", newline="") as f:
                csv.writer(f).writerow(["DateTime","Open","High","Low","Close","Ticks"])

    def write_bar(self, ts: datetime, o: float, h: float, l: float, c: float, n: int):
        with self.path.open("a", newline="") as f:
            csv.writer(f).writerow([ts.astimezone(IST).isoformat(timespec="seconds"), o, h, l, c, n])

async def run_agg(symbols):
    # per-symbol rolling state: (window_start, o,h,l,c, tick_count)
    state: Dict[str, Tuple[datetime, float, float, float, float, int]] = {}
    writers: Dict[str, BarWriter] = {s: BarWriter(s) for s in symbols}

    async for batch in live_tick_stream(symbols):
        for sym, ts, price in batch:
            dt = datetime.fromtimestamp(ts, tz=IST)
            win = floor_5min(dt)
            if sym not in state:
                state[sym] = (win, price, price, price, price, 1)
                continue
            cur_win, o,h,l,c, n = state[sym]
            if win == cur_win:
                h = max(h, price); l = min(l, price); c = price; n += 1
                state[sym] = (cur_win, o,h,l,c, n)
            else:
                # flush completed bar
                writers[sym].write_bar(cur_win, o,h,l,c, n)
                print(f"[agg5] {sym} {cur_win.astimezone(IST).isoformat(timespec='minutes')} O={o} H={h} L={l} C={c} N={n}")
                # start new bar
                state[sym] = (win, price, price, price, price, 1)

async def main():
    syms = SETTINGS.symbols
    print("[agg5] starting for:", syms)
    await run_agg(syms)
    record_agg5_heartbeat()

if __name__ == "__main__":
    asyncio.run(main())
