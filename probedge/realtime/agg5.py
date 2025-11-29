# probedge/realtime/agg5.py
# Live 5-minute bar aggregator:
# - Consumes ticks from probedge.realtime.kite_live.live_tick_stream
# - Maintains per-symbol rolling OHLC over IST 5-minute windows
# - Appends bars to data/intraday/{sym}_5minute.csv
# - Updates data/state/live_state.json with latest quotes (ltp + ohlc + "volume"=tick_count)
# - Records agg5 heartbeat via probedge.infra.health.record_agg5_heartbeat

from __future__ import annotations

import asyncio
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Tuple, List

from probedge.infra.settings import SETTINGS
from probedge.infra.health import record_agg5_heartbeat
from probedge.realtime.kite_live import live_tick_stream
from probedge.storage.atomic_json import AtomicJSON

IST = timezone(timedelta(hours=5, minutes=30))

# live_state.json helper
STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)


def floor_5min(dt: datetime) -> datetime:
    """Floor a timezone-aware datetime to the start of its 5-minute window."""
    return dt.replace(second=0, microsecond=0, minute=(dt.minute // 5) * 5)


class BarWriter:
    """Append-only writer for 5-minute OHLC bars in the canonical intraday CSV."""

    def __init__(self, sym: str):
        p = SETTINGS.paths.intraday.format(sym=sym)
        self.path = Path(p)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.exists():
            with self.path.open("w", newline="") as f:
                csv.writer(f).writerow(
                    ["DateTime", "Open", "High", "Low", "Close", "Ticks"]
                )

    def write_bar(self, ts: datetime, o: float, h: float, l: float, c: float, n: int) -> None:
        """Append a completed 5-min bar."""
        with self.path.open("a", newline="") as f:
            csv.writer(f).writerow(
                [ts.astimezone(IST).isoformat(timespec="seconds"), o, h, l, c, n]
            )


def _update_live_state(sym: str, dt: datetime, o: float, h: float, l: float, c: float, n: int) -> None:
    """Merge latest quote for a symbol into live_state.json.

    We keep existing keys (portfolio_plan, health, etc.) and only touch:
      - mode
      - sim / sim_day / sim_clock
      - symbols[sym] = {ltp, ohlc, volume}
    """
    state = aj.read(default={}) or {}

    # Basic meta
    state["mode"] = SETTINGS.mode
    state["sim"] = False
    day_str = dt.date().isoformat()
    state["sim_day"] = day_str
    state["sim_clock"] = dt.isoformat()

    symbols = state.get("symbols") or {}
    symbols[sym] = {
        "ltp": float(c),
        "ohlc": {"o": float(o), "h": float(h), "l": float(l), "c": float(c)},
        # we don't have proper trade volume; use tick-count as a proxy
        "volume": int(n),
    }
    state["symbols"] = symbols

    aj.write(state)


async def run_agg(symbols: List[str]) -> None:
    """Main aggregation loop.

    Consumes batches of (symbol, ts_epoch, price) tuples from live_tick_stream,
    maintains rolling 5-min bars, writes completed bars to CSV,
    updates live_state.json quotes, and bumps the agg5 heartbeat.
    """
    # per-symbol rolling state: (window_start, o, h, l, c, tick_count)
    state: Dict[str, Tuple[datetime, float, float, float, float, int]] = {}
    writers: Dict[str, BarWriter] = {s: BarWriter(s) for s in symbols}

    async for batch in live_tick_stream(symbols):
        # batch is a list[tuple[sym, ts_epoch, price]]
        for sym, ts_epoch, price in batch:
            # Make sure symbol is in our configured universe
            if sym not in writers:
                continue

            # Map timestamp to IST 5-min window
            dt = datetime.fromtimestamp(float(ts_epoch), tz=IST)
            win = floor_5min(dt)

            if sym not in state:
                # first tick for this symbol
                state[sym] = (win, price, price, price, price, 1)
            else:
                cur_win, o, h, l, c, n = state[sym]

                if win == cur_win:
                    # same 5-min window: update running OHLC
                    h = max(h, price)
                    l = min(l, price)
                    c = price
                    n += 1
                    state[sym] = (cur_win, o, h, l, c, n)
                else:
                    # window changed -> flush completed bar for previous window
                    writers[sym].write_bar(cur_win, o, h, l, c, n)
                    print(
                        f"[agg5] {sym} {cur_win.astimezone(IST).isoformat(timespec='minutes')} "
                        f"O={o} H={h} L={l} C={c} N={n}"
                    )
                    # heartbeat for health endpoint
                    record_agg5_heartbeat()
                    # start new window with this tick
                    state[sym] = (win, price, price, price, price, 1)

            # After updating state for this tick, refresh live_state.json for this symbol
            cur_win, o, h, l, c, n = state[sym]
            try:
                _update_live_state(sym, dt, o, h, l, c, n)
            except Exception:
                # Do not kill the agg loop if JSON write fails; just log later via /api/health
                pass


async def main() -> None:
    syms = SETTINGS.symbols
    print("[agg5] starting for:", syms)
    await run_agg(list(syms))


if __name__ == "__main__":
    asyncio.run(main())
