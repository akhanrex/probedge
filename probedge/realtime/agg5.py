# probedge/realtime/agg5.py
#
# Live 5-minute bar aggregator (synchronous).
#
# - Consumes ticks from probedge.realtime.kite_live.tick_stream
# - Maintains per-symbol rolling OHLC over IST 5-minute windows
# - Appends bars to data/intraday/{sym}_5minute.csv
# - Updates data/state/live_state.json with latest quotes
# - Records agg5 heartbeat via probedge.infra.health.record_agg5_heartbeat

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

from probedge.infra.settings import SETTINGS
from probedge.infra.health import record_agg5_heartbeat
from probedge.realtime.kite_live import tick_stream
from probedge.storage.atomic_json import AtomicJSON

IST = timezone(timedelta(hours=5, minutes=30))

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
        with self.path.open("a", newline="") as f:
            csv.writer(f).writerow(
                [ts.astimezone(IST).isoformat(timespec="seconds"), o, h, l, c, n]
            )


def _update_live_state(sym: str, dt: datetime, o: float, h: float, l: float, c: float, n: int) -> None:
    """Merge latest quote for a symbol into live_state.json."""
    state = aj.read(default={}) or {}

    state["mode"] = SETTINGS.mode
    state["sim"] = False
    state["sim_day"] = dt.date().isoformat()
    state["sim_clock"] = dt.isoformat()

    symbols = state.get("symbols") or {}
    symbols[sym] = {
        "ltp": float(c),
        "ohlc": {"o": float(o), "h": float(h), "l": float(l), "c": float(c)},
        "volume": int(n),  # tick count as proxy
    }
    state["symbols"] = symbols

    aj.write(state)


def run_agg(symbols: Iterable[str]) -> None:
    # per-symbol rolling state: (window_start, o, h, l, c, tick_count)
    state: Dict[str, Tuple[datetime, float, float, float, float, int]] = {}
    writers: Dict[str, BarWriter] = {s: BarWriter(s) for s in symbols}

    print("[agg5] starting for:", list(symbols))

    for batch in tick_stream(symbols):
        # batch: list[(sym, ts_epoch, price)]
        for sym, ts_epoch, price in batch:
            if sym not in writers:
                continue

            dt = datetime.fromtimestamp(float(ts_epoch), tz=IST)
            win = floor_5min(dt)

            if sym not in state:
                state[sym] = (win, price, price, price, price, 1)
            else:
                cur_win, o, h, l, c, n = state[sym]
                if win == cur_win:
                    h = max(h, price)
                    l = min(l, price)
                    c = price
                    n += 1
                    state[sym] = (cur_win, o, h, l, c, n)
                else:
                    # flush completed bar
                    writers[sym].write_bar(cur_win, o, h, l, c, n)
                    print(
                        f"[agg5] {sym} {cur_win.astimezone(IST).isoformat(timespec='minutes')} "
                        f"O={o} H={h} L={l} C={c} N={n}"
                    )
                    record_agg5_heartbeat()
                    # new window
                    state[sym] = (win, price, price, price, price, 1)

            cur_win, o, h, l, c, n = state[sym]
            try:
                _update_live_state(sym, dt, o, h, l, c, n)
            except Exception:
                # we don't want to stop agg on JSON write error
                pass


def main() -> None:
    syms = SETTINGS.symbols
    run_agg(syms)


if __name__ == "__main__":
    main()
