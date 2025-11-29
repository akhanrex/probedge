# probedge/realtime/kite_live.py
#
# Thin wrapper around KiteTicker that exposes an async tick stream:
#   async for batch in live_tick_stream(symbols):
#       for sym, ts_epoch, ltp in batch:
#           ...
#
# Each batch is a small list of (symbol, ts_epoch, last_price) tuples.

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import AsyncIterator, Dict, List, Tuple

from kiteconnect import KiteConnect, KiteTicker

from probedge.infra.settings import SETTINGS

log = logging.getLogger(__name__)


def _instruments_map(kc: KiteConnect) -> Dict[str, int]:
    """Build NSE symbol -> instrument_token map once for the configured universe."""
    table = kc.instruments("NSE")
    mp: Dict[str, int] = {}
    want = set(s.upper() for s in SETTINGS.symbols)
    for row in table:
        tradingsymbol = row.get("tradingsymbol")
        token = row.get("instrument_token")
        if tradingsymbol and token and tradingsymbol.upper() in want:
            mp[tradingsymbol.upper()] = int(token)
    missing = sorted(list(want - set(mp)))
    if missing:
        log.warning("Missing tokens for symbols: %s", ",".join(missing))
    return mp


async def live_tick_stream(
    symbols: List[str] | None = None,
) -> AsyncIterator[List[Tuple[str, float, float]]]:
    """Async generator yielding batches of market data ticks.

    Each yielded `batch` is:
        List[Tuple[symbol, ts_epoch, last_price]]

    - `symbol` is the uppercase tradingsymbol (e.g. "SBIN").
    - `ts_epoch` is a float UNIX timestamp (seconds since epoch).
    - `last_price` is the LTP as float.
    """
    if symbols is None:
        symbols = list(SETTINGS.symbols)

    api_key = os.getenv("KITE_API_KEY", "").strip()
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip()
    if not api_key or not access_token:
        raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN missing in environment (.env).")

    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)

    sym2tok = _instruments_map(kc)
    tokens = [sym2tok[s.upper()] for s in symbols if s.upper() in sym2tok]
    if not tokens:
        raise RuntimeError("No instrument tokens resolved. Check symbols & KITE_ACCESS_TOKEN.")

    # Reverse map: instrument_token -> symbol
    tok2sym: Dict[int, str] = {tok: sym for sym, tok in sym2tok.items()}

    kt = KiteTicker(api_key, access_token)
    queue: asyncio.Queue[Tuple[str, float, float]] = asyncio.Queue(maxsize=5000)

    def on_ticks(ws, ticks):
        now = time.time()
        for t in ticks or []:
            token = t.get("instrument_token")
            if token is None:
                continue
            sym = tok2sym.get(int(token))
            if not sym:
                continue

            ltp = t.get("last_price") or t.get("last_traded_price")
            if ltp is None:
                continue

            ts_val = t.get("timestamp") or t.get("exchange_timestamp") or now
            try:
                # Kite typically passes a datetime-like object
                if hasattr(ts_val, "timestamp"):
                    ts_epoch = float(ts_val.timestamp())
                else:
                    ts_epoch = float(ts_val)
            except Exception:
                ts_epoch = now

            triple = (sym, ts_epoch, float(ltp))
            try:
                queue.put_nowait(triple)
            except asyncio.QueueFull:
                # drop ticks if consumer is lagging; health endpoint will show it
                pass

    def on_connect(ws, resp):
        log.info("KiteTicker connected; subscribing to %d tokens", len(tokens))
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)

    def on_close(ws, code, reason):
        log.warning("KiteTicker closed: %s %s", code, reason)

    def on_error(ws, code, reason):
        log.error("KiteTicker error: %s %s", code, reason)

    kt.on_ticks = on_ticks
    kt.on_connect = on_connect
    kt.on_close = on_close
    kt.on_error = on_error

    loop = asyncio.get_event_loop()

    def _run():
        try:
            kt.connect(threaded=False, disable_ssl_verification=False)
        except Exception as e:
            log.exception("KiteTicker connect failed: %s", e)

    # run KiteTicker in a background thread/executor
    loop.run_in_executor(None, _run)

    try:
        while True:
            # block until at least one tick arrives
            first = await queue.get()
            batch: List[Tuple[str, float, float]] = [first]

            # drain any additional ticks that arrived in the meantime
            while True:
                try:
                    more = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    batch.append(more)

            yield batch
    finally:
        try:
            kt.close()
        except Exception:
            pass
