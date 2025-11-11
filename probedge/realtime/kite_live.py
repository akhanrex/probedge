import asyncio
import logging
from typing import AsyncIterator, List, Dict, Any
from kiteconnect import KiteConnect, KiteTicker
from probedge.infra.settings import SETTINGS

log = logging.getLogger(__name__)

def _instruments_map(kc: KiteConnect) -> Dict[str, int]:
    """
    Build NSE symbol -> instrument_token map once.
    """
    table = kc.instruments("NSE")
    mp = {}
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

async def live_tick_stream(symbols: List[str] = None) -> AsyncIterator[Dict[str, Any]]:
    """Async generator yielding dict batches: {symbol, ts, ltp}"""
    if symbols is None:
        symbols = SETTINGS.symbols

    api_key = SETTINGS.kite_api_key
    access_token = SETTINGS.kite_access_token
    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)

    sym2tok = _instruments_map(kc)
    tokens = [sym2tok[s.upper()] for s in symbols if s.upper() in sym2tok]
    if not tokens:
        raise RuntimeError("No instrument tokens resolved. Check symbols & access token.")

    kt = KiteTicker(api_key, access_token)
    queue: asyncio.Queue = asyncio.Queue(maxsize=5000)

    def on_ticks(ws, ticks):
        for t in ticks or []:
            token = t.get("instrument_token")
            ltp = t.get("last_price") or t.get("last_traded_price")
            ts = t.get("timestamp") or t.get("exchange_timestamp")
            sym = next((s for s, tok in sym2tok.items() if tok == token), None)
            if sym and ltp is not None:
                item = {"symbol": sym, "ts": str(ts), "ltp": float(ltp)}
                try:
                    queue.put_nowait(item)
                except asyncio.QueueFull:
                    pass

    def on_connect(ws, resp):
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

    runner = loop.run_in_executor(None, _run)

    try:
        while True:
            item = await queue.get()
            yield item
    finally:
        try:
            kt.close()
        except Exception:
            pass
