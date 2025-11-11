import asyncio, os, time, logging, pathlib, yaml
from typing import List, Dict, Tuple
from kiteconnect import KiteTicker, KiteConnect

log = logging.getLogger(__name__)

def _apply_aliases(symbols: List[str]) -> List[str]:
    mfile = pathlib.Path("config/symbol_map.yaml")
    if not mfile.exists():
        return symbols
    try:
        y = yaml.safe_load(mfile.read_text()) or {}
        aliases = (y.get("aliases") or {})
        return [aliases.get(sym, sym) for sym in symbols]
    except Exception:
        return symbols

def _resolve_tokens(symbols: List[str], api_key: str, access_token: str) -> Tuple[list[int], dict[int,str]]:
    """Resolve instrument tokens using quote() so subscription matches exactly."""
    kc = KiteConnect(api_key=api_key)
    kc.set_access_token(access_token)
    tokens, tok2sym = [], {}
    for sym in symbols:
        key = sym.upper().strip()
        tok = None
        for ex in ("NSE","BSE"):
            try:
                q = kc.quote([f"{ex}:{key}"]).get(f"{ex}:{key}")
                if q and "instrument_token" in q:
                    tok = int(q["instrument_token"])
                    break
            except Exception:
                pass
        if tok:
            tokens.append(tok)
            tok2sym[tok] = key
        else:
            log.warning("[Kite] No instrument token for: %s", key)
    if not tokens:
        raise RuntimeError(f"No instrument tokens resolved for: {symbols}")
    return tokens, tok2sym

async def live_tick_stream(symbols: List[str]):
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        raise RuntimeError("KITE_API_KEY / KITE_ACCESS_TOKEN missing in environment")

    # apply aliases like TATAMOTORS->TMPV, SWIGGY->ZOMATO, etc
    symbols = _apply_aliases(symbols)

    tokens, tok2sym = _resolve_tokens(symbols, api_key, access_token)
    q: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()  # capture main loop

    def on_ticks(ws, ticks):
        ts = time.time()
        batch = []
        for t in ticks:
            tok = t.get("instrument_token"); ltp = t.get("last_price")
            sym = tok2sym.get(tok)
            if sym is not None and ltp is not None:
                batch.append((sym, ts, float(ltp)))
        if batch:
            loop.call_soon_threadsafe(q.put_nowait, batch)

    def on_connect(ws, resp):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)
        log.info("[Kite] Connected. Subscribed %d tokens", len(tokens))

    ktt = KiteTicker(api_key, access_token)
    ktt.on_connect = on_connect
    ktt.on_ticks   = on_ticks
    ktt.connect(threaded=True)  # background thread

    # Fail fast if no tick arrives (helps catch token/market issues)
    try:
        first = await asyncio.wait_for(q.get(), timeout=10.0)
        yield first
    except asyncio.TimeoutError:
        raise RuntimeError("No ticks received within 10s. Check market hours, token, or internet.")

    while True:
        yield await q.get()
