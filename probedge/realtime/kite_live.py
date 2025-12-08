# probedge/realtime/kite_live.py
#
# Synchronous tick stream using KiteTicker.
#
# Usage:
#   from probedge.realtime.kite_live import tick_stream
#   for batch in tick_stream(symbols):
#       for sym, ts_epoch, ltp in batch:
#           ...
#
# Each batch is a small list of (symbol, ts_epoch, last_price) tuples.

from __future__ import annotations

import logging
import os
import queue
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker

from probedge.infra.settings import SETTINGS

log = logging.getLogger(__name__)

import json

# Same session file logic as apps/api/routes/auth.py
SESSION_FILE: Path = (
    SETTINGS.kite_session_file
    if getattr(SETTINGS, "kite_session_file", None)
    else (SETTINGS.data_dir / "data/state/kite_session.json")
)

def _load_session() -> dict | None:
    """Load stored Kite session from disk, or None."""
    if not SESSION_FILE.exists():
        return None
    try:
        with SESSION_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

# Load .env explicitly from repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
DOTENV_PATH = REPO_ROOT / ".env"
load_dotenv(dotenv_path=DOTENV_PATH, override=False)

def _instruments_map(kc: KiteConnect) -> Dict[str, int]:
    """Build logical symbol -> instrument_token map for our universe.

    Uses SETTINGS.symbols as the logical universe and, if available,
    SETTINGS.symbol_map (or a similar mapping) to map logical symbols
    to real Kite tradingsymbols.

    Example:
        logical "TATAMOTORS" -> real "TMPV" (new Kite symbol)
    """
    # Logical universe
    logical_syms = [s.upper() for s in SETTINGS.symbols]

    # Try to get symbol_map from settings (if defined)
    # Expected shape: {"TATAMOTORS": "TMPV", "SBIN": "SBIN", ...}
    try:
        symbol_map = getattr(SETTINGS, "symbol_map", {}) or {}
    except Exception:
        symbol_map = {}

    # Build "real" tradingsymbols we will look for in Kite instruments
    real_for_logical: Dict[str, str] = {}
    for sym in logical_syms:
        real = symbol_map.get(sym, sym)
        real_for_logical[sym] = real.upper()

    wanted_real = set(real_for_logical.values())

    table = kc.instruments("NSE")
    mp: Dict[str, int] = {}

    for row in table:
        tradingsymbol = row.get("tradingsymbol")
        token = row.get("instrument_token")
        if not tradingsymbol or not token:
            continue

        ts = tradingsymbol.upper()
        if ts not in wanted_real:
            continue

        # Find all logical symbols that map to this real tradingsymbol
        for logical, real_ts in real_for_logical.items():
            if real_ts == ts:
                mp[logical] = int(token)

    missing = sorted(list(set(logical_syms) - set(mp.keys())))
    if missing:
        log.warning("Missing tokens for symbols: %s", ",".join(missing))
    return mp



def tick_stream(symbols: Iterable[str] | None = None) -> Iterator[List[Tuple[str, float, float]]]:
    """Blocking generator yielding batches of ticks.

    Yields:
        List[Tuple[symbol, ts_epoch, last_price]]
    """
    if symbols is None:
        symbols = SETTINGS.symbols

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

    tok2sym: Dict[int, str] = {tok: sym for sym, tok in sym2tok.items()}

    # Queue where on_ticks callback pushes ticks, main loop consumes
    q: "queue.Queue[Tuple[str, float, float]]" = queue.Queue(maxsize=5000)

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
                if hasattr(ts_val, "timestamp"):
                    ts_epoch = float(ts_val.timestamp())
                else:
                    ts_epoch = float(ts_val)
            except Exception:
                ts_epoch = now

            triple = (sym, ts_epoch, float(ltp))
            try:
                q.put_nowait(triple)
            except queue.Full:
                # drop if consumer lags
                pass

    def on_connect(ws, resp):
        log.info("KiteTicker connected; subscribing to %d tokens", len(tokens))
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)

    def on_close(ws, code, reason):
        log.warning("KiteTicker closed: %s %s", code, reason)

    def on_error(ws, code, reason):
        log.error("KiteTicker error: %s %s", code, reason)

    kws = KiteTicker(api_key, access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    # This starts Twisted reactor in a separate thread; this call returns immediately.
    kws.connect(threaded=True, disable_ssl_verification=False)

    # Main consumer loop
    while True:
        first = q.get()  # blocking
        batch: List[Tuple[str, float, float]] = [first]

        # Drain any extra ticks
        while True:
            try:
                more = q.get_nowait()
            except queue.Empty:
                break
            else:
                batch.append(more)

        yield batch
