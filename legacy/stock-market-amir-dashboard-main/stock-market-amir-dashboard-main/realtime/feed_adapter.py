from __future__ import annotations
import asyncio, os, time, random
from typing import Dict, Any, Callable, DefaultDict
from collections import defaultdict

# ---- Small helpers ---------------------------------------------------------

def _env_has_kite() -> bool:
    return bool(os.getenv("KITE_API_KEY") and os.getenv("KITE_ACCESS_TOKEN"))

def _now_ms() -> int:
    return int(time.time() * 1000)

# ---- FeedAdapter: either KITE (real) or MOCK (simulated) -------------------

class FeedAdapter:
    """
    One class, two behaviors:
      - KITE mode: uses Zerodha Kite quote() polling to produce snapshots + deltas
      - MOCK mode: deterministic random-walk ticks (what you had earlier)

    Public API (used by ws_gateway.py):
      - await get_snapshot(symbol) -> dict
      - ensure_stream(symbol, on_delta) -> starts async task to push {symbol, delta}
    """

    def __init__(self):
        self._mode = "KITE" if _env_has_kite() else "MOCK"
        self._state: Dict[str, Dict[str, Any]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._subs: DefaultDict[str, list[Callable[[str, Dict[str, Any]], None]]] = defaultdict(list)

        # MOCK rng
        self._rng = random.Random(42)

        # KITE client (lazy import to avoid dependency if not needed)
        self._kite = None
        if self._mode == "KITE":
            try:
                from kiteconnect import KiteConnect
                self._kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
                self._kite.set_access_token(os.environ["KITE_ACCESS_TOKEN"])
                print("FeedAdapter mode: KITE")
            except Exception as e:
                print("FeedAdapter KITE init failed, falling back to MOCK:", e)
                self._mode = "MOCK"

        if self._mode == "MOCK":
            print("FeedAdapter mode: MOCK")

    # -------------- Public API ----------------

    async def get_snapshot(self, symbol: str) -> Dict[str, Any]:
        if self._mode == "KITE":
            return await self._kite_snapshot(symbol)
        else:
            return await self._mock_snapshot(symbol)

    def ensure_stream(self, symbol: str, on_delta: Callable[[str, Dict[str, Any]], None]) -> None:
        self._subs[symbol].append(on_delta)
        # Start a per-symbol task if not running
        if symbol not in self._tasks or self._tasks[symbol].done():
            if self._mode == "KITE":
                self._tasks[symbol] = asyncio.create_task(self._run_kite(symbol))
            else:
                self._tasks[symbol] = asyncio.create_task(self._run_mock(symbol))

    # -------------- KITE mode -----------------

    async def _kite_snapshot(self, symbol: str) -> Dict[str, Any]:
        # Use quote() to build snapshot
        q = self._kite.quote([symbol])  # raises on bad token or bad symbol
        if symbol not in q:
            # If symbol invalid, raise — ws_gateway will handle exceptions
            raise ValueError(f"Symbol not found on Kite: {symbol}")
        row = q[symbol]
        last = float(row.get("last_price") or 0.0)
        ohlc = row.get("ohlc") or {}
        depth = row.get("depth") or {}
        top_buy = (depth.get("buy") or [{}])[0] if depth.get("buy") else {}
        top_sell = (depth.get("sell") or [{}])[0] if depth.get("sell") else {}
        bid = float(top_buy.get("price") or 0.0)
        ask = float(top_sell.get("price") or 0.0)
        vol = int(row.get("volume") or 0)

        snap = {
            "last": round(last, 2),
            "bid": round(bid, 2),
            "ask": round(ask, 2),
            "volume": vol,
            "ohlc": {
                "o": float(ohlc.get("open") or last),
                "h": float(ohlc.get("high") or last),
                "l": float(ohlc.get("low") or last),
                "c": float(ohlc.get("close") or last),
            },
            "ts": _now_ms(),
            "seq": 0,
        }
        # cache baseline
        self._state[symbol] = dict(snap)
        return snap

    async def _run_kite(self, symbol: str) -> None:
        """
        Poll quote() periodically and emit deltas.
        Note: Don’t set too tight; respect rate limits. ~1s is fine for now.
        """
        try:
            # Ensure we have baseline
            if symbol not in self._state:
                await self._kite_snapshot(symbol)

            while True:
                await asyncio.sleep(1.0)
                q = self._kite.quote([symbol])
                row = q.get(symbol)
                if not row:
                    continue

                last = float(row.get("last_price") or 0.0)
                depth = row.get("depth") or {}
                top_buy = (depth.get("buy") or [{}])[0] if depth.get("buy") else {}
                top_sell = (depth.get("sell") or [{}])[0] if depth.get("sell") else {}
                bid = float(top_buy.get("price") or 0.0)
                ask = float(top_sell.get("price") or 0.0)
                vol = int(row.get("volume") or 0)

                state = self._state[symbol]
                state["seq"] += 1
                state["last"] = round(last, 2)
                state["bid"] = round(bid, 2)
                state["ask"] = round(ask, 2)
                state["volume"] = vol
                state["ts"] = _now_ms()

                delta = {
                    "last": state["last"],
                    "bid": state["bid"],
                    "ask": state["ask"],
                    "volume": state["volume"],
                    "ts": state["ts"],
                    "seq": state["seq"],
                }

                for cb in list(self._subs.get(symbol, [])):
                    try:
                        cb(symbol, delta)
                    except Exception:
                        pass

        except asyncio.CancelledError:
            pass
        except Exception as e:
            # If token expires mid-run, the loop will stop; ws can show an error.
            print(f"KITE stream for {symbol} stopped: {e}")

    # -------------- MOCK mode (your existing logic) --------------

    async def _mock_snapshot(self, symbol: str) -> Dict[str, Any]:
        row = self._state.get(symbol)
        if row is None:
            px = 100.0 + self._rng.random() * 50.0
            row = {
                "last": round(px, 2),
                "bid": round(px - 0.05, 2),
                "ask": round(px + 0.05, 2),
                "volume": 0,
                "ohlc": {"o": px, "h": px, "l": px, "c": px},
                "ts": _now_ms(),
                "seq": 0,
            }
            self._state[symbol] = row
        return dict(row)

    async def _run_mock(self, symbol: str) -> None:
        try:
            while True:
                await asyncio.sleep(0.2)
                row = self._state.get(symbol)
                if row is None:
                    row = await self._mock_snapshot(symbol)
                    self._state[symbol] = row

                bump = (self._rng.random() - 0.5) * 0.2
                last = max(0.5, row["last"] + bump)
                bid = round(last - 0.05, 2)
                ask = round(last + 0.05, 2)
                vol = row["volume"] + self._rng.randint(1, 10)
                o, h, l, c = row["ohlc"]["o"], row["ohlc"]["h"], row["ohlc"]["l"], last
                h = max(h, last)
                l = min(l, last)

                row.update({
                    "last": round(last, 2),
                    "bid": bid,
                    "ask": ask,
                    "volume": vol,
                    "ohlc": {"o": o, "h": h, "l": l, "c": c},
                    "ts": _now_ms(),
                    "seq": row["seq"] + 1,
                })

                delta = {
                    "last": row["last"],
                    "bid": row["bid"],
                    "ask": row["ask"],
                    "volume": row["volume"],
                    "ts": row["ts"],
                    "seq": row["seq"],
                }

                for cb in list(self._subs.get(symbol, [])):
                    try:
                        cb(symbol, delta)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass
