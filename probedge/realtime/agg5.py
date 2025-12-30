"""probedge.realtime.agg5

Agg5 (Phase-A live feed spine).

Minimum responsibilities (LIVE *and* paper-with-live-feed):
  1) Keep /api/state responsive (clock_ist + health.last_agg5_ts heartbeat)
  2) Maintain state["quotes"][sym] with a stable UI/execution contract:
        {"ltp":..., "ohlc": {"o","h","l","c"}, "volume":..., "ts_ist":...}
  3) Maintain state["last_closed"][sym] on each 5-min close:
        {"o","h","l","c","v","t_start","t_end"}
  4) Best-effort append closed bars to canonical TM5 CSV:
        data/intraday/{sym}_5minute.csv  (date,time,open,high,low,close,volume)

Design constraints:
  - PATCH-only writes via AtomicJSON (never clobber other writers)
  - If Kite isn't configured/logged-in, fall back to TM5 seeding + heartbeat.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from probedge.infra.clock_source import IST, now_ist
from probedge.infra.settings import SETTINGS
from probedge.storage.atomic_json import AtomicJSON
from probedge.storage.resolver import intraday_path, locate_for_read

DEFAULT_SLEEP_SEC = 1.0

log = logging.getLogger(__name__)


def _seed_quotes_from_tm5(symbols: List[str], day: str) -> Dict[str, Any]:
    quotes: Dict[str, Any] = {}
    for sym in symbols:
        try:
            p = locate_for_read("intraday", sym)
            # Best-effort; older files might not have volume.
            try:
                df = pd.read_csv(
                    p,
                    usecols=["date", "time", "open", "high", "low", "close", "volume"],
                    engine="python",
                )
            except Exception:
                df = pd.read_csv(
                    p,
                    usecols=["date", "time", "open", "high", "low", "close"],
                    engine="python",
                )
            d = df[df["date"].astype(str).str.strip() == day]
            last = (d.iloc[-1] if len(d) else df.iloc[-1])  # fallback to latest available bar
            o = float(last["open"])
            h = float(last["high"])
            l = float(last["low"])
            c = float(last["close"])
            v = float(last["volume"]) if "volume" in d.columns else 0.0
            quotes[str(sym)] = {
                "ltp": c,
                "ohlc": {"o": o, "h": h, "l": l, "c": c},
                "volume": v,
                "ts_ist": now_ist().isoformat(),
            }
        except Exception:
            # best-effort seeding only
            continue
    return quotes


def _bar_bucket_start(dt_ist: datetime, bar_seconds: int) -> datetime:
    """Floor an IST datetime to the start of its bar bucket."""
    if dt_ist.tzinfo is None:
        dt_ist = dt_ist.replace(tzinfo=IST)
    dt0 = dt_ist.replace(second=0, microsecond=0)
    step_min = max(1, int(bar_seconds // 60))
    minute = (dt0.minute // step_min) * step_min
    return dt0.replace(minute=minute)


def _tail_last_dt_key(p: Path) -> Optional[str]:
    """Return last row's (date,time) key from a CSV, or None."""
    try:
        if not p.exists():
            return None
        with p.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size <= 0:
                return None
            f.seek(max(0, size - 4096))
            chunk = f.read().decode("utf-8", errors="ignore")
        lines = [ln.strip() for ln in chunk.splitlines() if ln.strip()]
        if not lines:
            return None
        last = lines[-1]
        if last.lower().startswith("date,"):
            return None
        parts = [x.strip() for x in last.split(",")]
        if len(parts) < 2:
            return None
        return f"{parts[0]} {parts[1]}"
    except Exception:
        return None


def _append_tm5_row(sym: str, bar_start_ist: datetime, ohlc: Dict[str, float], volume: float) -> None:
    """Append one closed bar to canonical TM5 CSV, best-effort + dedup last row."""
    try:
        p = intraday_path(sym)
        p.parent.mkdir(parents=True, exist_ok=True)

        day = bar_start_ist.date().isoformat()
        tm = bar_start_ist.strftime("%H:%M:%S")
        key = f"{day} {tm}"

        # Cheap dedup against last row
        if _tail_last_dt_key(p) == key:
            return

        header_needed = not p.exists() or p.stat().st_size == 0
        row = f"{day},{tm},{ohlc['o']},{ohlc['h']},{ohlc['l']},{ohlc['c']},{int(volume)}\n"
        with p.open("a", encoding="utf-8") as f:
            if header_needed:
                f.write("date,time,open,high,low,close,volume\n")
            f.write(row)
    except Exception:
        # Never crash agg5 for file IO.
        return


def run_agg(symbols: List[str], bar_seconds: int = 300, state_path: Optional[str] = None) -> None:
    symbols = [str(s).upper().strip() for s in (symbols or []) if str(s).strip()]
    # state path must be DATA_DIR isolated; SETTINGS.paths.state is absolute in our patched settings
    sp = state_path or SETTINGS.paths.state
    aj = AtomicJSON(sp)

    # Seed quotes once from existing TM5 so UI has an LTP immediately
    st = aj.read(default={}) or {}
    day = (st.get("date") or now_ist().date().isoformat())
    quotes = _seed_quotes_from_tm5(symbols, day)
    if quotes:
        aj.write({"quotes": quotes})

    # Try to enable live ticks (Kite). If unavailable, fall back to heartbeat-only.
    tick_iter = None
    tick_source = "seed"
    try:
        if os.environ.get("PB_ENABLE_KITE_TICKS", "1").strip() != "0":
            from probedge.realtime.kite_live import tick_stream  # local import (kiteconnect may be optional)
            tick_iter = tick_stream(symbols)
            tick_source = "kite"
            log.info("agg5: live ticks enabled (Kite)")
    except Exception as e:
        tick_iter = None
        tick_source = "seed"
        log.warning("agg5: live ticks unavailable; using seed+heartbeat only (%s)", e)

    # In-memory 5m bar state: per symbol (bar_start_ist, ohlc, volume)
    cur_bar: Dict[str, Tuple[datetime, Dict[str, float], float]] = {}
    last_tick_epoch: Dict[str, float] = {}

    # Main loop
    while True:
        loop_now = now_ist()

        # 1) Pull a tick batch (blocking up to ~1s in Kite mode)
        batch = []
        if tick_iter is not None:
            try:
                batch = next(tick_iter) or []
            except Exception:
                # If ticker dies, degrade gracefully (do not crash Phase-A)
                log.exception("agg5: tick stream crashed; falling back to heartbeat-only")
                tick_iter = None
                tick_source = "seed"
                batch = []
        else:
            time.sleep(DEFAULT_SLEEP_SEC)

        quotes_patch: Dict[str, Any] = {}
        last_closed_patch: Dict[str, Any] = {}

        # 2) Update bars from ticks
        for sym, ts_epoch, ltp in batch:
            s = str(sym).upper().strip()
            try:
                px = float(ltp)
            except Exception:
                continue

            # Convert to IST
            try:
                ts_ist = datetime.fromtimestamp(float(ts_epoch), tz=IST)
            except Exception:
                ts_ist = loop_now

            bucket = _bar_bucket_start(ts_ist, bar_seconds)
            last_tick_epoch[s] = float(ts_epoch)

            prev = cur_bar.get(s)
            if prev is None:
                ohlc = {"o": px, "h": px, "l": px, "c": px}
                cur_bar[s] = (bucket, ohlc, 0.0)
            else:
                prev_start, ohlc, vol = prev
                if bucket != prev_start:
                    # Close previous bar
                    bar_end = prev_start + timedelta(seconds=bar_seconds)
                    last_closed_patch[s] = {
                        "o": float(ohlc["o"]),
                        "h": float(ohlc["h"]),
                        "l": float(ohlc["l"]),
                        "c": float(ohlc["c"]),
                        "v": float(vol),
                        "t_start": prev_start.isoformat(),
                        "t_end": bar_end.isoformat(),
                    }
                    _append_tm5_row(s, prev_start, ohlc, vol)

                    # Start new bar in new bucket
                    ohlc = {"o": px, "h": px, "l": px, "c": px}
                    cur_bar[s] = (bucket, ohlc, 0.0)
                else:
                    # Update in-progress bar
                    ohlc["h"] = float(max(ohlc["h"], px))
                    ohlc["l"] = float(min(ohlc["l"], px))
                    ohlc["c"] = px
                    cur_bar[s] = (prev_start, ohlc, vol)

            # Quote patch always reflects latest in-progress bar
            _, ohlc, vol = cur_bar[s]
            quotes_patch[s] = {
                "ltp": px,
                "ohlc": {
                    "o": float(ohlc["o"]),
                    "h": float(ohlc["h"]),
                    "l": float(ohlc["l"]),
                    "c": float(ohlc["c"]),
                },
                "volume": float(vol),
                "ts_ist": ts_ist.isoformat(),
            }

        # 2b) Bar rollover on wall-clock (prevents stale OHLC leaking into later bars)
        expected_bucket = _bar_bucket_start(loop_now, bar_seconds)
        for s, prev in list(cur_bar.items()):
            prev_start, ohlc, vol = prev
            if prev_start == expected_bucket:
                continue

            bar_end = prev_start + timedelta(seconds=bar_seconds)
            if loop_now < bar_end:
                continue

            last_closed_patch.setdefault(
                s,
                {
                    "o": float(ohlc["o"]),
                    "h": float(ohlc["h"]),
                    "l": float(ohlc["l"]),
                    "c": float(ohlc["c"]),
                    "v": float(vol),
                    "t_start": prev_start.isoformat(),
                    "t_end": bar_end.isoformat(),
                },
            )
            _append_tm5_row(s, prev_start, ohlc, vol)

            carry = float(ohlc["c"])
            new_ohlc = {"o": carry, "h": carry, "l": carry, "c": carry}
            cur_bar[s] = (expected_bucket, new_ohlc, 0.0)

            quotes_patch.setdefault(
                s,
                {
                    "ltp": carry,
                    "ohlc": {"o": carry, "h": carry, "l": carry, "c": carry},
                    "volume": 0.0,
                    "ts_ist": loop_now.isoformat(),
                },
            )

        # 3) Heartbeat / patch-write
        patch: Dict[str, Any] = {
            "clock_ist": loop_now.isoformat(),
            "health": {
                "last_agg5_ts": time.time(),
                "tick_source": tick_source,
                "last_tick_ts": max(last_tick_epoch.values()) if last_tick_epoch else None,
            },
        }
        if quotes_patch:
            patch["quotes"] = quotes_patch
        if last_closed_patch:
            patch["last_closed"] = last_closed_patch

        aj.write(patch)
