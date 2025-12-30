# apps/sim/replay_ticks.py
"""
Replay tick parquet into:
- SIM 5-minute intraday CSVs (data/intraday/*_5minute.csv)
- SIM live_state.json updates (quotes + last_closed)
- A 09:40:01 PlanSnapshot (single source of truth) written into live_state.json

This is the "weekend live-day simulator" for wiring.

It does NOT touch your live DATA_DIR if you pass a separate --data-dir.

Run order (recommended)
1) Start SIM API (separate port) pointing to SIM data dir:
     DATA_DIR=/Users/aamir/Downloads/probedge/probedge_sim \
     MODE=paper \
     uvicorn apps.api.main:app --host 127.0.0.1 --port 9102

2) Open UI:
     http://127.0.0.1:9102/live?day=2025-12-12

3) Replay (fast):
     python -m apps.sim.replay_ticks \
       --data-dir /Users/aamir/Downloads/probedge/probedge_sim \
       --day 2025-12-12 \
       --daily-risk 10000 \
       --speed 0 \
       --until 09:41

Verification (key)
- /api/plan_snapshot?day=YYYY-MM-DD returns:
    status READY or READY_PARTIAL
    built_for_ist == "09:40:01"
    watermark_bar5_all == true (or false with skip reasons)
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd

IST = ZoneInfo("Asia/Kolkata")



# --- PROBEDGE_JSON_DEFAULT_ENCODER ---
def _json_default(o):
    # Convert Bar/dataclass/namedtuple-like objects to JSON-safe dicts.
    try:
        if hasattr(o, "to_dict"):
            return o.to_dict()
    except Exception:
        pass

    try:
        if hasattr(o, "_asdict"):
            return o._asdict()
    except Exception:
        pass

    try:
        if hasattr(o, "__dict__"):
            d = dict(o.__dict__)
            for k, v in list(d.items()):
                try:
                    if hasattr(v, "isoformat"):
                        d[k] = v.isoformat()
                except Exception:
                    pass
            return d
    except Exception:
        pass

    return str(o)

def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, default=_json_default), encoding="utf-8")
    tmp.replace(path)


def _load_ticks(day_dir: Path) -> pd.DataFrame:
    files = sorted(day_dir.glob("*.parquet"))
    if not files:
        raise SystemExit(f"No tick parquet files under {day_dir}")
    dfs = []
    for fp in files:
        df = pd.read_parquet(fp)
        df.columns = [str(c).strip() for c in df.columns]
        # minimal required
        if not {"ts_epoch", "symbol", "ltp"}.issubset(set(df.columns)):
            raise SystemExit(f"Bad tick parquet schema: {fp}")
        dfs.append(df[["ts_epoch", "symbol", "ltp", "vol"]] if "vol" in df.columns else df[["ts_epoch", "symbol", "ltp"]])
    all_df = pd.concat(dfs, ignore_index=True)
    all_df["ts_epoch"] = pd.to_numeric(all_df["ts_epoch"], errors="coerce")
    all_df["ltp"] = pd.to_numeric(all_df["ltp"], errors="coerce")
    all_df = all_df.dropna(subset=["ts_epoch", "symbol", "ltp"]).sort_values("ts_epoch").reset_index(drop=True)
    return all_df


def _floor_5min(ts_ist: datetime) -> datetime:
    m = (ts_ist.minute // 5) * 5
    return ts_ist.replace(minute=m, second=0, microsecond=0)


@dataclass
class Bar:
    start: datetime
    o: float
    h: float
    l: float
    c: float
    v: int = 0

    def as_row(self) -> Dict[str, Any]:
        return {
            "DateTime": self.start.replace(tzinfo=None),
            "Open": float(self.o),
            "High": float(self.h),
            "Low": float(self.l),
            "Close": float(self.c),
            "Volume": int(self.v),
        }


class Agg5:
    def __init__(self) -> None:
        self.cur: Dict[str, Optional[Bar]] = {}
        self.bars: Dict[str, List[Bar]] = {}
        self.last_closed: Dict[str, Dict[str, Any]] = {}

    def on_tick(self, sym: str, ts_epoch: float, ltp: float, vol: int = 0) -> List[Tuple[str, Bar]]:
        # Convert epoch -> IST
        ts_ist = datetime.fromtimestamp(float(ts_epoch), tz=ZoneInfo("UTC")).astimezone(IST)
        bucket = _floor_5min(ts_ist)

        out_closed: List[Tuple[str, Bar]] = []
        b = self.cur.get(sym)

        if b is None:
            b = Bar(start=bucket, o=ltp, h=ltp, l=ltp, c=ltp, v=int(vol or 0))
            self.cur[sym] = b
            return out_closed

        # bucket rollover
        if bucket != b.start:
            out_closed.append((sym, b))
            self.bars.setdefault(sym, []).append(b)
            self.last_closed[sym] = {
                "bar_start": b.start.time().isoformat(timespec="seconds"),
                "o": float(b.o),
                "h": float(b.h),
                "l": float(b.l),
                "c": float(b.c),
                "v": int(b.v),
            }
            # start new bar
            b = Bar(start=bucket, o=ltp, h=ltp, l=ltp, c=ltp, v=int(vol or 0))
            self.cur[sym] = b
            return out_closed

        # update in-bucket
        b.c = ltp
        b.h = max(b.h, ltp)
        b.l = min(b.l, ltp)
        b.v += int(vol or 0)
        return out_closed

    def finalize(self) -> None:
        # close all open bars
        for sym, b in list(self.cur.items()):
            if b is None:
                continue
            self.bars.setdefault(sym, []).append(b)
            self.last_closed[sym] = {
                "bar_start": b.start.time().isoformat(timespec="seconds"),
                "o": float(b.o),
                "h": float(b.h),
                "l": float(b.l),
                "c": float(b.c),
                "v": int(b.v),
            }
            self.cur[sym] = None


def _write_intraday_csv(data_dir: Path, agg: Agg5, day: str) -> None:
    intraday = data_dir / "data" / "intraday"
    intraday.mkdir(parents=True, exist_ok=True)
    for sym, bars in agg.bars.items():
        if not bars:
            continue
        df = pd.DataFrame([b.as_row() for b in bars]).sort_values("DateTime").reset_index(drop=True)
        # keep only the day
        df["Date"] = pd.to_datetime(df["DateTime"]).dt.normalize()
        df = df[df["Date"] == pd.to_datetime(day).normalize()].drop(columns=["Date"])
        out_fp = intraday / f"{sym}_5minute.csv"
        df.to_csv(out_fp, index=False)


def _build_plan_snapshot(data_dir: Path, day: str, daily_risk: float) -> Dict[str, Any]:
    # Must set env before importing SETTINGS/plan_core
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ.setdefault("MODE", "paper")

    from probedge.infra.settings import SETTINGS  # noqa
    from probedge.decision.plan_core import build_parity_plan  # noqa



    symbols = list(getattr(SETTINGS, "symbols", []) or [])
    if not symbols:
        # fallback: infer from intraday CSVs
        intraday = data_dir / "data" / "intraday"
        symbols = sorted({p.name.replace("_5minute.csv", "") for p in intraday.glob("*_5minute.csv")})

    raw_plans = [build_parity_plan(sym, day_str=day) for sym in symbols]

    # active picks before qty split
    actives = [p for p in raw_plans if str(p.get("pick", "")).upper() in ("BULL", "BEAR")]
    active_n = len(actives)
    risk_per_trade = float(daily_risk) / active_n if active_n > 0 else 0.0

    plans = []
    active_trades = 0
    planned_total = 0.0

    for p in raw_plans:
        pick = str(p.get("pick", "")).upper()
        rps = float(p.get("risk_per_share") or 0.0)
        # default: keep tags/pick/conf/reason/entry/stop/t1/t2
        out = dict(p)
        out["daily_risk_rs"] = float(daily_risk)
        out["risk_per_trade_rs"] = float(risk_per_trade)

        if pick not in ("BULL", "BEAR") or rps <= 0 or active_n == 0:
            out["qty"] = 0
            out.setdefault("skip", "ABSTAIN" if pick not in ("BULL", "BEAR") else "bad_risk")
            out["planned_risk_rs"] = 0.0
            plans.append(out)
            continue

        qty = int(np.floor(risk_per_trade / rps))
        if qty <= 0:
            out["qty"] = 0
            out["skip"] = "qty=0_split"
            out["planned_risk_rs"] = 0.0
            plans.append(out)
            continue

        out["qty"] = qty
        out.pop("skip", None)
        pr = float(qty) * float(rps)
        out["planned_risk_rs"] = pr
        planned_total += pr
        active_trades += 1
        plans.append(out)

    status = "READY" if active_trades == active_n else "READY_PARTIAL"
    snap = {
        "day": day,
        "status": status,
        "built_at_wall_ist": datetime.now(tz=IST).isoformat(timespec="seconds"),
        "built_for_ist": "09:40:01",
        "version": 1,
        "portfolio_plan": {
            "plan_locked": True,
            "daily_risk_rs": float(daily_risk),
            "risk_per_trade_rs": float(risk_per_trade),
            "active_trades": int(active_trades),
            "active_picks": int(active_n),
            "total_planned_rs": float(round(planned_total, 3)),
            "plans": plans,
        },
    }
    return snap

def _extract_tags(p: Dict[str, Any]) -> Dict[str, Any]:
    t = (p or {}).get("tags") or {}

    def first(*keys):
        for k in keys:
            v = t.get(k)
            if v not in (None, ""):
                return v
        for k in keys:
            v = (p or {}).get(k)
            if v not in (None, ""):
                return v
        return None

    return {
        "PDC": first("PDC", "PrevDayContext", "pdc", "prev_day_context"),
        "OL":  first("OL",  "OpenLocation",  "ol",  "open_location"),
        "OT":  first("OT",  "OpeningTrend",  "ot",  "opening_trend"),
    }

def _pos_from_plan(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pick = str((p or {}).get("pick") or "").upper()
    qty = int((p or {}).get("qty") or 0)
    if pick not in ("BULL", "BEAR") or qty <= 0:
        return None

    def f(x):
        try:
            v = float(x)
            return v if v != 0 else None
        except Exception:
            return None

    side = "LONG" if pick == "BULL" else "SHORT"

    return {
        "status": "PENDING",         # PENDING -> OPEN -> CLOSED
        "side": side,
        "qty": qty,
        "entry": f(p.get("entry") or p.get("Entry") or p.get("trigger") or p.get("Trigger") or p.get("limit") or p.get("Limit")),
        "stop":  f(p.get("stop")  or p.get("sl")   or p.get("SL")   or p.get("Stop") or p.get("stop_loss")),
        "tp1":   f(p.get("tp1")   or p.get("t1")   or p.get("T1")   or p.get("target1")),
        "tp2":   f(p.get("tp2")   or p.get("t2")   or p.get("T2")   or p.get("target2")),
        "entry_price": None,
        "exit_price": None,
        "entry_time_ist": None,
        "exit_time_ist": None,
        "exit_reason": None,
        "tp1_hit": False,
        "open_pnl_rs": 0.0,
        "realized_pnl_rs": 0.0,
    }


def _update_positions_and_pnl(state: Dict[str, Any], ts_ist: datetime) -> None:
    positions = state.get("positions") or {}
    quotes = state.get("quotes") or {}

    for sym, pos in positions.items():
        q = quotes.get(sym) or {}
        ltp = q.get("ltp")
        if ltp is None:
            continue
        try:
            ltp = float(ltp)
        except Exception:
            continue

        st = pos.get("status")

        entry = pos.get("entry")
        stop  = pos.get("stop")
        tp1   = pos.get("tp1")
        tp2   = pos.get("tp2")
        side  = pos.get("side")
        qty   = int(pos.get("qty") or 0)

        # PENDING -> OPEN when entry triggers (limit-style fill at entry)
        if st == "PENDING" and entry is not None:
            if side == "LONG" and ltp >= float(entry):
                pos["status"] = "OPEN"
                pos["entry_price"] = float(entry)
                pos["entry_time_ist"] = ts_ist.isoformat(timespec="seconds")
            elif side == "SHORT" and ltp <= float(entry):
                pos["status"] = "OPEN"
                pos["entry_price"] = float(entry)
                pos["entry_time_ist"] = ts_ist.isoformat(timespec="seconds")

        # If OPEN, update pnl and evaluate exits
        if pos.get("status") == "OPEN" and pos.get("entry_price") is not None:
            ep = float(pos["entry_price"])

            if side == "LONG":
                pos["open_pnl_rs"] = (ltp - ep) * qty
                if tp1 is not None and (not pos.get("tp1_hit")) and ltp >= float(tp1):
                    pos["tp1_hit"] = True
                if stop is not None and ltp <= float(stop):
                    exit_px = float(stop)
                    pos["status"] = "CLOSED"
                    pos["exit_reason"] = "SL"
                elif tp2 is not None and ltp >= float(tp2):
                    exit_px = float(tp2)
                    pos["status"] = "CLOSED"
                    pos["exit_reason"] = "T2"
                else:
                    exit_px = None
            else:  # SHORT
                pos["open_pnl_rs"] = (ep - ltp) * qty
                if tp1 is not None and (not pos.get("tp1_hit")) and ltp <= float(tp1):
                    pos["tp1_hit"] = True
                if stop is not None and ltp >= float(stop):
                    exit_px = float(stop)
                    pos["status"] = "CLOSED"
                    pos["exit_reason"] = "SL"
                elif tp2 is not None and ltp <= float(tp2):
                    exit_px = float(tp2)
                    pos["status"] = "CLOSED"
                    pos["exit_reason"] = "T2"
                else:
                    exit_px = None

            # On CLOSE: realize pnl at exit_px and zero open pnl
            if pos.get("status") == "CLOSED" and exit_px is not None:
                pos["exit_price"] = float(exit_px)
                pos["exit_time_ist"] = ts_ist.isoformat(timespec="seconds")
                if side == "LONG":
                    pos["realized_pnl_rs"] = (float(exit_px) - ep) * qty
                else:
                    pos["realized_pnl_rs"] = (ep - float(exit_px)) * qty
                pos["open_pnl_rs"] = 0.0

    # recompute totals
    open_pnl = 0.0
    realized = 0.0
    for pos in positions.values():
        realized += float(pos.get("realized_pnl_rs") or 0.0)
        if pos.get("status") == "OPEN":
            open_pnl += float(pos.get("open_pnl_rs") or 0.0)

    state["positions"] = positions
    day_total = open_pnl + realized
    state["pnl"] = {
        "day": day_total,
        "day_total_rs": day_total,
        "open": open_pnl,
        "open_rs": open_pnl,
        "realized": realized,
        "realized_rs": realized,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="SIM DATA_DIR that contains data/ticks/<day>/*.parquet")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--daily-risk", type=float, default=10000.0)
    ap.add_argument("--speed", type=float, default=0.0, help="Replay speed factor. 0 = no sleep (fastest)")
    ap.add_argument("--until", default="09:41", help="Stop replay after this IST time (HH:MM or HH:MM:SS)")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()
    day = args.day.strip()
    day_dir = data_dir / "data" / "ticks" / day
    state_fp = data_dir / "data" / "state" / "live_state.json"

    until_parts = [int(x) for x in args.until.split(":")]
    until_t = dtime(until_parts[0], until_parts[1], until_parts[2] if len(until_parts) > 2 else 0)

    ticks = _load_ticks(day_dir)

    agg = Agg5()
    # Make sure plan_core reads from SIM DATA_DIR
    os.environ["DATA_DIR"] = str(data_dir)
    os.environ.setdefault("MODE", "paper")

    from probedge.decision.plan_core import build_parity_plan  # noqa
    from probedge.infra.settings import SETTINGS  # noqa
    from probedge.storage.resolver import ALIASES as _ALIASES  # noqa

    ALIASES = dict(_ALIASES or {})                 # logical -> tradable
    REV_ALIASES = {v: k for k, v in ALIASES.items()}  # tradable -> logical

    # Logical universe for tags/plan (UI symbols)
    symbols_all = [str(s).upper() for s in (getattr(SETTINGS, "symbols", []) or [])]
    if not symbols_all:
        # fallback: derive from parquet symbols (tradable) but convert to logical
        symbols_all = sorted({REV_ALIASES.get(str(s).upper(), str(s).upper()) for s in ticks["symbol"].unique()})


    pdc_done = False
    ol_done = False


    # state scaffold
    state: Dict[str, Any] = {
        "sim": True,
        "sim_day": day,
        "clock_ist": None,
        "daily_risk_rs": float(args.daily_risk),
        "quotes": {},
        "positions": {},   # symbol -> position dict
        "pnl": {"day": 0.0, "open": 0.0, "realized": 0.0},

        "day_open": {},
        "tags": {},          # <-- ADD THIS
        "last_closed": {},
        "health": {
            "system_status": "OK",   # <-- change UP -> OK (UI expects OK/WARN)

            "reason": "SIM replay running",
            "last_agg5_ts": time.time(),
            "last_batch_ts": None,
        },
        "plan_snapshot": None,
        "portfolio_plan": None,
    }
    _atomic_write_json(state_fp, state)

    # trigger flags
    snapshot_done = False
    last_flush = time.time()

    # replay loop
    prev_ts = None
    for row in ticks.itertuples(index=False):
        ts_epoch = float(row.ts_epoch)
        sym_trad = str(row.symbol).upper()            # e.g. TMPV
        sym_ui = REV_ALIASES.get(sym_trad, sym_trad)  # e.g. TATAMOTORS (logical)
        ltp = float(row.ltp)
        vol = int(getattr(row, "vol", 0) or 0)

        ts_ist = datetime.fromtimestamp(ts_epoch, tz=ZoneInfo("UTC")).astimezone(IST)
        if ts_ist.time() > until_t:
            break

        if prev_ts is not None and args.speed and args.speed > 0:
            dt = max(0.0, ts_epoch - prev_ts)
            time.sleep(dt / args.speed)
        prev_ts = ts_epoch

        closed = agg.on_tick(sym_trad, ts_epoch, ltp, vol)


        # update quotes & last_closed
        
        if sym_ui not in state.get("day_open", {}):
            state["day_open"][sym_ui] = float(ltp)
        base = float(state["day_open"].get(sym_ui) or ltp)

        chg = float(ltp) - base
        chg_pct = (chg / base * 100.0) if base else 0.0

        # publish a quote object compatible with multiple UIs
        state["quotes"][sym_ui] = {
        "ltp": float(ltp),
        "last_price": float(ltp),
        "lp": float(ltp),
        "price": float(ltp),
        "change": float(chg),
        "chg": float(chg),
        "change_pct": float(chg_pct),
        "chg_pct": float(chg_pct),
        "pct": float(chg_pct),
        "ts_epoch": float(ts_epoch),
        }
        iso_ist = ts_ist.isoformat(timespec="seconds")
        state["sim_now_ist"] = iso_ist
        state["clock_ist"] = iso_ist

        # --- progressive tags like a LIVE day ---
        if (not pdc_done) and (ts_ist.time() >= dtime(9, 25, 0)):
            # write partial closed bars so far (no finalize; no future bars)
            _write_intraday_csv(data_dir, agg, day)

            for s in symbols_all:
                try:
                    p = build_parity_plan(s, day_str=day)
                    tags = _extract_tags(p)
                    if tags.get("PDC") is not None:
                        state["tags"].setdefault(s, {})["PDC"] = tags["PDC"]
                except Exception:
                    pass

            pdc_done = True

        if (not ol_done) and (ts_ist.time() >= dtime(9, 30, 0)):
            _write_intraday_csv(data_dir, agg, day)

            for s in symbols_all:
                try:
                    p = build_parity_plan(s, day_str=day)
                    tags = _extract_tags(p)
                    if tags.get("PDC") is not None:
                        state["tags"].setdefault(s, {})["PDC"] = tags["PDC"]
                    if tags.get("OL") is not None:
                        state["tags"].setdefault(s, {})["OL"] = tags["OL"]
                except Exception:
                    pass

            ol_done = True


        # propagate closed bars info
        if closed:
            state["last_closed"] = {REV_ALIASES.get(k, k): v for k, v in agg.last_closed.items()}


        # build snapshot right after we have bar-5 closed (09:35 bucket) and time >= 09:40:01
        if (not snapshot_done) and (ts_ist.time() >= dtime(9, 40, 1)):
            # write intraday CSVs up to this point
            _write_intraday_csv(data_dir, agg, day)
            snap = _build_plan_snapshot(data_dir, day, args.daily_risk)
            if isinstance(snap, dict):
                snap["built_at_sim_ist"] = state.get("clock_ist") or state.get("sim_now_ist")
                snap["built_at"] = snap["built_at_sim_ist"] or snap.get("built_at_wall_ist")

            # publish OT from the snapshot plans into state_raw.tags (post 09:40 only)
            try:
                plans = ((snap or {}).get("portfolio_plan") or {}).get("plans") or []
                for pp in plans:
                    ss = str(pp.get("symbol") or "").upper()
                    if not ss:
                        continue
                    tags = _extract_tags(pp)
                    if tags.get("PDC") is not None:
                        state["tags"].setdefault(ss, {})["PDC"] = tags["PDC"]
                    if tags.get("OL") is not None:
                        state["tags"].setdefault(ss, {})["OL"] = tags["OL"]
                    if tags.get("OT") is not None:
                        state["tags"].setdefault(ss, {})["OT"] = tags["OT"]
            except Exception:
                pass


            # Ensure API-compatible field exists
            if isinstance(snap, dict) and "built_at" not in snap:
                snap["built_at"] = snap.get("built_at_wall_ist") or datetime.now(tz=IST).isoformat(timespec="seconds")

            # Write per-day archived snapshot (THIS is what /api/plan_snapshot prefers)
            snap_dir = state_fp.parent / "plan_snapshots"
            snap_dir.mkdir(parents=True, exist_ok=True)
            snap_path = snap_dir / f"{day}.json"
            _atomic_write_json(snap_path, snap)

            # Also mirror into live_state for convenience
            state["plan_snapshot"] = snap
            state["portfolio_plan"] = snap.get("portfolio_plan")
            # seed positions from snapshot (only qty>0)
            pos = {}
            plans = ((snap or {}).get("portfolio_plan") or {}).get("plans") or []
            for pp in plans:
                sym = str(pp.get("symbol") or "").upper()
                if not sym:
                    continue
                one = _pos_from_plan(pp)
                if one:
                    pos[sym] = one
            state["positions"] = pos
            state["pnl"] = {"day": 0.0, "open": 0.0, "realized": 0.0}

            state["health"]["last_batch_ts"] = time.time()
            snapshot_done = True
            _atomic_write_json(state_fp, state)
            last_flush = time.time()

        # --- post-snapshot paper execution loop (positions + pnl) ---
        if snapshot_done:
            _update_positions_and_pnl(state, ts_ist)


        # flush state occasionally (1/sec wall)
        now = time.time()
        if now - last_flush >= 1.0:
            state["health"]["last_agg5_ts"] = now
            _atomic_write_json(state_fp, state)
            last_flush = now

        if snapshot_done and ts_ist.time() >= until_t:
            break

    # final flush
    state["health"]["last_agg5_ts"] = time.time()
    _atomic_write_json(state_fp, state)

    print("✅ SIM replay complete.")
    if state.get("plan_snapshot"):
        print("PlanSnapshot:", state["plan_snapshot"]["status"], "built_for_ist=", state["plan_snapshot"]["built_for_ist"])
    else:
        # FINALIZE: watermark-based snapshot build + forced bar close
        # If replay stops before 09:40:01, we must NOT attempt watermark/snapshot logic.
        if until_t < dtime(9, 40, 1):
            print("No PlanSnapshot built (until < 09:40:01).")
            return

        if not snapshot_done:
            # Force-close bar-5 (09:35–09:40) even if tick stream stops at 09:39:59.
            try:
                until_dt_ist = datetime.fromisoformat(f"{day}T{until_t.strftime('%H:%M:%S')}").replace(tzinfo=IST)
                force_epoch = float(until_dt_ist.timestamp())
            except Exception:
                force_epoch = float(prev_ts) if prev_ts is not None else time.time()

            # Inject 1 synthetic tick per seen symbol at force_epoch (uses last quote) to close the bar.
            for _sym, _q in (state.get("quotes") or {}).items():
                try:
                    _ltp = float((_q or {}).get("ltp") or 0.0)
                except Exception:
                    _ltp = 0.0
                if _ltp <= 0:
                    continue

                _sym_u = str(_sym).upper()
                _sym_trad = ALIASES.get(_sym_u, _sym_u)  # logical -> tradable
                agg.on_tick(_sym_trad, force_epoch, _ltp, 0)

            # refresh last_closed after forcing close (keys logical for UI)
            state["last_closed"] = {REV_ALIASES.get(k, k): v for k, v in agg.last_closed.items()}
            # write intraday CSVs AFTER forcing the 09:35 bar close
            _write_intraday_csv(data_dir, agg, day)


            # Watermark check: bar-5 is considered closed if last_closed.bar_start >= 09:35:00
            def _bar5_closed(rec) -> bool:
                if not isinstance(rec, dict):
                    return False
                bs = str(rec.get("bar_start") or "").strip()
                if not bs:
                    return False
                if " " in bs:
                    bs = bs.split()[-1]  # allow "YYYY-MM-DD HH:MM:SS"
                try:
                    parts = bs.split(":")
                    hh = int(parts[0]); mm = int(parts[1]); ss = int(parts[2]) if len(parts) > 2 else 0
                except Exception:
                    return False
                return (hh, mm, ss) >= (9, 35, 0)

            symbols_seen = sorted((state.get("quotes") or {}).keys())
            missing_inputs = []
            for _sym in symbols_seen:
                rec = (state.get("last_closed") or {}).get(str(_sym).upper())
                if not _bar5_closed(rec):
                    missing_inputs.append(str(_sym).upper())

            # Eligibility:
            # - if until >= 09:40:01, we publish even if partial (degrade gracefully),
            # - OR if watermark satisfied for at least some symbols.
            snap_deadline = dtime(9, 40, 1)
            eligible = (until_t >= snap_deadline) or (len(missing_inputs) < len(symbols_seen))

            if eligible:
                snap = _build_plan_snapshot(data_dir, day, args.daily_risk)
                if isinstance(snap, dict):
                    snap["built_at"] = snap.get("built_at_wall_ist") or datetime.now(tz=IST).isoformat(timespec="seconds")
                    snap["missing_inputs"] = missing_inputs
                    if missing_inputs:
                        snap["status"] = "READY_PARTIAL"

                # Write per-day archived snapshot (API prefers this)
                snap_dir = state_fp.parent / "plan_snapshots"
                snap_dir.mkdir(parents=True, exist_ok=True)
                snap_path = snap_dir / f"{day}.json"
                _atomic_write_json(snap_path, snap)

                # Mirror into live_state for UI convenience
                state["plan_snapshot"] = snap
                state["portfolio_plan"] = (snap or {}).get("portfolio_plan")

                # Seed tags + positions + pnl (so UI isn't blank if fallback snapshot path triggers)
                try:
                    plans = ((snap or {}).get("portfolio_plan") or {}).get("plans") or []
                    for pp in plans:
                        ss = str(pp.get("symbol") or "").upper()
                        if not ss:
                            continue
                        tags = _extract_tags(pp)
                        if tags.get("PDC") is not None:
                            state["tags"].setdefault(ss, {})["PDC"] = tags["PDC"]
                        if tags.get("OL") is not None:
                            state["tags"].setdefault(ss, {})["OL"] = tags["OL"]
                        if tags.get("OT") is not None:
                            state["tags"].setdefault(ss, {})["OT"] = tags["OT"]

                    pos = {}
                    for pp in plans:
                        ss = str(pp.get("symbol") or "").upper()
                        one = _pos_from_plan(pp)
                        if one and ss:
                            pos[ss] = one
                    state["positions"] = pos
                    state["pnl"] = {"day": 0.0, "open": 0.0, "realized": 0.0}
                except Exception:
                    pass

                state["health"]["last_batch_ts"] = time.time()
                snapshot_done = True
                _atomic_write_json(state_fp, state)
                last_flush = time.time()

        if not snapshot_done:
            print("No PlanSnapshot built (missing bar-5 watermark).")

if __name__ == "__main__":
    main()
