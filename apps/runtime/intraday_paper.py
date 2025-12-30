from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, Any, Literal
from datetime import datetime, time as dtime
from probedge.infra.clock_source import get_now_ist
from probedge.infra.settings import SETTINGS
# --- Clock helper (tz-aware IST) ---
try:
    # Preferred: clock_source provides now_ist()
    from probedge.infra.clock_source import now_ist  # type: ignore
except Exception:  # pragma: no cover
    from probedge.infra.clock_source import get_now_ist as _get_now_ist
    def now_ist():
        return _get_now_ist(None)

from probedge.storage.atomic_json import AtomicJSON

from probedge.risk.engine import compute_risk_state
from probedge.orders.plan_guard import get_locked_portfolio_plan, evaluate_plan_gate

STATE_PATH = Path(SETTINGS.paths.state or "data/state/live_state.json")
aj = AtomicJSON(str(STATE_PATH))

StatusT = Literal["PENDING", "OPEN", "CLOSED"]
SideT = Literal["LONG", "SHORT"]


def _get_float(d: Dict[str, Any], keys, default=None):
    """
    Try multiple possible keys in order and return the first convertible float.
    If nothing found, return default.
    """
    if isinstance(keys, str):
        keys = [keys]
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] is not None:
            try:
                return float(d[k])
            except (TypeError, ValueError):
                continue
    return default


def load_state() -> Dict[str, Any]:
    try:
        return aj.read(default={}) or {}
    except Exception:
        return {}

def save_state(patch: Dict[str, Any]) -> None:
    """PATCH-ONLY state update (prevents clobber across writers)."""
    patch = dict(patch or {})
    # Always stamp clock_ist so UI stays alive (safe for SIM too — SIM will overwrite via its own clock).
    patch.setdefault("clock_ist", get_now_ist(load_state()).isoformat())

    # --- PB_SAVE_PLAN_SNAPSHOT_PATCH_V3 ---
    # Keep plan_snapshot self-consistent & persisted (UI/gates read it)
    try:
        st = None
        for _v in locals().values():
            if isinstance(_v, dict) and ("plan_snapshot" in _v or "portfolio_plan" in _v):
                st = _v
                break

        if isinstance(st, dict):
            ps = st.get("plan_snapshot")
            pp = st.get("portfolio_plan")
            if isinstance(ps, dict) and isinstance(pp, dict):
                # force embed real plan
                ps["portfolio_plan"] = pp
                ps["locked"] = bool(ps.get("locked") or pp.get("plan_locked"))
                ps["status"] = ps.get("status") or ("READY" if ps["locked"] else "MISSING")
                if ps.get("day") is None:
                    ps["day"] = st.get("plan_day") or st.get("date")

                # persist via patch when available
                _patch = locals().get("patch")
                if isinstance(_patch, dict):
                    _patch["plan_snapshot"] = ps
                st["plan_snapshot"] = ps
    except Exception:
        pass
    # --- /PB_SAVE_PLAN_SNAPSHOT_PATCH_V3 ---

    aj.write(patch)


def _side_from_pick(pick: str) -> SideT | None:
    if pick == "BULL":
        return "LONG"
    if pick == "BEAR":
        return "SHORT"
    return None


def _build_initial_positions(portfolio_plan: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build the initial positions dictionary from today's portfolio plan.

    We also seed ladder-related fields:
      - pending_entry_px: current trigger price (starts at planner entry, i.e. 5th-bar level)
      - ladder_ref_bar:   which bar's high/low this trigger corresponds to (starts at 5)
    """
    positions: Dict[str, Dict[str, Any]] = {}
    for plan in portfolio_plan.get("plans", []):
        symbol = plan["symbol"]
        pick = plan.get("pick", "ABSTAIN")
        side = _side_from_pick(pick)
        qty = int(plan.get("qty", 0) or 0)

        if side is None or qty <= 0:
            # ABSTAIN or zero-qty: no position to track
            continue

        # Be defensive with key names
        entry = _get_float(plan, ["entry", "Entry", "ENTRY"])
        stop = _get_float(plan, ["stop", "sl", "SL", "Stop", "SL_price"])
        t1 = _get_float(plan, ["t1", "T1", "target1", "Target1", "T1_price"], default=None)
        t2 = _get_float(plan, ["t2", "T2", "target2", "Target2", "T2_price"], default=None)
        exit_at = str(plan.get("exit_at") or plan.get("exit_rule") or "").strip() or "R2"

        if entry is None or stop is None:
            # Without entry/stop, we cannot sensibly simulate this symbol
            print(f"intraday_paper: skipping {symbol} – missing entry/stop in plan: keys={list(plan.keys())}")
            continue

        positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "status": "PENDING",
            "qty": qty,
            # This will eventually become the realized entry when we fill
            "entry_price": float(entry),
            "entry_time": None,
            "stop_price": float(stop),
            "t1_price": t1,
            "t2_price": t2,
            "exit_at": exit_at,
            "exit_price": None,
            "exit_time": None,
            "exit_reason": None,
            "realized_pnl_rs": 0.0,
            "open_pnl_rs": 0.0,
            # Laddered entry fields
            "pending_entry_px": float(entry),
            "ladder_ref_bar": 5,
        }

    return positions


def _get_ltp(symbol: str, state: Dict[str, Any]) -> float | None:
    # SIM: use last_closed close as the LTP stream (bar-based replay)
    if bool(state.get("sim")):
        lc = (state.get("last_closed") or {}).get(symbol) or {}
        v = lc.get("c")
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass

    q = (state.get("quotes") or {}).get(symbol) or {}
  
    # direct LTP keys
    for key in ("ltp", "last_price", "LTP", "LastPrice"):
        v = q.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            pass

    # fallback: ohlc close
    ohlc = q.get("ohlc")
    if isinstance(ohlc, dict):
        for key in ("c", "close", "C"):
            v = ohlc.get(key)
            if v is None:
                continue
            try:
                return float(v)
            except (TypeError, ValueError):
                pass

    # fallback: quote root close
    for key in ("c", "close", "C"):
        v = q.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            pass

    return None




def _get_ohlc(symbol: str, state: Dict[str, Any]) -> Dict[str, Any] | None:
    # SIM: trust last_closed bar OHLC (replay writes this correctly)
    if bool(state.get("sim")):
        lc = (state.get("last_closed") or {}).get(symbol) or {}
        if isinstance(lc, dict) and all(k in lc for k in ("o","h","l","c")):
            return lc
    q = (state.get("quotes") or {}).get(symbol) or {}

    # Preferred schema: quotes[sym]['ohlc'] = {o,h,l,c}
    ohlc = q.get("ohlc")
    if isinstance(ohlc, dict):
        return ohlc

    # Alternate schema: quotes[sym] directly has o/h/l/c
    if isinstance(q, dict):
        def pick(*ks):
            for k in ks:
                if k in q and q.get(k) is not None:
                    return q.get(k)
            return None
        o = pick("o","open","O")
        h = pick("h","high","H")
        l = pick("l","low","L")
        c = pick("c","close","C")
        if o is not None and h is not None and l is not None and c is not None:
            return {"o": o, "h": h, "l": l, "c": c}

    # Fallback: last_closed bar (from agg5)
    lc = (state.get("last_closed") or {}).get(symbol) or {}
    if isinstance(lc, dict) and all(k in lc for k in ("o","h","l","c")):
        return lc

    return None

def _update_position_pnl(pos: Dict[str, Any], ltp: float | None) -> None:
    # Only OPEN positions have live open P&L; everything else is 0.
    if pos.get("status") != "OPEN" or ltp is None:
        pos["open_pnl_rs"] = 0.0
        return

    side: SideT = pos["side"]
    entry = float(pos["entry_price"])
    qty = int(pos.get("qty") or 0)

    if qty <= 0:
        pos["open_pnl_rs"] = 0.0
        return

    if side == "LONG":
        pos["open_pnl_rs"] = (ltp - entry) * qty
    else:  # SHORT
        pos["open_pnl_rs"] = (entry - ltp) * qty


def _current_dt_from_state(now: datetime, state: Dict[str, Any]) -> datetime | None:
    """
    Single place where we decide "what time is it" for the engine:

      - In SIM: use sim_clock from state.
      - In LIVE: use wall-clock.
    """
    if bool(state.get("sim")):
        sim_clock = state.get("sim_clock")
        if not sim_clock:
            return None
        try:
            ts_str = str(sim_clock)
            # Strip timezone suffix like "+05:30" or "Z"
            if "T" in ts_str:
                ts_str = ts_str.split("+")[0].split("Z")[0]
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None
    return now


def _current_bar_index(current_dt: datetime | None) -> int | None:
    if current_dt is None:
        return None
    t = current_dt.time()
    start = dtime(hour=9, minute=15)
    minutes = (t.hour * 60 + t.minute) - (start.hour * 60 + start.minute)
    if minutes < 0:
        return None

    # ✅ Boundary fix: if time is exactly on a 5-min boundary (and not 09:15),
    # treat it as the CLOSE of the previous bar in SIM/replay.
    if minutes > 0 and (minutes % 5 == 0) and getattr(t, "second", 0) == 0:
        minutes -= 1

    return 1 + (minutes // 5)




def _update_ladder_trigger(symbol: str, pos: Dict[str, Any], state: Dict[str, Any], bar_index: int | None) -> None:
    """
    Maintain laddered pending entry price for bars 5..10.

    Spec:
      - Pick=BEAR  (SHORT): entry trigger is low of bar 5 initially, then
                            low of bar 6, then 7, 8, 9, 10 as each closes.
      - Pick=BULL  (LONG):  mirror with highs.

    Implementation:
      - pos["pending_entry_px"] holds current trigger.
      - pos["ladder_ref_bar"] records which bar that trigger came from.
      - We bump ladder_ref_bar by 1 each time bar_index advances, up to 10.
    """
    if pos.get("status") != "PENDING":
        return
    if bar_index is None or bar_index < 5:
        return

    side: SideT | None = pos.get("side")
    if side not in ("LONG", "SHORT"):
        return

    # Seed ladder fields if missing (e.g. if engine restarted mid-day)
    if pos.get("ladder_ref_bar") is None:
        pos["ladder_ref_bar"] = 5
    if pos.get("pending_entry_px") is None and pos.get("entry_price") is not None:
        pos["pending_entry_px"] = float(pos["entry_price"])

    last_ref = int(pos.get("ladder_ref_bar") or 0)

    # Once we are past bar 10, we stop laddering.
    if last_ref >= 10:
        return

    # If we've moved into a later bar within 5..10, shift the trigger
    # Update trigger only after the NEXT bar has closed:
    # ref=5 stays active during bar6, ref=6 becomes active during bar7, etc.
    if bar_index >= (last_ref + 2) and bar_index <= 11:

        lc = (state.get("last_closed") or {}).get(symbol) or {}
        if side == "SHORT":
            new_trigger = _get_float(lc, ["l"])
        else:  # LONG
            new_trigger = _get_float(lc, ["h"])
        if new_trigger is None:
            return


        if new_trigger is None:
            return

        new_trigger = float(new_trigger)
        pos["pending_entry_px"] = new_trigger
        pos["ladder_ref_bar"] = min(bar_index - 1, 10)

        # UI-sync disabled: do not mutate state['symbols']; UI should read positions.pending_entry_px


def _maybe_open_position(
    symbol: str,
    pos: Dict[str, Any],
    state: Dict[str, Any],
    can_open: bool,
    bar_index: int | None,
) -> None:
    """
    Entry logic with laddered trigger:

      - Uses pos["pending_entry_px"] as the current trigger.
      - For LONG: enter when bar high >= trigger.
      - For SHORT: enter when bar low  <= trigger.
    """
    if pos.get("status") != "PENDING":
        return
    if not can_open:
        return

    # Don’t allow fill in the SAME bar that defines the trigger.
    ref_bar = int(pos.get("ladder_ref_bar") or 5)
    if bar_index is None or bar_index <= ref_bar:
        return


    side: SideT = pos["side"]
    trigger = pos.get("pending_entry_px") or pos.get("entry_price")
    if trigger is None:
        return
    trigger = float(trigger)

    ohlc = _get_ohlc(symbol, state)
    if not ohlc:
        return

    bar_high = _get_float(ohlc, ["h"])
    bar_low = _get_float(ohlc, ["l"])

    filled = False
    if side == "LONG" and bar_high is not None and bar_high >= trigger:
        filled = True
    elif side == "SHORT" and bar_low is not None and bar_low <= trigger:
        filled = True

    if not filled:
        return

    # Use market time for entry (sim_clock in SIM, wall-clock in LIVE)
    now = get_now_ist(state)
    current_dt = _current_dt_from_state(now, state)
    bar_index = _current_bar_index(current_dt)

    pos["status"] = "OPEN"
    pos["entry_price"] = trigger
    pos["entry_time"] = (current_dt or now).isoformat()
    pos["entry_bar"] = bar_index  # 5..10 in our ORB ladder
    # open_pnl will be recomputed in _update_position_pnl()



def _maybe_close_position(symbol: str, pos: Dict[str, Any], state: Dict[str, Any], ltp: float) -> None:
    if pos.get("status") != "OPEN":
        return

    side: SideT = pos["side"]
    stop = float(pos["stop_price"])
    t1 = pos.get("t1_price")
    t2 = pos.get("t2_price")
    exit_at = str(pos.get("exit_at") or "R2").upper().strip()

    target = None
    if exit_at in ("R1", "T1"):
        target = float(t1) if t1 is not None else None
    else:
        target = float(t2) if t2 is not None else None

    # SIM: use last_closed HIGH/LOW to detect hits inside the bar
    bar_hi = bar_lo = None
    if bool(state.get("sim")):
        lc = (state.get("last_closed") or {}).get(symbol) or {}
        try:
            bar_hi = float(lc.get("h")) if lc.get("h") is not None else None
            bar_lo = float(lc.get("l")) if lc.get("l") is not None else None
        except Exception:
            bar_hi = bar_lo = None

    def exit_at_price(reason: str, px: float):
        entry = float(pos["entry_price"])
        qty = int(pos.get("qty") or 0)
        if qty <= 0:
            pnl = 0.0
        else:
            pnl = (px - entry) * qty if side == "LONG" else (entry - px) * qty
        pos["status"] = "CLOSED"
        pos["exit_price"] = px
        pos["exit_time"] = datetime.now().isoformat()
        pos["exit_reason"] = reason
        pos["realized_pnl_rs"] = pnl
        pos["open_pnl_rs"] = 0.0

    # Determine hits (use hi/lo in SIM; else fallback to ltp)
    if side == "LONG":
        if (bar_lo is not None and bar_lo <= stop) or (bar_lo is None and ltp <= stop):
            return exit_at_price("SL", stop)
        if target is not None and ((bar_hi is not None and bar_hi >= target) or (bar_hi is None and ltp >= target)):
            return exit_at_price("T1" if exit_at in ("R1","T1") else "T2", target)
    else:  # SHORT
        if (bar_hi is not None and bar_hi >= stop) or (bar_hi is None and ltp >= stop):
            return exit_at_price("SL", stop)
        if target is not None and ((bar_lo is not None and bar_lo <= target) or (bar_lo is None and ltp <= target)):
            return exit_at_price("T1" if exit_at in ("R1","T1") else "T2", target)



def _maybe_eod_close(pos: Dict[str, Any], ltp: float) -> None:
    if pos.get("status") != "OPEN":
        return
    # Force close at EOD
    entry = float(pos["entry_price"])
    qty = int(pos.get("qty") or 0)
    side: SideT = pos["side"]

    if qty <= 0:
        pnl = 0.0
    else:
        if side == "LONG":
            pnl = (ltp - entry) * qty
        else:
            pnl = (entry - ltp) * qty

    pos["status"] = "CLOSED"
    pos["exit_price"] = ltp
    pos["exit_time"] = datetime.now().isoformat()
    pos["exit_reason"] = "EOD"
    pos["realized_pnl_rs"] = pnl
    pos["open_pnl_rs"] = 0.0


def _maybe_mark_no_fill(pos: Dict[str, Any], bar_index: int | None) -> None:
    """
    If we are past the 10th bar and the trade never filled, mark as NOFILL.
    """
    if pos.get("status") != "PENDING":
        return
    if bar_index is None or bar_index <= 10:
        return

    pos["status"] = "CLOSED"
    pos["exit_price"] = pos.get("entry_price")
    pos["exit_time"] = datetime.now().isoformat()
    pos["exit_reason"] = "NOFILL"
    pos["realized_pnl_rs"] = 0.0
    pos["open_pnl_rs"] = 0.0


def _is_after_eod(now: datetime, state: Dict[str, Any]) -> bool:
    """
    Decide if we're past EOD cut-off.

    - In SIM (state["sim"] == True): use sim_clock from state.
    - In LIVE: use wall-clock time.

    Hard cut-off is 15:05 IST – all trades must be flat by then.
    """
    # SIM mode: trust sim_clock written by playback / agg5
    if bool(state.get("sim")):
        sim_clock = state.get("sim_clock")
        if not sim_clock:
            return False
        try:
            ts_str = str(sim_clock)
            # Strip timezone suffix like "+05:30" or "Z"
            if "T" in ts_str:
                ts_str = ts_str.split("+")[0].split("Z")[0]
            sim_dt = datetime.fromisoformat(ts_str)
        except Exception:
            return False
        return sim_dt.time() >= dtime(hour=15, minute=5)

    # LIVE mode: simple wall-clock check
    return now.time() >= dtime(hour=15, minute=5)


def run_intraday_paper_loop(poll_seconds: float = 2.0) -> None:
    """
    Main entry. Run this AFTER the 09:40 planner has written portfolio_plan
    into live_state.json. It will:
      - Initialise positions from today's plan
      - Every poll: read latest LTPs + OHLC from state.symbols
      - Maintain laddered entry triggers (bars 5–10)
      - Update positions (entries/exits)
      - Compute P&L and risk
      - Write positions/pnl/risk/batch_agent back into live_state.json
    """
    state = load_state()
    # --- PB_PLAN_SNAPSHOT_HYDRATE_V2 ---
    # Ensure plan_snapshot embeds the real locked portfolio_plan
    try:
        ps0 = state.get('plan_snapshot') or {}
        pp0 = state.get('portfolio_plan') or {}
        if isinstance(ps0, dict) and isinstance(pp0, dict):
            locked = bool(pp0.get('plan_locked')) or bool(ps0.get('locked'))
            ps = dict(ps0)
            ps['portfolio_plan'] = pp0  # force real plan
            ps['locked'] = locked
            ps['status'] = 'READY' if locked else (ps.get('status') or 'MISSING')
            if ps.get('day') is None:
                ps['day'] = state.get('plan_day') or state.get('date')
            state['plan_snapshot'] = ps
    except Exception:
        pass
    # --- /PB_PLAN_SNAPSHOT_HYDRATE_V2 ---

    # ---- EXECUTION GATE (single source of truth) ----
    # Do not simulate/execute anything until the 09:40 plan snapshot is:
    #   - status in {READY, READY_PARTIAL}
    #   - AND portfolio_plan.plan_locked == True
    #
    # This is the same gate Phase B OMS will use.
    portfolio_plan, gate = get_locked_portfolio_plan(state)

    if not gate.ok or not portfolio_plan.get("plans"):
        # Planner thread may have finished but failed to write a usable snapshot.
        # Poll briefly so a slow disk write doesn't falsely block the day.
        t0 = time.time()
        print(f"intraday_paper: waiting for executable plan snapshot... ({gate.reason})")
        while time.time() - t0 < 120.0:
            state = load_state()
            portfolio_plan, gate = get_locked_portfolio_plan(state)
            if gate.ok and portfolio_plan.get("plans"):
                break

            # Publish a clear heartbeat for UI/debug
            save_state({
                "batch_agent": {
                    "status": "WAITING_FOR_PLAN",
                    "phase": "PHASE_A",
                    "last_heartbeat_ts": datetime.now().isoformat(),
                    "details": f"execution blocked until plan_snapshot READY+locked ({gate.reason})",
                }
            })
            time.sleep(0.5)

        if not gate.ok or not portfolio_plan.get("plans"):
            save_state({
                "batch_agent": {
                    "status": "EXECUTION_BLOCKED",
                    "phase": "PHASE_A",
                    "last_heartbeat_ts": datetime.now().isoformat(),
                    "details": f"EXECUTION_BLOCKED: {gate.reason} (status={gate.status}, locked={gate.plan_locked})",
                }
            })
            print(f"intraday_paper: EXECUTION_BLOCKED – {gate.reason} (status={gate.status}, locked={gate.plan_locked})")
            return

    date = portfolio_plan.get("date") or state.get("date") or state.get("sim_day")
    positions = _build_initial_positions(portfolio_plan)
    daily_risk_rs = float(portfolio_plan.get("daily_risk_rs", 0.0) or 0.0)

    print(f"intraday_paper: starting for {date}, positions={len(positions)} (gate={gate.status}, locked={gate.plan_locked})")

    while True:
        state = load_state()  # pull latest quotes + sim_clock/etc
        now = get_now_ist(state)

        # Effective clock (SIM vs LIVE) and current bar index
        current_dt = _current_dt_from_state(now, state)
        bar_index = _current_bar_index(current_dt)

        # Manual kill flag could be wired from state later
        manual_kill = bool((state or {}).get('kill_switch', False) or (state or {}).get('manual_kill', False) or (state or {}).get('manual_stop', False))

        # If the plan snapshot is not executable anymore, block any NEW entries.
        # (Should never happen on a clean day, but protects us from miswiring.)
        g2 = evaluate_plan_gate(state, day=str(date) if date else None)
        if not g2.ok:
            manual_kill = bool((state or {}).get('kill_switch', False) or (state or {}).get('manual_kill', False) or (state or {}).get('manual_stop', False))

        # First pass: ladder + mark-to-market P&L
        for sym, pos in positions.items():
            # Maintain laddered trigger for PENDING orders between bars 5 and 10
            _update_ladder_trigger(sym, pos, state, bar_index)

            # Update open P&L for any OPEN positions
            ltp = _get_ltp(sym, state)
            _update_position_pnl(pos, ltp)

        # Compute risk and decide if new entries allowed
        risk_state = compute_risk_state(positions, daily_risk_rs, manual_kill)
        can_open = risk_state["can_open_new_trades"]

        # Apply entries/exits / NOFILL / EOD
        after_eod = _is_after_eod(now, state)
        for sym, pos in positions.items():
            ltp = _get_ltp(sym, state)
            if ltp is None:
                continue

            if not after_eod:
                _maybe_open_position(sym, pos, state, can_open, bar_index)
                _maybe_close_position(sym, pos, state, ltp)
                _maybe_mark_no_fill(pos, bar_index)
            else:
                # Force close open positions at EOD
                _maybe_eod_close(pos, ltp)

        # Recompute risk and P&L after state changes
        risk_state = compute_risk_state(positions, daily_risk_rs, manual_kill)

        # PATCH-ONLY write (never write full state back; avoid clobbering other writers)
        patch_out = {
            "positions": positions,
            "pnl": {
                "realized_rs": risk_state["realized_rs"],
                "open_rs": risk_state["open_rs"],
                "day_total_rs": risk_state["day_pnl_rs"],
                "realized": risk_state["realized_rs"],
                "open": risk_state["open_rs"],
                "day": risk_state["day_pnl_rs"],
            },
            "risk": risk_state,
            "batch_agent": {
                "status": "RUNNING" if not after_eod else "EOD_STOP",
                "phase": "PHASE_A",
                "last_heartbeat_ts": now.isoformat(),
                "details": "intraday_paper+risk active",
            },
        }
        if date is not None:
            patch_out["date"] = date
        patch_out["daily_risk_rs"] = daily_risk_rs

        save_state(patch_out)

        if after_eod:
            print("intraday_paper: EOD reached; stopping loop.")
            break

        time.sleep(poll_seconds)
