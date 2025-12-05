# apps/runtime/intraday_paper.py

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, Any, Literal
from datetime import datetime, time as dtime

from probedge.risk.engine import compute_risk_state

STATE_PATH = Path("data/state/live_state.json")


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
        if k in d and d[k] is not None:
            try:
                return float(d[k])
            except (TypeError, ValueError):
                continue
    return default


def load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    with STATE_PATH.open() as f:
        return json.load(f)


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(state, f, indent=2, sort_keys=True, default=str)
    tmp.replace(STATE_PATH)


def _side_from_pick(pick: str) -> SideT | None:
    if pick == "BULL":
        return "LONG"
    if pick == "BEAR":
        return "SHORT"
    return None


def _build_initial_positions(portfolio_plan: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
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

        if entry is None or stop is None:
            # Without entry/stop, we cannot sensibly simulate this symbol
            print(f"intraday_paper: skipping {symbol} – missing entry/stop in plan: keys={list(plan.keys())}")
            continue

        positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "status": "PENDING",
            "qty": qty,
            "entry_price": entry,
            "entry_time": None,
            "stop_price": stop,
            "t1_price": t1,
            "t2_price": t2,
            "exit_price": None,
            "exit_time": None,
            "exit_reason": None,
            "realized_pnl_rs": 0.0,
            "open_pnl_rs": 0.0,
        }

    return positions



def _get_ltp(symbol: str, state: Dict[str, Any]) -> float | None:
    """
    Try to read last traded price from state["symbols"][symbol].
    Adjust key names if your symbols section is different.
    """
    symbols = state.get("symbols") or {}
    info = symbols.get(symbol) or {}
    # Common patterns we might have used:
    for key in ("ltp", "LTP", "last_price", "close"):
        if key in info and info[key] is not None:
            try:
                return float(info[key])
            except (TypeError, ValueError):
                continue
    return None


def _update_position_pnl(pos: Dict[str, Any], ltp: float) -> None:
    # Only OPEN positions have live open P&L; everything else is 0.
    if pos.get("status") != "OPEN":
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


def _maybe_open_position(pos: Dict[str, Any], ltp: float, can_open: bool) -> None:
    if pos["status"] != "PENDING":
        return
    if not can_open:
        return

    side: SideT = pos["side"]
    entry = float(pos["entry_price"])

    if side == "LONG" and ltp >= entry:
        pos["status"] = "OPEN"
        pos["entry_time"] = datetime.now().isoformat()
    elif side == "SHORT" and ltp <= entry:
        pos["status"] = "OPEN"
        pos["entry_time"] = datetime.now().isoformat()
    # else: still pending


def _maybe_close_position(pos: Dict[str, Any], ltp: float) -> None:
    if pos["status"] != "OPEN":
        return

    side: SideT = pos["side"]
    stop = float(pos["stop_price"])
    t1 = pos.get("t1_price")
    t2 = pos.get("t2_price")

    exit_reason = None

    if side == "LONG":
        if ltp <= stop:
            exit_reason = "SL"
        elif t2 is not None and ltp >= float(t2):
            exit_reason = "T2"
        elif t1 is not None and ltp >= float(t1):
            exit_reason = "T1"
    else:  # SHORT
        if ltp >= stop:
            exit_reason = "SL"
        elif t2 is not None and ltp <= float(t2):
            exit_reason = "T2"
        elif t1 is not None and ltp <= float(t1):
            exit_reason = "T1"

    if exit_reason is None:
        return

    entry = float(pos["entry_price"])
    qty = int(pos["qty"] or 0)

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
    pos["exit_reason"] = exit_reason
    pos["realized_pnl_rs"] = pnl
    pos["open_pnl_rs"] = 0.0  # important: no open P&L after close


def _maybe_eod_close(pos: Dict[str, Any], ltp: float) -> None:
    if pos["status"] != "OPEN":
        return
    # Force close at EOD
    entry = float(pos["entry_price"])
    qty = int(pos["qty"] or 0)
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


def _is_after_eod(now: datetime) -> bool:
    # 15:10 IST hard-coded. Adjust if needed.
    return now.time() >= dtime(hour=15, minute=10)


def run_intraday_paper_loop(poll_seconds: float = 2.0) -> None:
    """
    Main entry. Run this AFTER the 09:40 planner has written portfolio_plan
    into live_state.json. It will:
      - Initialise positions from today's plan
      - Every poll: read latest LTPs from state.symbols
      - Update positions (entries/exits)
      - Compute P&L and risk
      - Write positions/pnl/risk/batch_agent back into live_state.json
    """
    state = load_state()
    portfolio_plan = state.get("portfolio_plan") or {}
    date = portfolio_plan.get("date") or state.get("date")

    if not portfolio_plan or not portfolio_plan.get("plans"):
        print("intraday_paper: no portfolio_plan present; nothing to do.")
        return

    positions = _build_initial_positions(portfolio_plan)
    daily_risk_rs = float(portfolio_plan.get("daily_risk_rs", 0.0) or 0.0)

    print(f"intraday_paper: starting for {date}, positions={len(positions)}")

    while True:
        now = datetime.now()
        state = load_state()  # pull latest quotes

        # Manual kill flag could be wired from state later
        manual_kill = bool(state.get("kill_switch", False))

        # For each position, update using latest LTP
        for sym, pos in positions.items():
            ltp = _get_ltp(sym, state)
            if ltp is None:
                continue

            # Entry logic (if still pending)
            # We will compute risk after all PnL updates
            _update_position_pnl(pos, ltp)

        # Compute risk and decide if new entries allowed
        risk_state = compute_risk_state(positions, daily_risk_rs, manual_kill)
        can_open = risk_state["can_open_new_trades"]

        # Now re-loop to apply entries+exits with can_open in hand
        for sym, pos in positions.items():
            ltp = _get_ltp(sym, state)
            if ltp is None:
                continue

            if not _is_after_eod(now):
                _maybe_open_position(pos, ltp, can_open)
                _maybe_close_position(pos, ltp)
            else:
                # Force close open positions at EOD
                _maybe_eod_close(pos, ltp)

        # Recompute risk and P&L after state changes
        risk_state = compute_risk_state(positions, daily_risk_rs, manual_kill)

        # Attach positions/pnl/risk/batch_agent to full state and write
        state["positions"] = positions
        state["pnl"] = {
            "realized_rs": risk_state["realized_rs"],
            "open_rs": risk_state["open_rs"],
            "day_total_rs": risk_state["day_pnl_rs"],
        }
        state["risk"] = risk_state
        state["batch_agent"] = {
            "status": "RUNNING",
            "phase": "PHASE_A",
            "last_heartbeat_ts": now.isoformat(),
            "details": "intraday_paper+risk active",
        }

        # Ensure top-level date / daily_risk_rs are present for UI and tools
        if "date" not in state or state.get("date") is None:
            state["date"] = date
        if "daily_risk_rs" not in state or state.get("daily_risk_rs") is None:
            state["daily_risk_rs"] = daily_risk_rs

        save_state(state)

        if _is_after_eod(now):
            print("intraday_paper: EOD reached; stopping loop.")
            break

        time.sleep(poll_seconds)
