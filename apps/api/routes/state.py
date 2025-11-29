# apps/api/routes/state.py

from __future__ import annotations

import math
from datetime import datetime, date
from math import floor
from typing import Dict, Any, List, Optional
from fastapi import Query

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger
from probedge.storage.atomic_json import AtomicJSON
from probedge.decision.plan_core import build_parity_plan

log = get_logger(__name__)
router = APIRouter()

# live_state.json path + helper
STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)


# -------------------------------
# 0) Small helpers
# -------------------------------

def _today_str() -> str:
    """Return today's date as YYYY-MM-DD (system local time; on your Mac this is IST)."""
    return datetime.now().date().isoformat()


def _write_portfolio_plan_to_state(portfolio_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Persist a portfolio-level plan into live_state.json under key 'portfolio_plan'.
    Returns the same object back to the caller.
    """
    state = aj.read(default={})
    state["portfolio_plan"] = portfolio_state
    aj.write(state)
    return portfolio_state


# -------------------------------
# 1) Daily risk & parity helpers
# -------------------------------

def _effective_daily_risk_rs() -> int:
    """
    Decide which daily risk number to use.

    - MODE = 'test' → use RISK_RS_TEST (usually 1,000).
    - Otherwise → use risk_budget_rs (derived from RISK_RS_DEFAULT, usually 10,000).
    """
    mode = (SETTINGS.mode or "").lower()
    if mode == "test":
        return int(getattr(SETTINGS, "risk_rs_test", 1000))

    # Preferred config hook
    if getattr(SETTINGS, "risk_budget_rs", None):
        return int(SETTINGS.risk_budget_rs)

    # Fallback
    return int(getattr(SETTINGS, "risk_rs_default", 10000))


def _is_active_plan(p: Dict[str, Any]) -> bool:
    """
    Decide if a plan is tradable for portfolio purposes.
    """
    if not isinstance(p, dict):
        return False

    pick = p.get("pick")
    if pick not in ("BULL", "BEAR"):
        return False

    entry = p.get("entry")
    stop = p.get("stop")

    if entry is None or stop is None:
        return False

    try:
        e = float(entry)
        s = float(stop)
    except (TypeError, ValueError):
        return False

    if not (math.isfinite(e) and math.isfinite(s)):
        return False

    if abs(e - s) <= 0:
        return False

    return True


def _build_raw_plans_for_day(day_str: Optional[str]) -> List[Dict[str, Any]]:
    """
    Build single-symbol parity plans for all portfolio symbols for a given day.

    day_str may be:
    - 'YYYY-MM-DD' → explicit day
    - None         → builder uses latest available day per symbol
    """
    symbols = SETTINGS.symbols
    raw: List[Dict[str, Any]] = []

    for sym in symbols:
        try:
            plan = build_parity_plan(sym, day_str)
        except HTTPException as exc:
            # If tm5 / master missing, mark as ABSTAIN and continue
            log.warning("build_parity_plan failed for %s: %s", sym, exc)
            raw.append(
                {
                    "symbol": sym,
                    "pick": "ABSTAIN",
                    "reason": f"PLAN_ERROR: {exc.detail}",
                }
            )
            continue

        raw.append(plan)

    return raw


def _apply_portfolio_split(
    raw_plans: List[Dict[str, Any]],
    daily_risk_rs: int,
) -> Dict[str, Any]:
    """
    Take raw single-symbol plans and apply equal risk-splitting across active trades.

    - Only BULL/BEAR with valid entry/stop are considered active.
    - Risk per active trade = floor(daily_risk_rs / active_count).
    - Qty = floor(risk_per_trade / |entry - stop|).
    """
    # Decide which date to use for the state payload (best-effort from any plan)
    day: Optional[str] = None
    for p in raw_plans:
        val = p.get("date")
        if val:
            day = str(val).split("T")[0]
            break

    active_indices = [i for i, p in enumerate(raw_plans) if _is_active_plan(p)]
    active_count = len(active_indices)

    if active_count == 0 or daily_risk_rs <= 0:
        # No trades: pass-through
        return {
            "date": day,
            "mode": SETTINGS.mode,
            "daily_risk_rs": daily_risk_rs,
            "active_trades": 0,
            "risk_per_trade_rs": 0,
            "plans": raw_plans,
        }

    risk_per_trade_rs = int(daily_risk_rs // active_count)

    adjusted: List[Dict[str, Any]] = []
    total_planned = 0.0

    for idx, p in enumerate(raw_plans):
        q = dict(p)  # shallow copy

        if idx not in active_indices:
            # Non-active: make sure qty and per-trade risk are zeroed for clarity
            q["qty"] = int(q.get("qty") or 0)
            q["per_trade_risk_rs_used"] = 0
            q["parity_mode"] = False
            adjusted.append(q)
            continue

        entry = float(p.get("entry"))
        stop = float(p.get("stop"))
        risk_per_share = abs(entry - stop)

        if not math.isfinite(risk_per_share) or risk_per_share <= 0:
            q["qty"] = 0
            q["per_trade_risk_rs_used"] = 0
            q["parity_mode"] = False
            adjusted.append(q)
            continue

        qty = int(risk_per_trade_rs // risk_per_share)

        if qty <= 0:
            q["qty"] = 0
            q["per_trade_risk_rs_used"] = 0
            q["parity_mode"] = False
            adjusted.append(q)
            continue

        per_trade_risk = qty * risk_per_share
        total_planned += per_trade_risk

        q["risk_per_share"] = risk_per_share
        q["qty"] = qty
        q["per_trade_risk_rs_used"] = per_trade_risk
        q["parity_mode"] = True

        adjusted.append(q)

    return {
        "date": day,
        "mode": SETTINGS.mode,
        "daily_risk_rs": daily_risk_rs,
        "active_trades": active_count,
        "risk_per_trade_rs": risk_per_trade_rs,
        "total_planned_risk_rs": total_planned,
        "plans": adjusted,
    }


# -------------------------------
# 2) Existing parity endpoint
# -------------------------------

@router.get("/api/state")
def api_state(
    day: Optional[date] = Query(None),
    risk: Optional[int] = Query(None, description="Override daily risk budget in rupees"),
) -> Dict[str, Any]:
    """
    State for parity plan for a given day.

    If `risk` is provided, we override the daily risk budget and
    recompute qty + per_trade_risk purely from entry/stop.
    """
    # Resolve day
    day = day or date.today()
    day_str = day.isoformat()

    # 1) Build raw plans
    raw_plans = _build_raw_plans_for_day(day_str)

    # 2) Decide daily risk
    if risk is not None:
        daily_risk_rs = int(risk)
    else:
        daily_risk_rs = _effective_daily_risk_rs()

    # 3) Apply portfolio split
    portfolio_state = _apply_portfolio_split(raw_plans, daily_risk_rs)

    # Force portfolio date to requested day
    portfolio_state["date"] = day_str

    return portfolio_state


# -------------------------------
# 3) Live-state + ARM control
# -------------------------------

class ArmRequest(BaseModel):
    symbol: str
    strategy: str = "batch_v1"

@router.get("/api/live_state")
def api_live_state(
    day: Optional[date] = Query(
        None,
        description="Day to use for parity plan; defaults to today or sim_day",
    ),
    risk: Optional[int] = Query(
        None,
        description="Override daily risk budget for parity plan",
    ),
) -> Dict[str, Any]:
    """
    Merge fake-live quotes from live_state.json with the parity portfolio plan.

    Shape is tailored for the live grid UI:

    {
      "meta": { "sim_day": ..., "sim_clock": ..., "mode": ... },
      "symbols": {
        "SBIN": {
          "ltp": ...,
          "ohlc": {"o": ..., "h": ..., "l": ..., "c": ...},
          "volume": ...,
          "tags": { "OpeningTrend": ..., "OpenLocation": ..., "PrevDayContext": ... },
          "plan": { "pick": ..., "confidence": ... }
        },
        ...
      }
    }
    """
    # 1) Read live_state.json (quotes from playback or live agg)
    live_state: Dict[str, Any] = aj.read(default={}) or {}
    symbols_quotes: Dict[str, Any] = live_state.get("symbols") or {}

    sim_day_str = live_state.get("sim_day")
    sim_clock = live_state.get("sim_clock")
    mode = live_state.get("mode", SETTINGS.mode)

    # 2) Decide which day to use for the plan
    if day is not None:
        plan_day = day
    elif sim_day_str:
        # sim_day is plain YYYY-MM-DD
        plan_day = date.fromisoformat(sim_day_str)
    else:
        plan_day = date.today()

    plan_day_str = plan_day.isoformat()

    # 3) Build parity plan for that day (same as /api/state)
    raw_plans = _build_raw_plans_for_day(plan_day_str)
    if risk is not None:
        daily_risk_rs = int(risk)
    else:
        daily_risk_rs = _effective_daily_risk_rs()

    portfolio_state = _apply_portfolio_split(raw_plans, daily_risk_rs)

    plans: List[Dict[str, Any]] = portfolio_state.get("plans") or []
    plans_by_sym: Dict[str, Dict[str, Any]] = {
        p.get("symbol"): p for p in plans if isinstance(p, dict)
    }

    # 4) Build symbol-level view for UI
    result_symbols: Dict[str, Any] = {}

    # Always iterate over configured universe so you see all 10 rows
    for sym in SETTINGS.symbols:
        quote = symbols_quotes.get(sym, {}) or {}
        plan = plans_by_sym.get(sym, {}) or {}

        # tags may live under plan["tags"] as a dict
        tags = plan.get("tags") or {}
        if not isinstance(tags, dict):
            tags = {}

        # Fallback: if tags were flattened in plan, pick them up
        for key in ("OpeningTrend", "OpenLocation", "PrevDayContext"):
            if key not in tags and key in plan:
                tags[key] = plan.get(key)

        # Plan mini-view for UI
        confidence = None
        if "confidence%" in plan:
            confidence = plan.get("confidence%")
        elif "confidence" in plan:
            confidence = plan.get("confidence")

        result_symbols[sym] = {
            "ltp": quote.get("ltp"),
            "ohlc": quote.get("ohlc") or {},
            "volume": quote.get("volume"),
            "tags": tags,
            "plan": {
                "pick": plan.get("pick"),
                "confidence": confidence,
            },
        }

    meta = {
        "mode": mode,
        "sim_day": sim_day_str or plan_day_str,
        "sim_clock": sim_clock,
    }

    return {
        "meta": meta,
        "symbols": result_symbols,
    }


@router.get("/api/live_state")
def api_live_state(
    day: Optional[date] = Query(
        None,
        description="Day to use for parity plan; defaults to today or sim_day",
    ),
    risk: Optional[int] = Query(
        None,
        description="Override daily risk budget for parity plan",
    ),
) -> Dict[str, Any]:
    """
    Merge fake-live quotes from live_state.json with the parity portfolio plan.

    Returns:
    {
      "meta": { "sim_day": ..., "sim_clock": ..., "mode": ... },
      "symbols": {
        "SBIN": {
          "ltp": ...,
          "ohlc": {"o": ..., "h": ..., "l": ..., "c": ...},
          "volume": ...,
          "tags": { "OpeningTrend": ..., "OpenLocation": ..., "PrevDayContext": ... },
          "plan": { "pick": ..., "confidence": ... }
        },
        ...
      }
    }
    """
    # 1) Read live_state.json (quotes)
    live_state: Dict[str, Any] = aj.read(default={}) or {}
    quotes_by_sym: Dict[str, Any] = live_state.get("symbols") or {}

    sim_day_str = live_state.get("sim_day")
    sim_clock = live_state.get("sim_clock")
    mode = live_state.get("mode", SETTINGS.mode)

    # 2) Decide which day to use for the plan
    if day is not None:
        plan_day = day
    elif sim_day_str:
        plan_day = date.fromisoformat(sim_day_str)
    else:
        plan_day = date.today()

    plan_day_str = plan_day.isoformat()

    # 3) Build parity plan (same logic as /api/state)
    raw_plans = _build_raw_plans_for_day(plan_day_str)

    if risk is not None:
        daily_risk_rs = int(risk)
    else:
        daily_risk_rs = _effective_daily_risk_rs()

    portfolio_state = _apply_portfolio_split(raw_plans, daily_risk_rs)
    plans: List[Dict[str, Any]] = portfolio_state.get("plans") or []

    plans_by_sym: Dict[str, Dict[str, Any]] = {
        p.get("symbol"): p for p in plans if isinstance(p, dict)
    }

    # 4) Build per-symbol view for UI
    result_symbols: Dict[str, Any] = {}

    for sym in SETTINGS.symbols:
        quote = quotes_by_sym.get(sym, {}) or {}
        plan = plans_by_sym.get(sym, {}) or {}

        tags = plan.get("tags") or {}
        if not isinstance(tags, dict):
            tags = {}

        # Fallback if tags were flattened into the plan
        for key in ("OpeningTrend", "OpenLocation", "PrevDayContext"):
            if key not in tags and key in plan:
                tags[key] = plan.get(key)

        confidence = None
        if "confidence%" in plan:
            confidence = plan.get("confidence%")
        elif "confidence" in plan:
            confidence = plan.get("confidence")

        result_symbols[sym] = {
            "ltp": quote.get("ltp"),
            "ohlc": quote.get("ohlc") or {},
            "volume": quote.get("volume"),
            "tags": tags,
            "plan": {
                "pick": plan.get("pick"),
                "confidence": confidence,
            },
        }

    meta = {
        "mode": mode,
        "sim_day": sim_day_str or plan_day_str,
        "sim_clock": sim_clock,
    }

    return {
        "meta": meta,
        "symbols": result_symbols,
    }


@router.get("/api/state_raw")
def api_state_raw():
    """
    Raw live_state.json dump (whatever batch_agent + other components wrote).
    Used for debugging and UI introspection.
    """
    state = aj.read(default={})
    return state or {}


@router.post("/api/control/arm")
def api_control_arm(req: ArmRequest):
    """
    Ask batch_agent to compute a plan for TODAY for a symbol.

    This just writes control fields into live_state.json:
      - control.action  = 'arm'
      - control.symbol  = SYMBOL (upper-case)
      - control.strategy = strategy (lower-case, default 'batch_v1')

    The batch_agent loop will pick this up and write a 'plan' block back
    into live_state.json.
    """
    state = aj.read(default={})
    ctrl = state.get("control") or {}

    ctrl["action"] = "arm"
    ctrl["symbol"] = req.symbol.strip().upper()
    ctrl["strategy"] = req.strategy.strip().lower()

    state["control"] = ctrl
    aj.write(state)

    return {
        "status": "ok",
        "control": ctrl,
    }


# -------------------------------
# 4) Portfolio ARM for a day
# -------------------------------

@router.post("/api/plan/arm_day")
def api_plan_arm_day(
    day: Optional[str] = Query(
        None,
        description="YYYY-MM-DD; if omitted, uses today's date (system local)",
    ),
    risk: Optional[int] = Query(
        None,
        description="Override daily risk budget in rupees for this ARM operation",
    ),
):
    """
    Build the full 10-symbol parity plan for a specific trading day and
    persist it into live_state.json under 'portfolio_plan'.

    - Uses the same logic as GET /api/state.
    - If 'day' is omitted, uses today's date.
    - If 'risk' is provided, it overrides the default daily risk.
    """
    if day is None:
        day = _today_str()

    # Build raw plans (tags + pick + entry/stop/targets) for each symbol
    raw_plans = _build_raw_plans_for_day(day)

    # Decide daily risk: override if query param provided, else use settings
    if risk is not None:
        daily_risk_rs = int(risk)
    else:
        daily_risk_rs = _effective_daily_risk_rs()

    # Apply portfolio parity split
    portfolio_state = _apply_portfolio_split(raw_plans, daily_risk_rs)

    # Force portfolio date to match requested day
    portfolio_state["date"] = day

    # Persist into live_state.json under 'portfolio_plan'
    return _write_portfolio_plan_to_state(portfolio_state)

