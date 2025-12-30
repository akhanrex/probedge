# apps/api/routes/state.py

from __future__ import annotations

import math
from datetime import datetime, date, time as dtime
from probedge.infra.clock_source import get_now_ist
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

# Plan is only published after this cutover (IST wall-clock).
T_PLAN_READY = dtime(9, 40, 1)

# live_state.json path + helper
STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)


# -------------------------------
# 0) Small helpers
# -------------------------------

def _today_str() -> str:
    """Return today's date as YYYY-MM-DD (system local time; on your Mac this is IST)."""
    return get_now_ist().date().isoformat()


def _write_portfolio_plan_to_state(portfolio_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Persist a portfolio-level plan into live_state.json under key 'portfolio_plan'.

    Also mirrors some key fields at the top level for easier inspection:
      - plan_day
      - daily_risk_rs
    """
    state = aj.read(default={}) or {}
    state["portfolio_plan"] = portfolio_state

    # Convenience mirrors
    state["plan_day"] = portfolio_state.get("date")
    state["daily_risk_rs"] = portfolio_state.get("daily_risk_rs")

    aj.write(state)
    return portfolio_state



# -------------------------------
# 1) Daily risk & parity helpers
# -------------------------------

def _effective_daily_risk_rs() -> int:
    """
    Decide which daily risk number to use.

    Priority:
    1) risk_override_rs from live_state.json (set via /api/risk), if present
    2) SETTINGS.risk_budget_rs
    3) fallback constants (risk_rs_test / RISK_RS_DEFAULT)
    """
    # 1) State-level override
    try:
        state = aj.read(default={}) or {}
    except Exception:
        state = {}

    override = state.get("risk_override_rs") or state.get("daily_risk_rs")
    if override:
        try:
            return int(override)
        except Exception:
            pass

    # 2) Mode-based + SETTINGS
    mode = (SETTINGS.mode or "").lower()
    if mode == "test":
        return int(getattr(SETTINGS, "risk_rs_test", 1000))

    if getattr(SETTINGS, "risk_budget_rs", None):
        return int(SETTINGS.risk_budget_rs)

    # 3) Hard fallback
    return 10000



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

    - Only BULL/BEAR with valid entry/stop are considered active (via _is_active_plan).
    - Risk per active trade = floor(daily_risk_rs / active_count).
    - Qty = floor(risk_per_trade / |entry - stop|).

    IMPORTANT:
    We preserve all informational fields from the raw plans:
      - tags (OpeningTrend / OpenLocation / PrevDayContext)
      - confidence%
      - reason, target1, target2, etc.

    So /api/live_state can display full tags + confidence for transparency.
    """

    # Decide which date to use for the state payload (best-effort from any plan)
    day: Optional[str] = None
    for p in raw_plans:
        val = p.get("date")
        if val:
            day = val
            break

    # Identify active (tradable) plans
    active_indices = [i for i, p in enumerate(raw_plans) if _is_active_plan(p)]
    active_count = len(active_indices)

    if daily_risk_rs is None:
        daily_risk_rs = 0

    if active_count <= 0 or daily_risk_rs <= 0:
        # No active trades → pass plans through with meta only
        return {
            "date": day,
            "mode": SETTINGS.mode,
            "daily_risk_rs": int(daily_risk_rs),
            "active_trades": 0,
            "risk_per_trade_rs": 0,
            "total_planned_risk_rs": 0,
            "plans": list(raw_plans),
        }

    # Equal split across active trades
    risk_per_trade_rs: int = int(math.floor(daily_risk_rs / active_count))

    adjusted: List[Dict[str, Any]] = []
    total_planned: float = 0.0

    for idx, p in enumerate(raw_plans):
        # Always start from a full copy so we don't lose tags / confidence% / targets, etc.
        q = dict(p)

        if idx not in active_indices:
            # ABSTAIN or otherwise inactive → ensure qty is present but zero
            q.setdefault("qty", 0)
            adjusted.append(q)
            continue

        entry = p.get("entry")
        stop = p.get("stop")

        # Safety: if for some reason entry/stop are missing/invalid, treat as inactive.
        if entry is None or stop is None:
            q.setdefault("qty", 0)
            adjusted.append(q)
            continue

        try:
            entry_f = float(entry)
            stop_f = float(stop)
        except Exception:
            q.setdefault("qty", 0)
            adjusted.append(q)
            continue

        risk_per_share = abs(entry_f - stop_f)
        if risk_per_share <= 0:
            q.setdefault("qty", 0)
            adjusted.append(q)
            continue

        qty = int(math.floor(risk_per_trade_rs / risk_per_share))
        if qty < 0:
            qty = 0

        per_trade_risk = qty * risk_per_share
        total_planned += per_trade_risk

        q["qty"] = qty
        q["per_trade_risk_rs_used"] = per_trade_risk
        q["parity_mode"] = True

        adjusted.append(q)

    return {
        "date": day,
        "mode": SETTINGS.mode,
        "daily_risk_rs": int(daily_risk_rs),
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
    """    # Read state once (used for stored plan + time gating).
    try:
        live_state: Dict[str, Any] = aj.read(default={}) or {}
    except Exception:
        live_state = {}

    now_ist = get_now_ist(live_state)

    # Resolve day (SIM-safe): prefer persisted plan day if caller didn't specify.
    if day is None:
        ps = live_state.get("plan_snapshot") or {}
        day_str = (
            ps.get("day")
            or live_state.get("plan_day")
            or live_state.get("date")
            or now_ist.date().isoformat()
        )
    else:
        day_str = day.isoformat()

    # If no explicit risk override and we already have a stored portfolio_plan
    # for this day (written by Phase A / arm_day), return that as single source of truth.
    if risk is None:
        portfolio_from_state = live_state.get("portfolio_plan")
        if isinstance(portfolio_from_state, dict):
            if portfolio_from_state.get("date") == day_str:
                out = dict(portfolio_from_state)

                # Make UI gating robust even when returning stored plan
                out.setdefault("plan_locked", True)
                out.setdefault("plan_status", "READY")
                out.setdefault("plan_source", "stored")

                return out



    # Before 09:40:01, we do NOT publish tradable plans for *today* unless they
    # were explicitly armed and persisted by Phase A.
    if day_str == now_ist.date().isoformat() and now_ist.time() < T_PLAN_READY:
        syms = list(SETTINGS.symbols or [])
        return {
            "date": day_str,
            "mode": SETTINGS.mode,
            "daily_risk_rs": int(_effective_daily_risk_rs() if risk is None else risk),
            "active_trades": 0,
            "risk_per_trade_rs": 0,
            "total_planned_risk_rs": 0,
            "plan_status": "NOT_READY",
            "plan_source": "gated",
            "plans": [
                {
                    "symbol": s,
                    "pick": "PENDING",
                    "confidence%": 0,
                    "skip": "plan_not_armed_yet",
                }
                for s in syms
            ],
        }
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

    # UI stability fields (computed plan)
    portfolio_state.setdefault("plan_status", "READY")
    portfolio_state.setdefault("plan_locked", False)
    portfolio_state.setdefault("plan_source", "computed")

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
    Merge live quotes from live_state.json with the parity portfolio plan.

    Key rules:
    - Quotes come from live_state.json["quotes"] (written by agg5).
    - Tags come from:
        1) plan["tags"] if present (post-09:40 plan),
        2) else live_state.json["tags"][sym] (pre-09:40 tags-only stage).
    - Before 09:40:01 for *today*, DO NOT build/publish tradable plans here.
      Only show tags + PENDING plan view (unless portfolio_plan already exists).
    """
    # 1) Read live_state.json (quotes + tags)
    live_state: Dict[str, Any] = aj.read(default={}) or {}
    quotes_by_sym: Dict[str, Any] = live_state.get("quotes") or {}
    tags_by_sym: Dict[str, Any] = live_state.get("tags") or {}

    sim_day_str = live_state.get("sim_day")
    sim_clock = live_state.get("sim_clock")
    mode = live_state.get("mode", SETTINGS.mode)

    now_ist = get_now_ist(live_state)

    # 2) Decide which day to use for the plan
    if day is not None:
        plan_day = day
    elif sim_day_str:
        plan_day = date.fromisoformat(sim_day_str)
    else:
        plan_day = now_ist.date()

    plan_day_str = plan_day.isoformat()

    # 3) Decide daily risk
    daily_risk_rs = int(risk) if risk is not None else int(_effective_daily_risk_rs())

    # 4) Decide portfolio_state with strict 09:40 gating
    portfolio_state: Dict[str, Any] | None = None
    plan_source = "computed"

    # 4.1) If portfolio_plan already persisted for this day, use it (single source of truth)
    if risk is None:
        ps = live_state.get("portfolio_plan")
        if isinstance(ps, dict) and ps.get("date") == plan_day_str:
            portfolio_state = dict(ps)
            portfolio_state.setdefault("plan_locked", True)
            portfolio_state.setdefault("plan_status", "READY")
            plan_source = "stored"

    # 4.2) If still None, enforce gating for *today* before 09:40:01
    if portfolio_state is None:
        if plan_day_str == now_ist.date().isoformat() and now_ist.time() < T_PLAN_READY:
            plan_source = "gated"
            syms = list(SETTINGS.symbols or [])
            portfolio_state = {
                "date": plan_day_str,
                "mode": SETTINGS.mode,
                "daily_risk_rs": daily_risk_rs,
                "active_trades": 0,
                "risk_per_trade_rs": 0,
                "total_planned_risk_rs": 0,
                "plan_status": "NOT_READY",
                "plans": [
                    {
                        "symbol": s,
                        "pick": "PENDING",
                        "confidence%": 0,
                        "skip": "plan_not_armed_yet",
                    }
                    for s in syms
                ],
            }
        else:
            # Post-gate (or past day): compute plan normally
            plan_source = "computed"
            raw_plans = _build_raw_plans_for_day(plan_day_str)
            portfolio_state = _apply_portfolio_split(raw_plans, daily_risk_rs) or {}
            portfolio_state["date"] = plan_day_str
            portfolio_state.setdefault("plan_status", "READY")
            plan_source = "stored"

    plans: List[Dict[str, Any]] = portfolio_state.get("plans") or []
    plans_by_sym: Dict[str, Dict[str, Any]] = {
        p.get("symbol"): p for p in plans if isinstance(p, dict)
    }

    # 5) Build per-symbol view for UI
    result_symbols: Dict[str, Any] = {}

    for sym in SETTINGS.symbols:
        quote = quotes_by_sym.get(sym, {}) or {}
        plan = plans_by_sym.get(sym, {}) or {}

        # Tags priority:
        # (A) plan["tags"] (post-plan)
        tags: Dict[str, Any] = {}
        plan_tags = plan.get("tags")
        if isinstance(plan_tags, dict):
            tags.update(plan_tags)

        # (B) flattened plan keys (compat)
        for key in ("OpeningTrend", "OpenLocation", "PrevDayContext"):
            if key not in tags and key in plan:
                tags[key] = plan.get(key)

        # (C) state["tags"][sym] fallback (pre-09:40 tags-only stage)
        st = tags_by_sym.get(sym) or {}
        if isinstance(st, dict):
            for key in ("OpeningTrend", "OpenLocation", "PrevDayContext"):
                if not str(tags.get(key) or "").strip():
                    v = st.get(key)
                    if v is not None:
                        tags[key] = v

        # Ensure keys always exist (UI stability)
        tags.setdefault("OpeningTrend", "")
        tags.setdefault("OpenLocation", "")
        tags.setdefault("PrevDayContext", "")

        # Confidence: allow both "confidence%" and "confidence"
        confidence = None
        if "confidence%" in plan:
            confidence = plan.get("confidence%")
        elif "confidence" in plan:
            confidence = plan.get("confidence")

        plan_view = {
            "pick": plan.get("pick"),
            "confidence": confidence,
            "entry": plan.get("entry"),
            "stop": plan.get("stop"),
            "qty": plan.get("qty"),
            "target1": plan.get("target1"),
            "target2": plan.get("target2"),
            "per_trade_risk_rs": plan.get("per_trade_risk_rs_used"),
            "skip": plan.get("skip"),
            "reason": plan.get("reason"),
        }

        result_symbols[sym] = {
            "ltp": quote.get("ltp"),
            "ohlc": quote.get("ohlc") or {},
            "volume": quote.get("volume"),
            "tags": tags,
            "plan": plan_view,
        }

    # 6) Overlay paper truth (positions/PnL) onto plan view
    positions_by_sym = live_state.get("positions") or {}
    open_total = 0.0
    realized_total = 0.0

    if isinstance(positions_by_sym, dict):
        for sym, row in result_symbols.items():
            pos = positions_by_sym.get(sym)
            if not isinstance(pos, dict):
                continue

            row["paper"] = pos
            plan_view = row.get("plan") or {}

            side = str(pos.get("side") or "").upper()
            if side == "LONG":
                plan_view["pick"] = "BULL"
            elif side == "SHORT":
                plan_view["pick"] = "BEAR"

            # Force critical fields from paper execution
            plan_view["entry"] = pos.get("entry_price")
            plan_view["stop"] = pos.get("stop_price")
            plan_view["qty"] = pos.get("qty")
            plan_view["target1"] = pos.get("t1_price")
            plan_view["target2"] = pos.get("t2_price")

            open_pnl = float(pos.get("open_pnl_rs") or 0.0)
            realized_pnl = float(pos.get("realized_pnl_rs") or 0.0)
            pnl_rs = open_pnl + realized_pnl
            plan_view["pnl_rs"] = pnl_rs

            st_status = str(pos.get("status") or "").upper()
            exit_reason = str(pos.get("exit_reason") or "")
            if st_status == "OPEN":
                plan_view["status"] = "OPEN"
            elif exit_reason:
                plan_view["status"] = f"CLOSED ({exit_reason})"
            else:
                plan_view["status"] = "CLOSED"

            row["plan"] = plan_view
            open_total += open_pnl
            realized_total += realized_pnl

    day_total = open_total + realized_total

    meta = {
        "mode": mode,
        "sim_day": sim_day_str or plan_day_str,
        "sim_clock": sim_clock,
        "portfolio_date": portfolio_state.get("date"),
        "daily_risk_rs": portfolio_state.get("daily_risk_rs"),
        "active_trades": portfolio_state.get("active_trades"),
        "risk_per_trade_rs": portfolio_state.get("risk_per_trade_rs"),
        "plan_source": plan_source,
        "plan_status": portfolio_state.get("plan_status"),
        "day_pnl_rs": day_total,
        "open_pnl_rs": open_total,
        "realized_pnl_rs": realized_total,
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
    state = aj.read(default={}) or {}

    # Guarantee schema for UI (LIVE + SIM)
    if state.get("quotes") is None:
        state["quotes"] = {}

    return state


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

