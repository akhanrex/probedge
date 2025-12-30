"""
Runtime daily timeline / gating.

This module is responsible for producing an immutable PlanSnapshot for the trading day and
mirroring it into live_state.json without clobbering other writers.

Key contract:
- PlanSnapshot is the single source of truth after it is locked (>= 09:40:01 IST by default)
- All writes to live_state.json are PATCH-ONLY merges (never full-state overwrites)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date as Date, time as Time
from pathlib import Path
from typing import Any, Dict, Optional

from probedge.infra.settings import SETTINGS
from probedge.infra.clock_source import get_now_ist, now_ist
from probedge.storage.atomic_json import AtomicJSON
from probedge.decision.portfolio_planner import build_portfolio_state_for_day

def _build_portfolio_state_for_day_compat(day_date, risk_rs=None):
    """Compat: different repo versions use different arg names."""
    # try known keyword names
    if risk_rs is None:
        return build_portfolio_state_for_day(day_date)

    for kw in ("risk_rs", "daily_risk_rs", "risk", "risk_budget_rs"):
        try:
            return build_portfolio_state_for_day(day_date, **{kw: risk_rs})
        except TypeError:
            pass

    # final fallback: positional second arg (if supported)
    try:
        return build_portfolio_state_for_day(day_date, risk_rs)
    except TypeError:
        # if builder only accepts (day_date), still return without risk
        return build_portfolio_state_for_day(day_date)


log = logging.getLogger(__name__)

STATE_PATH = SETTINGS.paths.state
PLAN_READY_TIME = Time(9, 40, 1)


def _plan_snapshot_dir() -> Path:
    # keep next to live_state.json to ensure DATA_DIR isolation
    return Path(STATE_PATH).parent / "plan_snapshots"


def _plan_snapshot_path(day: str) -> Path:
    return _plan_snapshot_dir() / f"{day}.json"


def _archive_snapshot(day: str, snapshot: Dict[str, Any]) -> None:
    p = _plan_snapshot_path(day)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(p)


def publish_tags_only(day: str, risk_rs: Optional[int] = None) -> None:
    """
    Optional early-stage helper: compute today's tags and mirror into live_state.
    This does NOT lock a plan snapshot.
    """
    aj = AtomicJSON(STATE_PATH)
    state = aj.read(default={}) or {}
    if risk_rs is None:
        risk_rs = int(state.get("daily_risk_rs") or 10000)

    ps = build_portfolio_state_for_day(Date.fromisoformat(day), risk_rs=risk_rs)
    tags_by_sym: Dict[str, Any] = {}
    for plan in ps.get("plans", []) or []:
        sym = plan.get("symbol")
        if sym:
            tags_by_sym[str(sym)] = plan.get("tags") or {}

    aj.write(
        {
            "date": day,
            "plan_day": day,
            "tags": tags_by_sym,
            "clock_ist": now_ist().isoformat(),
        }
    )


def arm_portfolio_for_day(day: str, risk_rs: Optional[int] = None, wait_for_time: bool = True) -> None:
    """
    Build + lock the immutable PlanSnapshot for `day`.

    If an archived snapshot exists, we re-export it into live_state and return.
    """
    aj = AtomicJSON(STATE_PATH)

    # 1) FAST PATH: if archived snapshot exists, export and return
    snap_path = _plan_snapshot_path(day)
    if snap_path.exists():
        snapshot = json.loads(snap_path.read_text(encoding="utf-8"))
        # Ensure snapshot has READY fields (UI + exec gating)
        if "status" not in snapshot:
            snapshot["status"] = "READY" if snapshot.get("locked") else "BUILDING"
        if "built_at" not in snapshot:
            snapshot["built_at"] = snapshot.get("created_at_ist") or now_ist().isoformat()
        # keep archive up-to-date so UI sees READY
        _archive_snapshot(day, snapshot)
        portfolio_plan = snapshot.get("portfolio_plan") or snapshot.get("portfolio_plan_state") or {}
        aj.write(
            {
                "date": day,
                "plan_day": day,
                "plan_snapshot": snapshot,
                "portfolio_plan": portfolio_plan,
                "plan_locked": True,
                "clock_ist": now_ist().isoformat(),
            }
        )
        log.info("arm_portfolio_for_day: exported archived snapshot day=%s", day)
        return

    # 2) Determine risk
    state = aj.read(default={}) or {}
    if risk_rs is None:
        risk_rs = int(state.get("daily_risk_rs") or 10000)

    # 3) Time gate (optional)
    if wait_for_time:
        while True:
            state = aj.read(default={}) or {}
            now = get_now_ist(state)
            if now.time() >= PLAN_READY_TIME:
                break
            time.sleep(0.5)

    # 4) Build portfolio state (plans + split)
    portfolio_state = _build_portfolio_state_for_day_compat(Date.fromisoformat(day), risk_rs)

    active_trades = int(portfolio_state.get("active_trades") or 0)
    risk_per_trade_rs = int(portfolio_state.get("risk_per_trade_rs") or 0)

    snapshot: Dict[str, Any] = {
        "day": day,
        "locked": True,
        "created_at_ist": now_ist().isoformat(),
        "status": "READY",
        "built_at": now_ist().isoformat(),
        "daily_risk_rs": int(risk_rs),
        "active_trades": active_trades,
        "risk_per_trade_rs": risk_per_trade_rs,
        "symbols": list(getattr(SETTINGS, "symbols", []) or []),
        "plans": portfolio_state.get("plans", []) or [],
        # keep a copy to make the snapshot self-contained
        "portfolio_plan": {
            "active_trades": active_trades,
            "risk_per_trade_rs": risk_per_trade_rs,
            "daily_risk_rs": int(risk_rs),
            "plan_locked": True,
            "plans": portfolio_state.get("plans", []) or [],
        },
    }

    # 5) Archive + export (PATCH-ONLY)
    _archive_snapshot(day, snapshot)

    aj.write(
        {
            "date": day,
            "plan_day": day,
            "plan_snapshot": snapshot,
            "portfolio_plan": snapshot["portfolio_plan"],
            "plan_locked": True,
            "daily_risk_rs": int(risk_rs),
            "clock_ist": now_ist().isoformat(),
        }
    )

    log.info(
        "arm_portfolio_for_day: locked snapshot day=%s active_trades=%s risk_per_trade_rs=%s",
        day,
        active_trades,
        risk_per_trade_rs,
    )
