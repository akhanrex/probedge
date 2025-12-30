"""Execution gating based on the immutable 09:40 plan snapshot.

Invariant (single source of truth):
- Block *all* execution (paper/live OMS, intraday simulation) unless:
    1) state.plan_snapshot.status in {"READY", "READY_PARTIAL"}
    2) AND plan_snapshot.portfolio_plan.plan_locked is True

Anything that recomputes later (e.g. /api/plan) is diagnostic-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


ALLOWED_SNAPSHOT_STATUSES = {"READY", "READY_PARTIAL"}


@dataclass(frozen=True)
class PlanGate:
    ok: bool
    reason: str
    status: str = "MISSING"
    plan_locked: bool = False


def evaluate_plan_gate(state: Dict[str, Any], *, day: Optional[str] = None) -> PlanGate:
    """Evaluate whether execution is allowed for this state/day."""
    snap = state.get("plan_snapshot")
    if not isinstance(snap, dict):
        return PlanGate(False, "plan_snapshot missing", status="MISSING", plan_locked=False)

    status = str(snap.get("status") or "MISSING").upper()
    snap_day = snap.get("day")

    if day and snap_day and str(snap_day) != str(day):
        return PlanGate(
            False,
            f"plan_snapshot day mismatch (have={snap_day}, want={day})",
            status=status,
            plan_locked=False,
        )

    portfolio = snap.get("portfolio_plan")
    if not isinstance(portfolio, dict):
        return PlanGate(False, "plan_snapshot.portfolio_plan missing", status=status, plan_locked=False)

    locked = bool(portfolio.get("plan_locked") is True)

    if status not in ALLOWED_SNAPSHOT_STATUSES:
        return PlanGate(False, f"plan_snapshot.status={status} not executable", status=status, plan_locked=locked)

    if not locked:
        return PlanGate(False, "portfolio_plan.plan_locked is false", status=status, plan_locked=False)

    return PlanGate(True, "ok", status=status, plan_locked=True)


def require_plan_gate(state: Dict[str, Any], *, day: Optional[str] = None) -> None:
    """Raise RuntimeError if execution gate is not satisfied."""
    g = evaluate_plan_gate(state, day=day)
    if not g.ok:
        raise RuntimeError(f"EXECUTION_BLOCKED: {g.reason} (status={g.status}, plan_locked={g.plan_locked})")


def get_locked_portfolio_plan(state: Dict[str, Any], *, day: Optional[str] = None) -> Tuple[Dict[str, Any], PlanGate]:
    """Return (portfolio_plan, gate) from the snapshot.

    If gate is not ok, portfolio_plan will be {}.
    """
    g = evaluate_plan_gate(state, day=day)
    if not g.ok:
        return {}, g
    snap = state.get("plan_snapshot") or {}
    return dict(snap.get("portfolio_plan") or {}), g
