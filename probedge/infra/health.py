# probedge/infra/health.py
#
# Simple health & heartbeat helper for the Probedge runtime.

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional

from probedge.infra.settings import SETTINGS
from probedge.storage.atomic_json import AtomicJSON


STATE_PATH = SETTINGS.paths.state  # e.g. data/state/live_state.json
_HEALTH_KEY = "health"


@dataclass
class HealthState:
    system_status: str          # "OK", "WARN", "DOWN"
    reason: str                 # short human-readable reason
    last_agg5_ts: Optional[float] = None
    last_batch_ts: Optional[float] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "HealthState":
        return cls(
            system_status=d.get("system_status", "DOWN"),
            reason=d.get("reason", "uninitialized"),
            last_agg5_ts=d.get("last_agg5_ts"),
            last_batch_ts=d.get("last_batch_ts"),
        )


def _read_state() -> Dict[str, Any]:
    """Read the existing live_state.json (if any)."""
    try:
        if not os.path.exists(STATE_PATH):
            return {}
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _atomic_write_state(state: Dict[str, Any]) -> None:
    """Write live_state.json atomically (avoid partial writes)."""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    AtomicJSON(STATE_PATH).write(state)


def _parse_iso_ts(v: Any) -> Optional[float]:
    """Parse ISO timestamp string (or epoch) -> epoch seconds. Returns None on failure."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return None
    if not isinstance(v, str):
        return None
    try:
        dt = datetime.fromisoformat(v)
        return dt.timestamp()
    except Exception:
        return None


def record_agg5_heartbeat() -> None:
    """Called by realtime.agg5 whenever it successfully writes/updates bars."""
    now = time.time()
    state = _read_state()
    health_dict = state.get(_HEALTH_KEY, {})
    health = HealthState.from_dict(health_dict)

    # Fallback: if last_batch_ts missing but batch_agent.last_heartbeat_ts exists, derive it.
    if health.last_batch_ts is None:
        try:
            ba = state.get("batch_agent") or {}
            ts = ba.get("last_heartbeat_ts")
            if isinstance(ts, str) and ts:
                dt = __import__("datetime").datetime.fromisoformat(ts)
                health.last_batch_ts = dt.timestamp()
        except Exception:
            pass
    health.last_agg5_ts = now
    state[_HEALTH_KEY] = asdict(health)
    _atomic_write_state(state)


def record_batch_agent_heartbeat(
    agent: str = "batch_agent",
    status: str = "RUNNING",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Heartbeat for Phase A / batch loop.

    Backward compatible:
      - old callers: record_batch_agent_heartbeat()
      - new callers: record_batch_agent_heartbeat(agent, status, extra)

    Side effects:
      - updates state["health"]["last_batch_ts"]
      - updates state["batch_agent"]["last_heartbeat_ts"] (ISO)
    """
    from datetime import datetime, timezone, timedelta

    IST = timezone(timedelta(hours=5, minutes=30))

    now = time.time()
    state = _read_state()

    # health block
    health_dict = state.get(_HEALTH_KEY, {})
    health = HealthState.from_dict(health_dict)
    health.last_batch_ts = now
    state[_HEALTH_KEY] = asdict(health)

    # batch_agent block (UI/debug)
    ba = state.get("batch_agent")
    if not isinstance(ba, dict):
        ba = {}
    ba["status"] = status
    ba["phase"] = ba.get("phase") or "PHASE_A"
    ba["agent"] = agent
    ba["last_heartbeat_ts"] = datetime.now(tz=IST).isoformat(timespec="seconds")

    if isinstance(extra, dict) and extra:
        # Keep it minimal / stable
        if isinstance(extra.get("details"), str):
            ba["details"] = extra["details"]

    state["batch_agent"] = ba
    _atomic_write_state(state)


def assess_health(max_bar_lag_sec: int = 600, max_plan_lag_sec: int = 600) -> HealthState:
    """
    Compute overall system_status based on heartbeats.
    - If agg5 or batch_agent have not reported in a long time, mark WARN/DOWN.
    - If ENABLE_AGG5=false, we *do not* treat missing agg5 heartbeat as a problem.
    """
    now = time.time()
    state = _read_state()
    health_dict = state.get(_HEALTH_KEY)

    if not health_dict:
        return HealthState(
            system_status="WARN",
            reason="health block not initialized; components may not be running yet",
            last_agg5_ts=None,
            last_batch_ts=None,
        )

    health = HealthState.from_dict(health_dict)
    problems = []
    notes = []

    enable_agg5 = os.getenv("ENABLE_AGG5", "true").lower() == "true"

    # --- agg5 check (only if enabled) ---
    if enable_agg5:
        if health.last_agg5_ts is None:
            problems.append("agg5 has never reported")
        else:
            lag = now - health.last_agg5_ts
            if lag > max_bar_lag_sec:
                problems.append(f"agg5 heartbeat stale ({int(lag)}s ago)")
    else:
        notes.append("agg5 disabled via ENABLE_AGG5=false")

    # --- batch_agent check (always required) ---
    # fallback to state["batch_agent"]["last_heartbeat_ts"] if health.last_batch_ts missing
    if health.last_batch_ts is None:
        try:
            ba = state.get("batch_agent") or {}
            if isinstance(ba, dict):
                fb = _parse_iso_ts(ba.get("last_heartbeat_ts"))
                if fb is not None:
                    health.last_batch_ts = fb
        except Exception:
            pass

    if health.last_batch_ts is None:
        problems.append("batch_agent has never reported")
    else:
        lag = now - health.last_batch_ts
        if lag > max_plan_lag_sec:
            problems.append(f"batch_agent heartbeat stale ({int(lag)}s ago)")

    # --- decide overall status ---
    if not problems:
        health.system_status = "OK"
        extra = f" ({'; '.join(notes)})" if notes else ""
        health.reason = "all critical components reporting within thresholds" + extra
    else:
        if len(problems) >= 2:
            health.system_status = "DOWN"
        else:
            health.system_status = "WARN"
        msg = "; ".join(problems)
        if notes:
            msg = msg + " | " + "; ".join(notes)
        health.reason = msg

    state[_HEALTH_KEY] = asdict(health)
    _atomic_write_state(state)
    return health


def set_system_status(status: str, reason: str) -> None:
    """
    Force-set system_status + reason (used by supervisor when a process dies
    or when user stops the system). Next assess_health() call may refine it
    based on heartbeats.
    """
    state = _read_state()
    health_dict = state.get(_HEALTH_KEY, {})
    health = HealthState.from_dict(health_dict)
    health.system_status = status
    health.reason = reason
    state[_HEALTH_KEY] = asdict(health)
    _atomic_write_state(state)
