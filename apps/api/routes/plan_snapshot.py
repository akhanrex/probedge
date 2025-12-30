# apps/api/routes/plan_snapshot.py

from __future__ import annotations

from pathlib import Path

from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from probedge.infra.settings import SETTINGS
from probedge.storage.atomic_json import AtomicJSON

router = APIRouter()

STATE_FILE = Path(SETTINGS.paths.state or "data/state/live_state.json")
# Resolve relative state path under DATA_DIR (critical for SIM vs LIVE)
if not STATE_FILE.is_absolute():
    STATE_FILE = Path(SETTINGS.data_dir) / STATE_FILE

aj = AtomicJSON(str(STATE_FILE))


@router.get("/api/plan_snapshot")
def api_plan_snapshot(
    day: Optional[date] = Query(None),
) -> Dict[str, Any]:
    """Return the immutable 09:40 plan snapshot, if present."""
    day = day or date.today()
    day_str = day.isoformat()


    # Prefer per-day archived snapshot if available (for replay/debug).
    try:
        snap_path = STATE_FILE.parent / "plan_snapshots" / f"{day_str}.json"
        if snap_path.exists():
            snap = AtomicJSON(str(snap_path)).read(default=None)
            if isinstance(snap, dict) and snap.get("day") == day_str:
                return snap
    except Exception:
        pass

    state = aj.read(default={}) or {}
    snap = state.get("plan_snapshot")

    if isinstance(snap, dict) and snap.get("day") == day_str:
        return snap

    return {"day": day_str, "status": "MISSING"}
