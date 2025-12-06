# apps/api/routes/risk.py

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from probedge.infra.settings import SETTINGS
from probedge.storage.atomic_json import AtomicJSON

router = APIRouter(prefix="/api", tags=["risk"])

STATE_PATH = SETTINGS.paths.state or "data/state/live_state.json"
aj = AtomicJSON(STATE_PATH)


class RiskUpdate(BaseModel):
    daily_risk_rs: int = Field(
        ...,
        ge=1000,
        le=500000,
        description="Daily risk budget in rupees for this stack",
    )


@router.post("/risk")
def update_risk(payload: RiskUpdate):
    """
    Persist a daily risk override for the current stack into live_state.json.

    Semantics (Phase A, paper mode):
    - Updates top-level `daily_risk_rs` for UI + intraday_paper.
    - Updates nested `portfolio_plan.daily_risk_rs` if present, so future
      planning runs have the same number.
    """
    daily_risk_rs = int(payload.daily_risk_rs)

    state = aj.read(default={}) or {}

    # Top-level override for intraday_paper + UI meta
    state["daily_risk_rs"] = daily_risk_rs
    state["risk_override_rs"] = daily_risk_rs

    # Also keep portfolio_plan in sync if it exists
    portfolio = state.get("portfolio_plan") or {}
    portfolio["daily_risk_rs"] = daily_risk_rs
    state["portfolio_plan"] = portfolio

    aj.write(state)

    return {
        "status": "ok",
        "daily_risk_rs": daily_risk_rs,
    }
