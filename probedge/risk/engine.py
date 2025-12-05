# probedge/risk/engine.py

from __future__ import annotations
from typing import Dict, Any


def compute_risk_state(
    positions: Dict[str, Dict[str, Any]],
    daily_risk_rs: float,
    manual_kill: bool = False,
) -> Dict[str, Any]:
    """
    Aggregate realized/open P&L and decide if new trades are allowed.
    """
    realized_rs = 0.0
    open_rs = 0.0

    for pos in positions.values():
        realized_rs += float(pos.get("realized_pnl_rs", 0.0) or 0.0)
        if pos.get("status") == "OPEN":
            open_rs += float(pos.get("open_pnl_rs", 0.0) or 0.0)

    day_pnl_rs = realized_rs + open_rs
    loss_cap_rs = -float(daily_risk_rs)

    status = "OK"
    can_open_new_trades = True
    reason = "OK"

    if manual_kill:
        status = "HARD_STOP"
        can_open_new_trades = False
        reason = "MANUAL_KILL_SWITCH"
    elif day_pnl_rs <= loss_cap_rs:
        status = "HARD_STOP"
        can_open_new_trades = False
        reason = "DAY_PNL_BELOW_LOSS_CAP"
    elif day_pnl_rs < 0:
        status = "WARN"
        can_open_new_trades = True
        reason = "DAY_PNL_NEGATIVE"

    return {
        "realized_rs": round(realized_rs, 2),
        "open_rs": round(open_rs, 2),
        "day_pnl_rs": round(day_pnl_rs, 2),
        "loss_cap_rs": round(loss_cap_rs, 2),
        "status": status,
        "can_open_new_trades": can_open_new_trades,
        "reason": reason,
    }
