# probedge/journal/writer.py

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import csv
from datetime import datetime

from probedge.infra.settings import SETTINGS
from probedge.infra.logger import get_logger

log = get_logger(__name__)

JOURNAL_PATH = Path(SETTINGS.paths.journal or "data/journal/journal.csv")


JOURNAL_COLUMNS = [
    "day",
    "mode",
    "symbol",
    "side",                 # BUY for BULL, SELL for BEAR
    "qty",
    "entry",
    "stop",
    "target1",
    "target2",
    "planned_risk_rs",
    "daily_risk_rs",
    "confidence_pct",
    "tag_OT",
    "tag_OL",
    "tag_PDC",
    "reason",
    "parity_mode",
    "strategy",
    "created_at",
]


def _ensure_journal_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        log.info("Creating new journal at %s", path)
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=JOURNAL_COLUMNS)
            writer.writeheader()


def _plan_to_rows(portfolio_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert a portfolio_plan dict (from live_state.json or /api/state)
    into a list of journal rows (dicts with JOURNAL_COLUMNS keys).
    """
    day = str(portfolio_plan.get("date") or "")
    mode = str(portfolio_plan.get("mode") or "")
    daily_risk = float(portfolio_plan.get("daily_risk_rs") or 0)
    plans = portfolio_plan.get("plans") or []

    rows: List[Dict[str, Any]] = []
    created_at = datetime.now().isoformat(timespec="seconds")

    for p in plans:
        pick = p.get("pick")
        qty = int(p.get("qty") or 0)
        if pick not in ("BULL", "BEAR") or qty <= 0:
            # We only journal active trades
            continue

        symbol = str(p.get("symbol") or "")
        entry = float(p.get("entry") or 0.0)
        stop = float(p.get("stop") or 0.0)
        t1 = float(p.get("target1") or 0.0)
        t2 = float(p.get("target2") or 0.0)
        planned_risk = float(p.get("per_trade_risk_rs_used") or 0.0)
        conf = int(p.get("confidence%") or 0)
        tags = p.get("tags") or {}

        side = "BUY" if pick == "BULL" else "SELL"
        row = {
            "day": day,
            "mode": mode,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry": entry,
            "stop": stop,
            "target1": t1,
            "target2": t2,
            "planned_risk_rs": planned_risk,
            "daily_risk_rs": daily_risk,
            "confidence_pct": conf,
            "tag_OT": tags.get("OpeningTrend") or "",
            "tag_OL": tags.get("OpenLocation") or "",
            "tag_PDC": tags.get("PrevDayContext") or "",
            "reason": p.get("reason") or "",
            "parity_mode": bool(p.get("parity_mode")),
            "strategy": "parity_v1",
            "created_at": created_at,
        }
        rows.append(row)

    return rows


def append_portfolio_plan(portfolio_plan: Dict[str, Any]) -> int:
    """
    Append all active trades from a portfolio_plan into the journal CSV.

    Returns: number of rows written.
    """
    _ensure_journal_header(JOURNAL_PATH)

    rows = _plan_to_rows(portfolio_plan)
    if not rows:
        log.info("No active trades in portfolio_plan; nothing to journal.")
        return 0

    with JOURNAL_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=JOURNAL_COLUMNS)
        for row in rows:
            writer.writerow(row)

    log.info(
        "Appended %d planned trades to journal %s for day=%s",
        len(rows),
        JOURNAL_PATH,
        portfolio_plan.get("date"),
    )
    return len(rows)
