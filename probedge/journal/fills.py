# probedge/journal/fills.py

from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Dict

from probedge.infra.logger import get_logger
from probedge.infra.settings import SETTINGS

log = get_logger(__name__)


def _fills_path() -> Path:
    """
    Derive fills.csv next to journal.csv.
    """
    base = Path(SETTINGS.paths.journal or "data/journal/journal.csv")
    return base.with_name("fills.csv")


FILLS_FIELDS = [
    "day",
    "mode",
    "symbol",
    "side",
    "qty",
    "entry",
    "stop",
    "target1",
    "target2",
    "entry_ts",
    "exit_ts",
    "exit_price",
    "exit_reason",
    "pnl_rs",
    "pnl_r",
    "planned_risk_rs",
    "daily_risk_rs",
    "strategy",
    "created_at",
]


def append_fills(rows: Iterable[Dict[str, object]]) -> int:
    """
    Append one row per executed trade into fills.csv.

    `rows` must be dicts containing at least the keys in FILLS_FIELDS.
    Missing keys are written as empty strings.
    """
    path = _fills_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    write_header = not path.exists()
    count = 0

    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FILLS_FIELDS)
        if write_header:
            writer.writeheader()
        for r in rows:
            out = {k: r.get(k, "") for k in FILLS_FIELDS}
            writer.writerow(out)
            count += 1

    log.info("Appended %d fills to %s", count, path)
    return count
