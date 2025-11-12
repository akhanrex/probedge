from __future__ import annotations
from pathlib import Path
from typing import Optional, Tuple
from probedge.infra.settings import SETTINGS

# Symbol aliasing hidden from API/UI (TMPV maps to TATAMOTORS internally)
ALIASES = {"TMPV": "TATAMOTORS"}

def _canonical_sym(sym: str) -> str:
    s = (sym or "").upper().strip()
    return ALIASES.get(s, s)

def intraday_path(sym: str) -> Path:
    s = _canonical_sym(sym)
    # canonical write path
    p = SETTINGS.paths.intraday.format(sym=s)
    return (SETTINGS.data_dir / p).resolve()

def master_path(sym: str) -> Path:
    s = _canonical_sym(sym)
    p = SETTINGS.paths.masters.format(sym=s)
    return (SETTINGS.data_dir / p).resolve()

def journal_path() -> Path:
    return (SETTINGS.data_dir / SETTINGS.paths.journal).resolve()

def state_path() -> Path:
    return (SETTINGS.data_dir / SETTINGS.paths.state).resolve()

# ---- Legacy fallbacks (READ-ONLY) -------------------------------------
# We never WRITE to these, only read if canonical missing.
LEGACY_PATTERNS = [
    # intraday legacy
    ("intraday", "data/intraday/{sym}_5MINUTE.csv"),
    ("intraday", "data/{sym}/tm5min.csv"),
    # masters legacy
    ("masters",  "data/masters/{sym}_5MINUTE_MASTER_INDICATORS.csv"),
    ("masters",  "DATA_DIR/master/{sym}_Master.csv"),
]

def locate_for_read(kind: str, sym: Optional[str] = None) -> Path:
    """
    kind: 'intraday' | 'masters' | 'journal' | 'state'
    returns a path that exists (canonical first, then legacy fallbacks).
    """
    if kind == "journal":
        p = journal_path()
        return p
    if kind == "state":
        p = state_path()
        return p

    if not sym:
        raise ValueError("locate_for_read(kind=intraday/masters) requires sym")

    # canonical first
    p = intraday_path(sym) if kind == "intraday" else master_path(sym)
    if p.exists():
        return p

    # legacy fallbacks (read-only)
    s = _canonical_sym(sym)
    for k, pat in LEGACY_PATTERNS:
        if k != kind:
            continue
        lp = (SETTINGS.data_dir / pat.format(sym=s)).resolve()
        if lp.exists():
            return lp

    return p  # return canonical even if missing; callers can handle missing file
