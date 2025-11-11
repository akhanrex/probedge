# app/views/journal.py — thin wrapper around the journal tab (config-aware, import-after-env)
from __future__ import annotations

import os
import inspect
from pathlib import Path
import streamlit as st

from app.ui import show_logo


def _repo_root() -> Path:
    # app/views/journal.py → parents: [journal.py, views, app, <repo root>]
    return Path(__file__).resolve().parents[2]


def _load_cfg_file() -> dict:
    """Load config/journal_config.yaml if present."""
    cfg_path = _repo_root() / "config" / "journal_config.yaml"
    try:
        import yaml  # local import to keep run.py light

        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _normalize_dir(p: str | Path) -> Path:
    """Expand ~, make absolute, and resolve relative paths against repo root."""
    base = _repo_root()
    s = str(p) if isinstance(p, Path) else str(p or "")
    pp = Path(s).expanduser()
    if not pp.is_absolute():
        pp = base / pp
    try:
        return pp.resolve()
    except Exception:
        return pp


def _merged_cfg(passed: dict | None) -> dict:
    """
    Merge passed-in cfg on top of file cfg, and ensure journal.data_dir exists with a default.
    Default = <repo root>/data/latest  (NOT probedge/data/latest)
    """
    file_cfg = _load_cfg_file()
    cfg: dict = {}
    if isinstance(file_cfg, dict):
        cfg.update(file_cfg)
    if isinstance(passed, dict):
        cfg.update(passed)

    j = cfg.get("journal") or {}
    cfg["journal"] = j

    default_dir = _repo_root() / "data" / "latest"
    j["data_dir"] = str(_normalize_dir(j.get("data_dir", default_dir)))
    return cfg


def render_journal_view(cfg: dict | None = None) -> None:
    show_logo(centered=False)

    # Merge: YAML on disk + anything passed from run.py, normalize paths, set default
    merged = _merged_cfg(cfg)

    # Ensure the adapter gets the path BEFORE import (adapter reads env at import-time)
    base_dir = merged.get("journal", {}).get("data_dir", "")
    if base_dir:
        os.environ["PE_JOURNAL_DATA_DIR"] = str(base_dir)

    # Tiny hint so we can see what path is used
    st.caption(f"Journal data dir: `{base_dir}`")

    # Import AFTER setting env so the adapter sees the override
    try:
        from probedge.ui_adapters.journal_tab import render_journal as _render_journal
    except Exception:
        _render_journal = None

    if _render_journal is None:
        st.info("Journal module not available in this build.")
        return

    # Call downstream UI adapter; support both signatures (() vs (cfg))
    try:
        sig = inspect.signature(_render_journal)
        if len(sig.parameters) >= 1:
            _render_journal(merged)
        else:
            _render_journal()
    except TypeError:
        # If signature probing fails, try with cfg then without.
        try:
            _render_journal(merged)
        except Exception:
            _render_journal()

def read_today_journal(today_str: str):
    _ensure()
    rows = []
    with open(JOURNAL_PATH, "r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if row.get("date") == today_str:
                rows.append(row)
    return rows

