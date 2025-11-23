from __future__ import annotations

import shutil
from pathlib import Path
from typing import List

from probedge.infra.settings import SETTINGS
from probedge.storage import resolver
from probedge.infra.logger import get_logger

log = get_logger(__name__)


def _get_source_root() -> Path:
    """
    Local 'truth' data root for sync into this repo.

    We expect all CSVs (possibly in subfolders) to live under:
      <DATA_DIR>/data/import

    DATA_DIR is /Users/aamir/Downloads/probedge/probedge in your setup.
    """
    return (SETTINGS.data_dir / "data" / "import").resolve()


def _find_one(source_root: Path, sym: str, kind: str) -> Path:
    """
    Find a single source file for a symbol in source_root (recursively).

    kind: "tm5"     -> *_5minute.csv
          "master"  -> *_5MINUTE_MASTER*.csv
    """
    sym = sym.upper()
    patterns: List[str] = []

    if kind == "tm5":
        patterns.append(f"{sym}_5minute.csv")
        if sym == "TATAMOTORS":
            # allow legacy TMPV alias
            patterns.append("TMPV_5minute.csv")
    elif kind == "master":
        patterns.append(f"{sym}_5MINUTE_MASTER*.csv")
        if sym == "TATAMOTORS":
            patterns.append("TMPV_5MINUTE_MASTER*.csv")
    else:
        raise ValueError(f"Unknown kind={kind}")

    matches: List[Path] = []
    for pat in patterns:
        matches.extend(source_root.rglob(pat))

    if not matches:
        raise FileNotFoundError(f"No {kind} file found for {sym} under {source_root}")

    matches = sorted(set(matches))
    return matches[-1]  # take the last one (e.g. if there are backups)


def sync_symbol(source_root: Path, sym: str) -> None:
    sym = sym.upper()

    src_tm5 = _find_one(source_root, sym, "tm5")
    src_master = _find_one(source_root, sym, "master")

    dest_tm5 = resolver.intraday_path(sym)
    dest_master = resolver.master_path(sym)

    dest_tm5.parent.mkdir(parents=True, exist_ok=True)
    dest_master.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(src_tm5, dest_tm5)
    shutil.copy2(src_master, dest_master)

    log.info(
        "Synced %s:\n  tm5    %s -> %s\n  master %s -> %s",
        sym,
        src_tm5,
        dest_tm5,
        src_master,
        dest_master,
    )


def main() -> None:
    source_root = _get_source_root()
    if not source_root.exists():
        raise SystemExit(f"[data_sync] Source root does not exist: {source_root}")

    log.info("[data_sync] Using source root: %s", source_root)

    symbols = [s.upper() for s in SETTINGS.symbols]
    for sym in symbols:
        try:
            sync_symbol(source_root, sym)
        except Exception as e:
            log.error("Failed to sync %s: %s", sym, e)


if __name__ == "__main__":
    main()
