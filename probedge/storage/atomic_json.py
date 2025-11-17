# probedge/storage/atomic_json.py
#
# Simple atomic JSON file helper.
# Used by batch_agent and others to read/write live_state.json safely.

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any


class AtomicJSON:
    """
    Small helper around a JSON file:
      - ensures directory exists
      - reads JSON (or returns default on error)
      - writes atomically via temp file + os.replace
      - uses a local lock to avoid concurrent writes from this process
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def read(self, default: Any = None) -> Any:
        """
        Read JSON content. If file missing or invalid, return `default`.
        """
        with self._lock:
            if not self.path.exists():
                return default
            try:
                with self.path.open("r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return default

    def write(self, obj: Any) -> None:
        """
        Write JSON content atomically:
          - dump to tmp file
          - os.replace into final path
        """
        with self._lock:
            tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp_path, self.path)
