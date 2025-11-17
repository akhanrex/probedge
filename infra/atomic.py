"""
Atomic JSON write helper.

Use this instead of direct open(path, "w") to avoid partial/corrupt files.
"""

import os
import json
import tempfile
from typing import Any, Dict


def atomic_json_write(path: str, data: Dict[str, Any]) -> None:
    """
    Write JSON to `path` atomically:
      - write to a temp file in the same directory
      - fsync
      - os.replace to final path

    On POSIX, os.replace is atomic.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        dir=directory,
        prefix=".tmp_state_",
        suffix=".json",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        # In case of any exception before replace
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
