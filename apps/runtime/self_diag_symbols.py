# apps/runtime/self_diag_symbols.py

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # repo root: .../probedge/probedge

TARGET_DIRS = ["apps", "probedge"]
NEEDLES = ["TMPV", "TATAMOTORS", "_1minute.csv", "_5minute.csv"]


def scan():
    print(f"[SELF-DIAG] repo root = {ROOT}")
    for sub in TARGET_DIRS:
        base = ROOT / sub
        if not base.exists():
            print(f"[SELF-DIAG] SKIP missing dir: {base}")
            continue

        print(f"\n[SELF-DIAG] Scanning {base} ...")
        for path in base.rglob("*.py"):
            try:
                text = path.read_text()
            except Exception:
                continue

            hits = [needle for needle in NEEDLES if needle in text]
            if hits:
                rel = path.relative_to(ROOT)
                print(f"  - {rel}: {', '.join(hits)}")


if __name__ == "__main__":
    scan()
