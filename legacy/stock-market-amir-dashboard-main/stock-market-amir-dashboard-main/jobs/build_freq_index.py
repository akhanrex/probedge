from __future__ import annotations
from pathlib import Path
import yaml

from decision.freq_index import FreqIndex

CFG_PATH = Path("config/frequency.yaml")
with open(CFG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

fidx = FreqIndex(cfg)

for sym, mpath in (cfg.get("masters") or {}).items():
    fidx.build_for_symbol(sym, mpath)
    out = Path(f"storage/cache/freq_index_{sym}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    fidx.save_cache(sym, str(out))
    print(f"[OK] {sym}: wrote {out}")

print("[DONE] Frequency caches built.")
