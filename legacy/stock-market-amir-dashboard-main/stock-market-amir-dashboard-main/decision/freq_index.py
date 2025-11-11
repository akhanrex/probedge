from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd

Tag3 = Tuple[str, str, str]   # (OT, OL, PDC)
Tag2 = Tuple[str, str]        # (OT, OL)

@dataclass
class Pick:
    pick: str        # "BULL" | "BEAR" | "ABSTAIN"
    conf_pct: int
    bull_n: int
    bear_n: int
    total: int
    gap_pp: float
    level: str       # "L3" | "L2" | "L1" | "L0"
    reason: str = ""

class FreqIndex:
    """
    Colab-aligned ladder:
      L3: exact (OT, OL, PDC)
      L2: (OT, OL)
      L1: (OT)
      L0: global
    Gates per level: min_n, edge_pp, conf_floor; ignore TR in counts.
    If require_ot_align is true, only enforce when OT ∈ {BULL, BEAR}.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg or {}

        # gates
        self.edge_pp: float = float(self.cfg.get("edge_pp", 8.0))
        self.conf_floor: int = int(self.cfg.get("conf_floor", 55))

        mins = self.cfg.get("min_samples", {}) or {}
        self.min_L3: int = int(mins.get("L3", 8))
        self.min_L2: int = int(mins.get("L2", 6))
        self.min_L1: int = int(mins.get("L1", 4))
        self.min_L0: int = int(mins.get("L0", 3))

        fb = self.cfg.get("fallback", {}) or {}
        self.use_L2: bool = bool(fb.get("use_L2", True))
        self.use_L1: bool = bool(fb.get("use_L1", True))
        self.use_L0: bool = bool(fb.get("use_L0", True))

        self.require_ot_align: bool = bool(self.cfg.get("require_ot_align", True))

        # columns
        cols = self.cfg.get("cols", {}) or {}
        self.c_date = cols.get("date", "Date")
        self.c_res  = cols.get("result", "Result")
        self.c_ot   = cols.get("ot", "OpeningTrend")
        self.c_ol   = cols.get("ol", "OpenLocation")
        self.c_pdc  = cols.get("pdc", "PrevDayContext")

        # cubes[sym] = {"L3": {(ot,ol,pdc):{"BULL":x,"BEAR":y}}, "L2": {(ot,ol):...}, "L1": {ot:...}, "L0": {"BULL":x,"BEAR":y}}
        self.cubes: Dict[str, Dict[str, dict]] = {}

    # ---------- load & normalize ----------
    def _norm(self, s: pd.Series) -> pd.Series:
        return (
            s.astype(str)
             .str.strip()
             .str.upper()
             .replace({"NAN": ""})
        )

    def _load_master(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        df = df.rename(columns={
            self.c_date: "Date",
            self.c_res:  "Result",
            self.c_ot:   "OT",
            self.c_ol:   "OL",
            self.c_pdc:  "PDC",
        })
        for c in ("OT", "OL", "PDC", "Result"):
            if c in df.columns:
                df[c] = self._norm(df[c])
            else:
                df[c] = ""
        r = df["Result"]
        r = r.str.replace(r"\s+", " ", regex=True).replace({
            "BULL LEG": "BULL",
            "BEAR LEG": "BEAR",
            "BULLISH": "BULL",
            "BEARISH": "BEAR",
        })
        r = r.where(r.isin(["BULL", "BEAR", "TR"]), other="")
        df["Result"] = r
        if "Date" in df.columns:
            try:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
            except Exception:
                pass
        return df

    @staticmethod
    def _inc_bucket(d: dict, k, side: str) -> None:
        if side not in ("BULL", "BEAR"):
            return
        bucket = d.get(k)
        if bucket is None:
            bucket = {"BULL": 0, "BEAR": 0}
            d[k] = bucket
        bucket[side] += 1

    def build_for_symbol(self, symbol: str, master_path: str) -> None:
        df = self._load_master(master_path)
        L3: Dict[Tag3, Dict[str, int]] = {}
        L2: Dict[Tag2, Dict[str, int]] = {}
        L1: Dict[str, Dict[str, int]]   = {}
        L0: Dict[str, int]              = {"BULL": 0, "BEAR": 0}

        lab = df["Result"]
        m = (lab == "BULL") | (lab == "BEAR")
        for _, row in df.loc[m].iterrows():
            ot = (row.get("OT", "") or "").strip().upper()
            ol = (row.get("OL", "") or "").strip().upper()
            pdc = (row.get("PDC", "") or "").strip().upper()
            res = row["Result"]

            # L3 exact
            self._inc_bucket(L3, (ot, ol, pdc), res)
            # L2
            self._inc_bucket(L2, (ot, ol), res)
            # L1
            self._inc_bucket(L1, ot, res)
            # L0
            if res in ("BULL", "BEAR"):
                L0[res] += 1

        self.cubes[symbol] = {"L3": L3, "L2": L2, "L1": L1, "L0": L0}

    # ---------- persistence ----------
    def _ser_L3(self, mp: dict) -> list:
        return [{"k":[k[0],k[1],k[2]], "BULL":v.get("BULL",0), "BEAR":v.get("BEAR",0)} for k,v in mp.items()]
    def _de_L3(self, payload: list) -> dict:
        out = {}
        for row in payload or []:
            k = tuple(row.get("k", ["","",""]))
            out[k] = {"BULL": int(row.get("BULL",0)), "BEAR": int(row.get("BEAR",0))}
        return out

    def _ser_L2(self, mp: dict) -> list:
        return [{"k":[k[0],k[1]], "BULL":v.get("BULL",0), "BEAR":v.get("BEAR",0)} for k,v in mp.items()]
    def _de_L2(self, payload: list) -> dict:
        out = {}
        for row in payload or []:
            k = tuple(row.get("k", ["",""]))
            out[k] = {"BULL": int(row.get("BULL",0)), "BEAR": int(row.get("BEAR",0))}
        return out

    def _ser_L1(self, mp: dict) -> list:
        return [{"ot": k, "BULL": v.get("BULL",0), "BEAR": v.get("BEAR",0)} for k,v in mp.items()]
    def _de_L1(self, payload: list) -> dict:
        out = {}
        for row in payload or []:
            k = row.get("ot","")
            out[k] = {"BULL": int(row.get("BULL",0)), "BEAR": int(row.get("BEAR",0))}
        return out

    def save_cache(self, symbol: str, out_path: str) -> None:
        data = self.cubes.get(symbol, {})
        payload = {
            "L3": self._ser_L3(data.get("L3", {})),
            "L2": self._ser_L2(data.get("L2", {})),
            "L1": self._ser_L1(data.get("L1", {})),
            "L0": data.get("L0", {"BULL":0,"BEAR":0}),
        }
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        tmp.replace(p)

    def load_cache(self, symbol: str, in_path: str) -> bool:
        p = Path(in_path)
        if not p.exists():
            return False
        try:
            with open(p, "r", encoding="utf-8") as f:
                payload = json.load(f)
            self.cubes[symbol] = {
                "L3": self._de_L3(payload.get("L3", [])),
                "L2": self._de_L2(payload.get("L2", [])),
                "L1": self._de_L1(payload.get("L1", [])),
                "L0": payload.get("L0", {"BULL":0,"BEAR":0}),
            }
            return True
        except Exception:
            return False

    # ---------- helpers ----------
    @staticmethod
    def _decide(bull: int, bear: int) -> tuple[str, int, int, float]:
        total = bull + bear
        if total <= 0:
            return "ABSTAIN", 0, total, 0.0
        if bull == bear:
            return "ABSTAIN", int(round(100 * bull / total)), total, 0.0
        pick = "BULL" if bull > bear else "BEAR"
        conf = int(round(100 * max(bull, bear) / total))
        bull_pct = 100.0 * bull / total
        bear_pct = 100.0 * bear / total
        gap_pp = abs(bull_pct - bear_pct)
        return pick, conf, total, gap_pp

    def _gate(self, level: str, pick: str, conf: int, total: int, gap: float, ot: str) -> bool:
        if pick == "ABSTAIN":
            return False
        min_map = {"L3": self.min_L3, "L2": self.min_L2, "L1": self.min_L1, "L0": self.min_L0}
        if total < int(min_map[level]): return False
        if gap   < float(self.edge_pp): return False
        if conf  < int(self.conf_floor): return False
        if self.require_ot_align and ot in ("BULL","BEAR") and pick != ot:
            # only enforce if OT is directional; if OT==TR we allow the pick
            return False
        return True

    # ---------- public query ----------
    def query_three_tags(self, symbol: str, ot: str, ol: str, pdc: str) -> Pick:
        """Progressive ladder L3→L2→L1→L0 with gates on each level."""
        ot  = (ot or "").strip().upper()
        ol  = (ol or "").strip().upper()
        pdc = (pdc or "").strip().upper()

        if not (ot and ol and pdc):
            return Pick("ABSTAIN", 0, 0, 0, 0, 0.0, "L3", "select PDC · OL · OT")

        cubes = self.cubes.get(symbol, {})
        L3 = cubes.get("L3", {})
        L2 = cubes.get("L2", {})
        L1 = cubes.get("L1", {})
        L0 = cubes.get("L0", {"BULL":0,"BEAR":0})

        # ---- L3 exact
        c3 = L3.get((ot, ol, pdc))
        if c3:
            b, r = int(c3.get("BULL",0)), int(c3.get("BEAR",0))
            pick, conf, total, gap = self._decide(b, r)
            if self._gate("L3", pick, conf, total, gap, ot):
                reason = (f"L3 freq: OT={ot}, OL={ol}, PDC={pdc} | "
                          f"BULL={b}, BEAR={r}, N={total}, gap={gap:.1f}pp, conf={conf}%")
                return Pick(pick, conf, b, r, total, gap, "L3", reason)

        # ---- L2 (OT, OL)
        if self.use_L2:
            c2 = L2.get((ot, ol))
            if c2:
                b, r = int(c2.get("BULL",0)), int(c2.get("BEAR",0))
                pick, conf, total, gap = self._decide(b, r)
                if self._gate("L2", pick, conf, total, gap, ot):
                    reason = (f"L2 freq: OT={ot}, OL={ol} | "
                              f"BULL={b}, BEAR={r}, N={total}, gap={gap:.1f}pp, conf={conf}%")
                    return Pick(pick, conf, b, r, total, gap, "L2", reason)

        # ---- L1 (OT)
        if self.use_L1:
            c1 = L1.get(ot)
            if c1:
                b, r = int(c1.get("BULL",0)), int(c1.get("BEAR",0))
                pick, conf, total, gap = self._decide(b, r)
                if self._gate("L1", pick, conf, total, gap, ot):
                    reason = (f"L1 freq: OT={ot} | "
                              f"BULL={b}, BEAR={r}, N={total}, gap={gap:.1f}pp, conf={conf}%")
                    return Pick(pick, conf, b, r, total, gap, "L1", reason)

        # ---- L0 global
        if self.use_L0 and isinstance(L0, dict):
            b, r = int(L0.get("BULL",0)), int(L0.get("BEAR",0))
            pick, conf, total, gap = self._decide(b, r)
            if self._gate("L0", pick, conf, total, gap, ot):
                reason = (f"L0 freq: GLOBAL | "
                          f"BULL={b}, BEAR={r}, N={total}, gap={gap:.1f}pp, conf={conf}%")
                return Pick(pick, conf, b, r, total, gap, "L0", reason)

        # nothing strong → expose L3 counts if present for transparency
        b = r = total = 0
        gap = 0.0
        if c3:
            b, r = int(c3.get("BULL",0)), int(c3.get("BEAR",0))
            total = b + r
            if total:
                bull_pct = 100.0 * b / total
                bear_pct = 100.0 * r / total
                gap = abs(bull_pct - bear_pct)
        conf = int(round(100 * max(b, r) / total)) if total else 0
        return Pick("ABSTAIN", conf, b, r, total, gap, "L3", "insufficient N/edge")

# compatibility alias
FreqIndex3 = FreqIndex
__all__ = ["FreqIndex", "FreqIndex3", "Pick"]
