# app/instruments.py

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import os
from app.config import get_secret_first

@dataclass
class InstrumentConfig:
    key: str
    label: str
    master_csv_default: str
    master_csv_env: Optional[str] = None
    match_target: int = 150
    aplus_min_eff: float = 15
    aplus_rate: float = 60
    aplus_comp: float = 0.90
    hide_tags: List[str] = field(default_factory=list)

    @property
    def master_path(self) -> str:
        env = self.master_csv_env
        if env and os.getenv(env):
            return os.getenv(env)  # type: ignore
        mnt = os.path.join("/mnt/data", self.master_csv_default)
        if os.path.isdir("/mnt/data") and os.path.exists(mnt):
            return mnt
        return self.master_csv_default

INSTRUMENTS: Dict[str, InstrumentConfig] = {
    "tm": InstrumentConfig(
        key="tm",
        label="Tata Motors",
        master_csv_default="data/masters/TataMotors_Master.csv",
        master_csv_env="PROBEDGE_MASTER_CSV",  # keep as-is if you already use it
        match_target=int(get_secret_first("TM_MATCH_TARGET", default="150")),
        aplus_min_eff=float(get_secret_first("TM_APLUS_MIN_EFF", default="15")),
        aplus_rate=float(get_secret_first("TM_APLUS_RATE", default="60")),
        aplus_comp=float(get_secret_first("TM_APLUS_COMP", default="0.9")),
    ),
    "lt": InstrumentConfig(
        key="lt",
        label="LT",
        master_csv_default="data/masters/LT_Master.csv",     # <-- correct name
        master_csv_env="LT_MASTER_CSV",                      # optional env override
        match_target=int(get_secret_first("LT_MATCH_TARGET", default="150")),
        aplus_min_eff=float(get_secret_first("LT_APLUS_MIN_EFF", default="15")),
        aplus_rate=float(get_secret_first("LT_APLUS_RATE", default="60")),
        aplus_comp=float(get_secret_first("LT_APLUS_COMP", default="0.9")),
    ),
    "sbin": InstrumentConfig(
        key="sbin",
        label="SBIN",
        master_csv_default="data/masters/SBIN_Master.csv",   # <-- correct name
        master_csv_env="SBIN_MASTER_CSV",                    # optional env override
        match_target=int(get_secret_first("SBIN_MATCH_TARGET", default="150")),
        aplus_min_eff=float(get_secret_first("SBIN_APLUS_MIN_EFF", default="15")),
        aplus_rate=float(get_secret_first("SBIN_APLUS_RATE", default="60")),
        aplus_comp=float(get_secret_first("SBIN_APLUS_COMP", default="0.9")),
    ),
}

def get_tm_thresholds():
    return {
        "enter_threshold": float(get_secret_first("TM_ENTER_THRESHOLD", default="60")),
        "safety_min_eff_n": float(get_secret_first("TM_SAFETY_MIN_EFF_N", default="8")),
        "safety_min_comp": float(
            get_secret_first("TM_SAFETY_MIN_COMP", default="0.85")
        ),
        "pen_eff_max": float(get_secret_first("TM_PEN_EFF_MAX", default="15")),
        "pen_comp_max": float(get_secret_first("TM_PEN_COMP_MAX", default="15")),
        "pen_qual_max": float(get_secret_first("TM_PEN_QUAL_MAX", default="10")),
        "pen_sample_max": float(get_secret_first("TM_PEN_SAMPLE_MAX", default="8")),
        "pen_fat_max": float(get_secret_first("TM_PEN_FAT_MAX", default="6")),
        "qual_benchmark": float(get_secret_first("TM_QUAL_BENCHMARK", default="75")),
        "sample_bench": float(get_secret_first("TM_SAMPLE_BENCH", default="0.66")),
        "fatigue_scale": float(get_secret_first("TM_FATIGUE_SCALE", default="10")),
        "bonus_aplus": float(get_secret_first("TM_BONUS_APLUS", default="3")),
        "depth_norm": float(get_secret_first("TM_DEPTH_NORM", default="25")),
        "fatigue_cap": float(get_secret_first("TM_FATIGUE_CAP", default="0.2")),
    }
