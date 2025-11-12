import os, json
from typing import Dict, List, Optional
from pathlib import Path
from pydantic import BaseModel, BaseSettings, Field, validator
import yaml

# Modes
MODE_TEST = "test"
MODE_PAPER = "paper"
MODE_LIVE = "live"
VALID_MODES = {MODE_TEST, MODE_PAPER, MODE_LIVE}

class _Paths(BaseModel):
    masters: str
    intraday: str
    ticks: str
    journal: str
    state: str

class _FrequencyConfig(BaseModel):
    symbols: List[str]
    paths: _Paths

class _Env(BaseSettings):
    MODE: str = Field(default=MODE_PAPER)
    DATA_DIR: str = Field(default="./")
    # Risk budgets
    RISK_RS_DEFAULT: int = Field(default=10000)  # live/paper default
    RISK_RS_TEST: int = Field(default=1000)      # test default
    # Ops / CORS
    ALLOWED_ORIGINS: str = Field(default="*")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

class Settings(BaseModel):
    mode: str
    data_dir: Path
    symbols: List[str]
    paths: _Paths
    allowed_origins: List[str]
    risk_rs_default: int
    risk_rs_test: int

    # Derived — read-only
    @property
    def risk_budget_rs(self) -> int:
        """Current session’s daily risk budget driven by mode (test vs paper/live)."""
        return self.risk_rs_test if self.mode == MODE_TEST else self.risk_rs_default

    @validator("mode")
    def _valid_mode(cls, v):
        v = (v or "").lower().strip()
        if v not in VALID_MODES:
            raise ValueError(f"Invalid MODE={v}. Use one of {sorted(VALID_MODES)}")
        return v

def _load_yaml(path: Path) -> _FrequencyConfig:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return _FrequencyConfig(**raw)

def _split_origins(s: str) -> List[str]:
    s = (s or "").strip()
    if s == "*":
        return ["*"]
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts or ["*"]

def load_settings() -> Settings:
    env = _Env()  # loads from .env if present
    freq = _load_yaml(Path("config") / "frequency.yaml")

    # Normalize symbols (upper, unique, stable order)
    seen = set()
    symbols = []
    for sym in freq.symbols:
        u = str(sym).upper().strip()
        if u and u not in seen:
            symbols.append(u)
            seen.add(u)

    return Settings(
        mode=env.MODE.lower().strip(),
        data_dir=Path(env.DATA_DIR).resolve(),
        symbols=symbols,
        paths=freq.paths,
        allowed_origins=_split_origins(env.ALLOWED_ORIGINS),
        risk_rs_default=int(env.RISK_RS_DEFAULT),
        risk_rs_test=int(env.RISK_RS_TEST),
    )

# Singleton
SETTINGS = load_settings()

# Developer helper: pretty-print once at startup (optional)
def _debug_dump_settings():
    d = SETTINGS.dict()
    d["data_dir"] = str(SETTINGS.data_dir)
    print("[Settings]", json.dumps(d, indent=2))

if os.environ.get("PROBEDGE_DEBUG_SETTINGS") == "1":
    _debug_dump_settings()
