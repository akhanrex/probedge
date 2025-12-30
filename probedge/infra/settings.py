import os, json
from typing import List
from pathlib import Path
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
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
    # Accept .env and IGNORE any extra keys so your old env never breaks
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    MODE: str = Field(default=MODE_PAPER)
    DATA_DIR: str = Field(default="./")

    # Risk budgets
    RISK_RS_DEFAULT: int = Field(default=10000)  # live/paper default
    RISK_RS_TEST: int = Field(default=1000)      # test default

    # Legacy / OMS related keys (we accept & keep them)
    BAR_SECONDS: int = Field(default=300)
    SLIPPAGE_CAP_PCT: float = Field(default=0.0005)
    NUDGE_PCT: float = Field(default=0.0003)
    CLIENT_ID_PREFIX: str = Field(default="PROB")

    # CORS / Ops
    ALLOWED_ORIGINS: str = Field(default="*")

    # Kite keys (for later phases)
    KITE_API_KEY: str = Field(default="")
    KITE_API_SECRET: str = Field(default="")
    KITE_REDIRECT_URL: str = Field(default="")
    KITE_SESSION_FILE: str = Field(default="data/state/kite_session.json")
    KITE_ACCESS_TOKEN: str = Field(default="")


class Settings(BaseModel):
    # Core
    mode: str
    data_dir: Path
    symbols: List[str]
    paths: _Paths

    # Risk
    risk_rs_default: int
    risk_rs_test: int

    # OMS/Runtime knobs we will use in later phases
    bar_seconds: int
    slippage_cap_pct: float
    nudge_pct: float
    client_id_prefix: str

    # Ops
    allowed_origins: List[str]

    # Kite auth
    kite_api_key: str | None = None
    kite_api_secret: str | None = None
    kite_redirect_url: str | None = None
    kite_session_file: Path | None = None

    # Derived — read-only
    @property
    def risk_budget_rs(self) -> int:
        """Current session’s daily risk budget driven by mode (test vs paper/live)."""
        return self.risk_rs_test if self.mode == MODE_TEST else self.risk_rs_default

    @field_validator("mode")
    @classmethod
    def _valid_mode(cls, v: str) -> str:
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
        # Core
        mode=env.MODE.lower().strip(),
        data_dir=Path(env.DATA_DIR).resolve(),
        symbols=symbols,
        paths=freq.paths,

        # Risk
        risk_rs_default=int(env.RISK_RS_DEFAULT),
        risk_rs_test=int(env.RISK_RS_TEST),

        # OMS/Runtime
        bar_seconds=int(env.BAR_SECONDS),
        slippage_cap_pct=float(env.SLIPPAGE_CAP_PCT),
        nudge_pct=float(env.NUDGE_PCT),
        client_id_prefix=str(env.CLIENT_ID_PREFIX),

        # Ops
        allowed_origins=_split_origins(env.ALLOWED_ORIGINS),

        # Kite auth
        kite_api_key=env.KITE_API_KEY or None,
        kite_api_secret=env.KITE_API_SECRET or None,
        kite_redirect_url=env.KITE_REDIRECT_URL or None,
        kite_session_file=(
            Path(env.KITE_SESSION_FILE).resolve()
            if env.KITE_SESSION_FILE else None
        ),
    )

# Singleton
SETTINGS = load_settings()

def _debug_dump_settings():
    d = SETTINGS.model_dump()
    d["data_dir"] = str(SETTINGS.data_dir)
    print("[Settings]", json.dumps(d, indent=2))

if os.environ.get("PROBEDGE_DEBUG_SETTINGS") == "1":
    _debug_dump_settings()

# --- PROBEDGE_ABSOLUTE_STATE_PATH_UNDER_DATA_DIR ---
from pathlib import Path as _Path

def _pb__abs_under_data_dir(_maybe_path: str, _default_rel: str) -> str:
    base = _Path(getattr(SETTINGS, "data_dir", ".")).expanduser().resolve()
    if not _maybe_path:
        return str(base / _default_rel)
    P = _Path(str(_maybe_path)).expanduser()
    return str(P if P.is_absolute() else (base / P))

def _pb__safe_set(obj, name, value) -> None:
    try:
        setattr(obj, name, value)
        return
    except Exception:
        pass
    try:
        object.__setattr__(obj, name, value)
    except Exception:
        pass

# Force state path to be absolute under DATA_DIR so SIM/LIVE switching is deterministic.
try:
    _paths = getattr(SETTINGS, "paths", None)
    if _paths is not None:
        _pb__safe_set(_paths, "state", _pb__abs_under_data_dir(getattr(_paths, "state", None), "data/state/live_state.json"))
except Exception:
    pass

