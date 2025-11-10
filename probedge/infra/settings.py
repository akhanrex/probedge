from pydantic import BaseModel
from dotenv import load_dotenv
import os, yaml

load_dotenv()

class Paths(BaseModel):
    masters: str
    intraday: str
    ticks: str
    journal: str
    state: str

class Settings(BaseModel):
    mode: str
    bar_seconds: int
    data_dir: str
    risk_rs: float
    slippage_cap_pct: float
    nudge_pct: float
    client_id_prefix: str
    allowed_origins: str
    symbols: list[str]
    paths: Paths
    kite_api_key: str | None = None
    kite_api_secret: str | None = None
    kite_access_token: str | None = None

def load_settings() -> Settings:
    with open(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "frequency.yaml"), "r") as f:
        y = yaml.safe_load(f) or {}
    symbols = y.get("symbols", [])
    paths = y.get("paths", {})
    return Settings(
        mode=os.getenv("MODE", "paper"),
        bar_seconds=int(os.getenv("BAR_SECONDS", "300")),
        data_dir=os.getenv("DATA_DIR", "./data"),
        risk_rs=float(os.getenv("RISK_RS", "15000")),
        slippage_cap_pct=float(os.getenv("SLIPPAGE_CAP_PCT", "0.0005")),
        nudge_pct=float(os.getenv("NUDGE_PCT", "0.0003")),
        client_id_prefix=os.getenv("CLIENT_ID_PREFIX", "PROB"),
        allowed_origins=os.getenv("ALLOWED_ORIGINS", "*"),
        symbols=symbols,
        paths=Paths(**paths),
        kite_api_key=os.getenv("KITE_API_KEY"),
        kite_api_secret=os.getenv("KITE_API_SECRET"),
        kite_access_token=os.getenv("KITE_ACCESS_TOKEN"),
    )

SETTINGS = load_settings()
