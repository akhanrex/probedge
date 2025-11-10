import os, pandas as pd
from .common import ensure_dir
from ..infra.settings import SETTINGS

LEGACY_CANDIDATES = [
    "data/masters/{sym}_5MINUTE_MASTER_INDICATORS.csv",
    "DATA_DIR/master/{sym}_Master.csv",
]

def path_for(sym: str) -> str:
    p = SETTINGS.paths.masters.format(sym=sym)
    if os.path.exists(p):
        return p
    for c in LEGACY_CANDIDATES:
        c2 = c.replace("DATA_DIR", SETTINGS.data_dir).format(sym=sym)
        if os.path.exists(c2):
            return c2
    return p

def read(sym: str) -> pd.DataFrame:
    p = path_for(sym)
    if not os.path.exists(p):
        return pd.DataFrame()
    return pd.read_csv(p)

def write(sym: str, df: pd.DataFrame) -> str:
    p = SETTINGS.paths.masters.format(sym=sym)
    ensure_dir(os.path.dirname(p))
    df.to_csv(p, index=False)
    return p
