import os, pandas as pd
from .common import ensure_dir
from ..infra.settings import SETTINGS

LEGACY_TM5 = [
    "data/intraday/{sym}_tm5min.csv",
    "data/intraday/{sym}_TM5MIN.csv",
]

def path_for(sym: str) -> str:
    p = SETTINGS.paths.intraday.format(sym=sym)
    if os.path.exists(p):
        return p
    for c in LEGACY_TM5:
        c2 = c.format(sym=sym)
        if os.path.exists(c2):
            return c2
    return p

def read(sym: str) -> pd.DataFrame:
    p = path_for(sym)
    if not os.path.exists(p):
        return pd.DataFrame()
    return pd.read_csv(p)

def write(sym: str, df: pd.DataFrame) -> str:
    p = SETTINGS.paths.intraday.format(sym=sym)
    ensure_dir(os.path.dirname(p))
    df.to_csv(p, index=False)
    return p
