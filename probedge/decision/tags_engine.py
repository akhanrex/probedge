import pandas as pd
from probedge.core import classifiers as C
from pathlib import Path
from probedge.infra.settings import SETTINGS

def _intraday_path(sym: str) -> Path:
    raw = str(getattr(SETTINGS.paths, 'intraday', 'data/intraday'))
    if '{sym}' in raw:
        return Path(raw.format(sym=sym))
    p = Path(raw)
    return p if p.suffix.lower()=='.csv' else p / f"{sym}_5minute.csv"

def _master_path(sym: str) -> Path:
    raw = str(getattr(SETTINGS.paths, 'master', 'data/masters'))
    if '{sym}' in raw:
        return Path(raw.format(sym=sym))
    p = Path(raw)
    return p if p.suffix.lower()=='.csv' else p / f"{sym}_5MINUTE_MASTER.csv"

BASE_PATHS = getattr(SETTINGS, 'paths', None)
INTRA = Path(getattr(BASE_PATHS, 'intraday', 'data/intraday'))
MAST  = Path(getattr(BASE_PATHS, 'master',  'data/masters'))

def _read_intraday(sym: str) -> pd.DataFrame:
    p = _intraday_path(sym)
    df = pd.read_csv(p)
    # permissive normalize
    cols = {c.lower(): c for c in df.columns}
    def c(x): return cols.get(x.lower(), x)
    df = df.rename(columns={
        c("DateTime"): "DateTime",
        c("Open"): "Open", c("High"): "High",
        c("Low"): "Low",  c("Close"): "Close"
    })
    # Parse timezone-aware or naive; keep as pandas Timestamps
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    df["Date"] = df["DateTime"].dt.normalize()
    return df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime").reset_index(drop=True)

def _read_master(sym: str) -> pd.DataFrame:
    p = _master_path(sym)
    if not p.exists():
        return pd.DataFrame(columns=["Date","OpeningTrend","OpenLocation","PrevDayContext","Result"])
    df = pd.read_csv(p)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    for k in ("OpeningTrend","OpenLocation","PrevDayContext","Result"):
        if k in df.columns:
            df[k] = df[k].astype(str).str.strip().str.upper().replace({"NAN":""})
    return df

def _today(df_i: pd.DataFrame) -> pd.Timestamp:
    # last intraday date present = today's trading date for our offline calc
    return pd.to_datetime(df_i["Date"].max())

def compute_tags_for_day(sym: str, date_target=None):
    df_i = _read_intraday(sym)
    if df_i.empty: 
        raise ValueError(f"no intraday for {sym}")
    day = pd.to_datetime(date_target).normalize() if date_target else _today(df_i)
    prev_ohlc = C.prev_trading_day_ohlc(df_i, day)
    pdc = C.compute_prevdaycontext_robust(prev_ohlc)
    ol  = C.compute_openlocation_from_df(df_i, day, prev_ohlc)
    ot  = C.compute_openingtrend_robust(df_i, day)
    return {"PDC": pdc, "OL": ol, "OT": ot, "date": str(day.date())}

def compute_all_tags(symbols=None, date_target=None):
    syms = symbols or SETTINGS.symbols
    out = {}
    for s in syms:
        try:
            out[s] = compute_tags_for_day(s, date_target=date_target)
        except Exception as e:
            out[s] = {"error": str(e)}
    return out


# === TZ-SAFE OVERRIDES (last wins) ===
import pandas as _pd

def _to_naive_ist(dtser):
    dt = _pd.to_datetime(dtser, errors="coerce", utc=False)
    if _pd.api.types.is_datetime64tz_dtype(dt):
        try:
            dt = dt.dt.tz_convert("Asia/Kolkata")
        except Exception:
            pass
        dt = dt.dt.tz_localize(None)
    return dt

def _read_intraday(symbol: str):
    pth = _intraday_path(symbol)
    df = _pd.read_csv(pth)
    if "DateTime" in df.columns:
        dt = _to_naive_ist(df["DateTime"])
    elif "Date" in df.columns:
        dt = _to_naive_ist(df["Date"])
    else:
        raise ValueError("intraday csv must have DateTime or Date")
    df["DateTime"] = dt
    df["Date"] = dt.dt.normalize()
    for k in ("Open","High","Low","Close","Ticks","Volume"):
        if k in df.columns:
            df[k] = _pd.to_numeric(df[k], errors="coerce")
    df = df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime").reset_index(drop=True)
    return df

def _read_master(symbol: str):
    pth = _master_path(symbol)
    df = _pd.read_csv(pth)
    df["Date"] = _pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    for col in ("OpeningTrend","OpenLocation","PrevDayContext","Result"):
        if col in df.columns:
            df[col] = (df[col].astype(str).str.strip().str.upper().replace({"NAN": ""}))
    return df
