from fastapi import APIRouter, Query, HTTPException
import os, pathlib, yaml
import pandas as pd

router = APIRouter()

def _read_yaml():
    ypath = pathlib.Path("config/frequency.yaml")
    if ypath.exists():
        try:
            return yaml.safe_load(ypath.read_text()) or {}
        except Exception:
            return {}
    return {}

def _intraday_candidates(symbol: str):
    """Return a list of candidate file paths to try for tm5 CSV."""
    cfg = _read_yaml()
    paths = (cfg.get("paths") or {}) if isinstance(cfg, dict) else {}
    tmpl = paths.get("intraday")
    syms = [symbol]
    # Accept TATAMOTORS/TMPV interchangeably for file lookups
    if symbol == "TMPV":
        syms.append("TATAMOTORS")
    if symbol == "TATAMOTORS":
        syms.append("TMPV")

    cands = []
    for sym in syms:
        if tmpl:
            try:
                cands.append(tmpl.format(sym=sym))
            except Exception:
                pass
        # canonical fallback
        cands.append(f"data/intraday/{sym}_5minute.csv")
        # legacy dashboard path
        cands.append(f"legacy/stock-market-amir-dashboard-main/stock-market-amir-dashboard-main/data/intraday/{sym}_5minute.csv")
        # legacy single-file fallback
        cands.append("legacy/stock-market-amir-dashboard-main/stock-market-amir-dashboard-main/tm5min.csv")
    # de-dup while preserving order
    seen = set()
    out = []
    for p in cands:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def _load_tm5(symbol: str, limit: int):
    for p in _intraday_candidates(symbol):
        f = pathlib.Path(p)
        if not f.exists() or not f.is_file():
            continue
        try:
            df = pd.read_csv(f)
        except Exception:
            continue

        # normalize columns
        cols = {c.lower(): c for c in df.columns}
        def c_(x): return cols.get(x.lower(), x)
        rename = {
            c_("DateTime"): "DateTime",
            c_("date_time"): "DateTime",
            c_("date"): "Date",
            c_("open"): "Open",
            c_("high"): "High",
            c_("low"): "Low",
            c_("close"): "Close",
            c_("volume"): "Volume",
        }
        df = df.rename(columns=rename)
        # build DateTime if only Date exists
        if "DateTime" not in df.columns:
            if "Date" in df.columns:
                df["DateTime"] = pd.to_datetime(df["Date"], errors="coerce")
            else:
                continue
        else:
            df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")

        # keep only expected cols
        keep = [c for c in ["DateTime","Open","High","Low","Close","Volume"] if c in df.columns]
        if "Open" not in keep or "Close" not in keep:
            continue

        df = df.sort_values("DateTime").dropna(subset=["DateTime"]).reset_index(drop=True)
        # tail(limit) to return the latest rows (matches UI expectations)
        df = df.tail(limit).copy()

        # JSON safe
        df["DateTime"] = df["DateTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        # Ensure numerics are proper
        for k in ["Open","High","Low","Close","Volume"]:
            if k in df.columns:
                df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0)

        return df.to_dict(orient="records"), str(f)

    raise FileNotFoundError("No intraday file found for symbol")

@router.get("/api/tm5")
def get_tm5(symbol: str, limit: int = Query(200, ge=1, le=5000)):
    try:
        data, path = _load_tm5(symbol.strip().upper(), limit)
        return {"symbol": symbol.strip().upper(), "rows": len(data), "source": path, "data": data}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # surface any parsing errors cleanly
        raise HTTPException(status_code=500, detail=f"tm5 error: {e}")
