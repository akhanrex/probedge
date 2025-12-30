
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Optional, List
from datetime import time as dtime

import numpy as np
import pandas as pd

from apps.storage.tm5 import read_master
from probedge.storage.resolver import locate_for_read
from ._freq_select import apply_lookback, select_hist_batch_parity

router = APIRouter(prefix="/api", tags=["superpath"])

T0 = dtime(9, 40)
T1 = dtime(15, 5)

def _norm(s: Optional[str]) -> str:
    return str(s or "").strip().upper()

def _read_intraday_5m(sym: str) -> pd.DataFrame:
    path = locate_for_read("intraday", sym)
    df = pd.read_csv(path)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]
    cols = {c.lower(): c for c in df.columns}

    if "datetime" in cols:
        dt = pd.to_datetime(df[cols["datetime"]], errors="coerce")
    elif "date" in cols and ("time" in cols or "timestr" in cols):
        tcol = cols.get("time") or cols.get("timestr")
        dt = pd.to_datetime(df[cols["date"]].astype(str) + " " + df[tcol].astype(str), errors="coerce")
    elif "DateTime" in df.columns:
        dt = pd.to_datetime(df["DateTime"], errors="coerce")

    else:
        raise ValueError(f"Intraday file has no datetime columns: {path}")

    df["DateTime"] = dt
    df = df.dropna(subset=["DateTime"]).sort_values("DateTime").reset_index(drop=True)

    def pick(*names):
        for n in names:
            if n in cols: return cols[n]
        for c in df.columns:
            if c.lower() in names: return c
        return None

    om = pick("open","o"); hm = pick("high","h"); lm = pick("low","l"); cm = pick("close","c")
    if om and om != "Open": df.rename(columns={om: "Open"}, inplace=True)
    if hm and hm != "High": df.rename(columns={hm: "High"}, inplace=True)
    if lm and lm != "Low":  df.rename(columns={lm: "Low"}, inplace=True)
    if cm and cm != "Close":df.rename(columns={cm: "Close"}, inplace=True)

    for k in ("Open","High","Low","Close"):
        df[k] = pd.to_numeric(df[k], errors="coerce")

    df = df.dropna(subset=["Open","High","Low","Close"]).copy()
    df["Date"] = df["DateTime"].dt.date
    df["_t"] = df["DateTime"].dt.time
    return df

@router.get("/superpath")
def superpath(
    symbol: str = Query(...),
    ot: Optional[str] = Query(None),
    ol: Optional[str] = Query(None),
    pdc: Optional[str] = Query(None),
    asof: str | None = Query(None),
):
    sym = _norm(symbol)
    otN, olN, pdcN = _norm(ot), _norm(ol), _norm(pdc)

    try:
        m = read_master(sym)
        if m is None or m.empty:
            return {"bars": [], "cone": [], "meta": {"N": 0}}

        m, _day = apply_lookback(m, asof)
        hist_bb, meta_sel = select_hist_batch_parity(m, otN, olN, pdcN)
        if hist_bb is None or hist_bb.empty or "Date" not in hist_bb.columns:
            return {"bars": [], "cone": [], "meta": {"N": 0}}

        dd = pd.to_datetime(hist_bb["Date"], errors="coerce").dropna().dt.date
        match_dates = sorted(set(dd.tolist()))
        if not match_dates:
            return {"bars": [], "cone": [], "meta": {"N": 0}}

        intr = _read_intraday_5m(sym)

        series_list: List[pd.Series] = []
        missing = 0
        for d in match_dates:
            daydf = intr[intr["Date"] == d]
            if daydf.empty:
                missing += 1
                continue

            win = daydf[(daydf["_t"] >= T0) & (daydf["_t"] <= T1)].sort_values("DateTime")
            if win.empty:
                missing += 1
                continue

            entry = float(win["Open"].iloc[0])  # batch reference (09:40 open)
            if not np.isfinite(entry) or entry <= 0:
                missing += 1
                continue

            rel_pct = (win["Close"].to_numpy(float) / entry - 1.0) * 100.0
            series_list.append(pd.Series(rel_pct, index=range(len(rel_pct))))

        if not series_list:
            return {"bars": [], "cone": [], "meta": {"N": 0, "missing": len(match_dates)}}

        max_len = max(len(s) for s in series_list)
        mat = np.full((len(series_list), max_len), np.nan, dtype=float)
        for i, s in enumerate(series_list):
            mat[i, :len(s)] = s.to_numpy(dtype=float)

        med = np.nanmedian(mat, axis=0)
        p25 = np.nanpercentile(mat, 25, axis=0)
        p75 = np.nanpercentile(mat, 75, axis=0)

        cone = []
        for idx in range(max_len):
            if np.isnan(med[idx]) and np.isnan(p25[idx]) and np.isnan(p75[idx]):
                continue
            cone.append({"bar": idx, "med": float(round(med[idx], 3)), "p25": float(round(p25[idx], 3)), "p75": float(round(p75[idx], 3))})

        end_vals = [float(s.iloc[-1]) for s in series_list if len(s) > 0]
        mean_end = float(np.nanmean(end_vals)) if end_vals else 0.0
        bias = "BULL" if mean_end > 0 else ("BEAR" if mean_end < 0 else "NEUTRAL")
        hits = sum(1 for v in end_vals if (v > 0 if mean_end > 0 else (v < 0 if mean_end < 0 else False)))
        confidence = int(round(100 * hits / len(end_vals))) if end_vals else 0

        meta = {
            "N": int(len(series_list)),
            "N_total": int(len(match_dates)),
            "missing": int(missing),
            "level": meta_sel.get("level"),
            "mean_end": float(round(mean_end, 3)),
            "bias": bias,
            "confidence": confidence,
        }
        return {"bars": [], "cone": cone, "meta": meta}

    except Exception as e:
        return {"bars": [], "cone": [], "meta": {"N": 0}, "error": f"{type(e).__name__}: {e}"}
