
from __future__ import annotations
from pathlib import Path
from typing import Dict, List
from math import exp, log
from datetime import datetime as dt
import pandas as pd
import numpy as np
import uvicorn
import yaml
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from decision.freq_index import FreqIndex
# --- Journal integration (reuse Streamlit journal logic) ---
try:
    # same module Streamlit uses: probedge/ui_adapters/journal_tab.py
    from probedge.ui_adapters import journal_tab as journal_mod
    JOURNAL_IMPORT_ERROR = None
except Exception as e:
    journal_mod = None
    JOURNAL_IMPORT_ERROR = str(e)


ROOT = Path(__file__).resolve().parent.parent
WEBUI_DIR = ROOT / "webui"
ASSETS_DIR = ROOT / "assets"
HALF_LIFE_DAYS = 60.0
LAMBDA = log(2.0) / HALF_LIFE_DAYS  # exponential decay per day

app = FastAPI(title="Probedge Frequency API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# static
app.mount("/webui", StaticFiles(directory=str(WEBUI_DIR), html=True), name="webui")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR), html=False), name="assets")

@app.get("/")
async def root():
    return RedirectResponse(url="/webui/pages/terminal.html", status_code=307)

# ---- config ----
CFG_PATH = (ROOT / "config" / "frequency.yaml").resolve()
try:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        CFG: Dict = yaml.safe_load(f) or {}
        ALLOWED_SYMBOLS = sorted((CFG.get("masters") or {}).keys())
except Exception as e:
    # Fail loud and clear so you see it in the uvicorn logs
    raise RuntimeError(f"Failed to load config at {CFG_PATH}: {e}")


# ---- freq engine ----
FIDX = FreqIndex(CFG)
for sym, mpath in (CFG.get("masters") or {}).items():
    cache_p = Path(f"storage/cache/freq_index_{sym}.json")
    if not FIDX.load_cache(sym, str(cache_p)):
        FIDX.build_for_symbol(sym, mpath)
        FIDX.save_cache(sym, str(cache_p))

# ---- preload masters for matches ----
MASTERS: Dict[str, pd.DataFrame] = {}
for sym, mpath in (CFG.get("masters") or {}).items():
    try:
        df = pd.read_csv(mpath)
        # normalize headers
        norm = {str(c).strip().lower().replace("\ufeff",""): c for c in df.columns}
        def has(k): return k in norm
        def src(k): return norm[k]

        ren = {}
        if has("date"): ren[src("date")] = "Date"
        if has("openingtrend"): ren[src("openingtrend")] = "OT"
        if has("openlocation"): ren[src("openlocation")] = "OL"
        if has("prevdaycontext"): ren[src("prevdaycontext")] = "PDC"
        if has("firstcandletype"): ren[src("firstcandletype")] = "FCT"
        if has("rangestatus"): ren[src("rangestatus")] = "RS"
        if has("result"): ren[src("result")] = "Result"

        df = df.rename(columns=ren)
        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()]

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

        for k in ("OT","OL","PDC","FCT","RS","Result"):
            if k in df.columns:
                df[k] = df[k].astype(str).str.strip().str.upper().replace({"NAN": ""})

        MASTERS[sym] = df[["Date","OT","OL","PDC","FCT","RS","Result"]].copy()
    except Exception:
        MASTERS[sym] = pd.DataFrame(columns=["Date","OT","OL","PDC","FCT","RS","Result"])

# ---- APIs ----
@app.get("/api/freq3")
async def freq3(
    symbol: str = Query(...),
    ot: str = Query(...),
    ol: str = Query(...),
    pdc: str = Query(...),
):
    if symbol not in ALLOWED_SYMBOLS:
        return JSONResponse({"error": f"Unknown symbol {symbol}"}, status_code=404)

    res = FIDX.query_three_tags(symbol, ot or "", ol or "", pdc or "")
    return JSONResponse(
        {
            "symbol": symbol,
            "tags": {"ot": ot, "ol": ol, "pdc": pdc},
            "level": res.level,
            "bull_n": res.bull_n,
            "bear_n": res.bear_n,
            "total": res.total,
            "gap_pp": round(res.gap_pp, 1),
            "pick": res.pick,
            "conf_pct": res.conf_pct,
            "reason": res.reason,
        }
    )
@app.get("/api/journal/health")
async def journal_health():
    info = {"root": str(ROOT)}
    if journal_mod is None:
        info["journal_import"] = f"FAILED: {JOURNAL_IMPORT_ERROR}"
        return JSONResponse(info)
    cfg = getattr(journal_mod, "CONFIG", {}) or {}
    latest_dir = cfg.get("LATEST_DIR") or str(ROOT / "data" / "latest")
    master_path = cfg.get("MASTER_DEFAULT_PATH") or str(ROOT / "data" / "masters" / "TataMotors_Master.csv")
    info.update({"LATEST_DIR": str(latest_dir), "MASTER_DEFAULT_PATH": str(master_path)})
    try:
        tradebook_path = Path(latest_dir) / "tradebook.csv"
        info["tradebook_exists"] = tradebook_path.exists()
        if tradebook_path.exists():
            info["tradebook_size"] = tradebook_path.stat().st_size
    except Exception as e:
        info["error"] = str(e)
    return JSONResponse(info)

@app.get("/api/superpath/ping")
async def superpath_ping():
    return {"ok": True}

@app.get("/api/matches")
async def matches(
    symbol: str = Query(...),
    ot: str = Query(...),
    ol: str = Query(...),
    pdc: str = Query(...),
):
    if symbol not in ALLOWED_SYMBOLS:
        return JSONResponse({"symbol": symbol, "rows": [], "dates": [], "error": "unknown symbol"}, status_code=404)

    df = MASTERS.get(symbol, pd.DataFrame())
    if df.empty:
        return JSONResponse({"symbol": symbol, "rows": [], "dates": []})

    ot = (ot or "").strip().upper()
    ol = (ol or "").strip().upper()
    pdc = (pdc or "").strip().upper()

    m = (df["OT"] == ot) & (df["OL"] == ol) & (df["PDC"] == pdc)
    sub = df.loc[m, ["Date","PDC","OL","OT","FCT","RS","Result"]].sort_values("Date")
    rows: List[Dict] = sub.to_dict(orient="records")
    dates = sub["Date"].dropna().unique().tolist()
    return JSONResponse({"symbol": symbol, "rows": rows, "dates": dates})

@app.get("/api/tm5")
async def tm5(symbol: str = Query(...)):
    if symbol not in ALLOWED_SYMBOLS:
        return JSONResponse({"error": f"Unknown symbol {symbol}"}, status_code=404)

    tm5_map = (CFG.get("tm5") or {})
    path = tm5_map.get(symbol)
    if not path:
        return JSONResponse({"error": f"No tm5 path configured for {symbol}"}, status_code=404)
    p = Path(path)
    if not p.exists():
        return JSONResponse({"error": f"tm5 file not found: {path}"}, status_code=404)

    try:
        df = pd.read_csv(p, sep=None, engine="python", encoding="utf-8-sig")
        norm = {str(c).strip().lower().replace("\ufeff",""): c for c in df.columns}
        def has(k): return k in norm
        def src(k): return norm[k]

        ren = {}
        # price
        if has("open"):  ren[src("open")]  = "Open"
        if has("high"):  ren[src("high")]  = "High"
        if has("low"):   ren[src("low")]   = "Low"
        if has("close"): ren[src("close")] = "Close"
        # datetime resolution
        dt_src = None
        for cand in ("datetime","date_time","timestamp"):
            if has(cand): dt_src = src(cand); break
        if dt_src:
            ren[dt_src] = "DateTime"
            if has("date"): ren[src("date")] = "Date"
        else:
            if has("date"): ren[src("date")] = "Date"

        df = df.rename(columns=ren)
        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()]

        # synthesize DateTime if needed
        if "DateTime" not in df.columns and "Date" in df.columns:
            lower = {str(c).strip().lower(): c for c in df.columns}
            tcol = lower.get("time") or lower.get("timestr")
            if tcol:
                df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df[tcol].astype(str), errors="coerce")
            else:
                df["DateTime"] = pd.to_datetime(df["Date"], errors="coerce")

        need = {"DateTime","Open","High","Low","Close"}
        miss = [c for c in need if c not in df.columns]
        if miss:
            return JSONResponse({"error": f"tm5 missing columns {miss} in {path}"}, status_code=422)

        df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
        df = df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime")
        # Safer for JS Date parsing across browsers (no timezone suffix → local time)
        df["DateTime"] = df["DateTime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        csv = df[["DateTime","Open","High","Low","Close"]].to_csv(index=False)
        return Response(content=csv, media_type="text/csv")
    except Exception as e:
        return JSONResponse({"error": f"tm5 read error for {symbol} at {path}: {e}"}, status_code=500)

def _tm5_df(symbol: str, cfg: dict) -> pd.DataFrame:
    tm5_map = (cfg.get("tm5") or {})
    path = tm5_map.get(symbol)
    if not path:
        raise FileNotFoundError(f"No tm5 path configured for {symbol}")
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    # normalize columns (same as /api/tm5)
    norm = {str(c).strip().lower().replace("\ufeff",""): c for c in df.columns}
    def has(k): return k in norm
    def src(k): return norm[k]
    ren = {}
    if has("open"):  ren[src("open")]  = "Open"
    if has("high"):  ren[src("high")]  = "High"
    if has("low"):   ren[src("low")]   = "Low"
    if has("close"): ren[src("close")] = "Close"
    dt_src = None
    for cand in ("datetime","date_time","timestamp"):
        if has(cand): dt_src = src(cand); break
    if dt_src:
        ren[dt_src] = "DateTime"
        if has("date"): ren[src("date")] = "Date"
    else:
        if has("date"): ren[src("date")] = "Date"
    df = df.rename(columns=ren)
    if "DateTime" not in df.columns and "Date" in df.columns:
        lower = {str(c).strip().lower(): c for c in df.columns}
        tcol = lower.get("time") or lower.get("timestr")
        if tcol:
            df["DateTime"] = pd.to_datetime(df["Date"].astype(str) + " " + df[tcol].astype(str), errors="coerce")
        else:
            df["DateTime"] = pd.to_datetime(df["Date"], errors="coerce")
    df["DateTime"] = pd.to_datetime(df["DateTime"], errors="coerce")
    # force tz-naive for consistent math
    try:
        df["DateTime"] = df["DateTime"].dt.tz_convert(None)
    except Exception:
        df["DateTime"] = df["DateTime"].dt.tz_localize(None)

    # force tz-naive for consistent math downstream
    try:
        df["DateTime"] = df["DateTime"].dt.tz_convert(None)
    except Exception:
        df["DateTime"] = df["DateTime"].dt.tz_localize(None)

    df = df.dropna(subset=["DateTime","Open","High","Low","Close"]).sort_values("DateTime")
    df["Date"] = df["DateTime"].dt.normalize()
    # precompute minutes for fast slicing
    df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute
    return df

def _slice(df_day: pd.DataFrame, m0: int, m1: int) -> pd.DataFrame:
    if df_day is None or df_day.empty: return pd.DataFrame()
    m = (df_day["_mins"] >= m0) & (df_day["_mins"] <= m1)
    return df_day.loc[m, ["DateTime","Open","High","Low","Close","Date"]]

@app.get("/api/superpath")
async def superpath(
    symbol: str = Query(...),
    ot: str = Query(...),
    ol: str = Query(...),
    pdc: str = Query(...),
):
    stage = "init"
    try:
        import numpy as np

        # -------- 1) match masters --------
        stage = "load_masters"
        dfm = MASTERS.get(symbol, pd.DataFrame())
        if dfm.empty:
            return JSONResponse({"bars": [], "cone": [], "meta": {"N": 0}, "stage": stage})

        ot = (ot or "").strip().upper()
        ol = (ol or "").strip().upper()
        pdc = (pdc or "").strip().upper()

        for col in ("OT", "OL", "PDC", "Date"):
            if col not in dfm.columns:
                return JSONResponse(
                    {"bars": [], "cone": [], "meta": {"N": 0}, "error": f"masters missing '{col}'", "stage": stage}
                )

        stage = "filter_matches"
        m = (dfm["OT"] == ot) & (dfm["OL"] == ol) & (dfm["PDC"] == pdc)
        sub = dfm.loc[m, ["Date"]].dropna().copy()
        if sub.empty:
            return JSONResponse({"bars": [], "cone": [], "meta": {"N": 0}, "stage": stage})

        stage = "normalize_dates"
        sub["Date"] = pd.to_datetime(sub["Date"], errors="coerce").dt.normalize()
        dates = [d for d in sub["Date"].dropna().unique().tolist() if pd.notna(d)]
        if not dates:
            return JSONResponse({"bars": [], "cone": [], "meta": {"N": 0}, "stage": stage})

        # -------- 2) load tm5 & group --------
        stage = "load_tm5"
        tf = _tm5_df(symbol, CFG)

        stage = "group_by_day"
        by_day = {d: g.sort_values("DateTime").reset_index(drop=True) for d, g in tf.groupby("Date")}

        # trade window only: 09:40 → 15:05
        T0_M, T1_M = 9*60 + 40, 15*60 + 5

        # -------- 3) build return paths + weights --------
        stage = "build_paths"
        paths = []
        path_dates = []
        weights = []

        # make 'now' tz-naive to avoid "tz-aware vs tz-naive" subtraction
        now = pd.Timestamp.utcnow().tz_localize(None).normalize()

        for d in dates:
            day = pd.to_datetime(d).normalize()
            dfd = by_day.get(day)
            if dfd is None or dfd.empty:
                continue

            win = _slice(dfd, T0_M, T1_M)
            if win.empty:
                continue

            base = float(win["Open"].iloc[0])
            closes = win["Close"].astype(float).to_numpy()
            if not (np.isfinite(base) and base > 0 and closes.size > 0):
                continue

            r = 100.0 * (closes - base) / base  # % move path from 09:40 open
            paths.append(r)
            path_dates.append(day)

            # recency weight (half-life 60d)
            age_days = max(0.0, float((now - day).days))
            weights.append(exp(-LAMBDA * age_days))

        if not paths:
            return JSONResponse({"bars": [], "cone": [], "meta": {"N": 0}, "stage": stage})

        # -------- 4) align arrays --------
        stage = "align_arrays"
        L = int(min(len(p) for p in paths))
        if L <= 0:
            return JSONResponse({"bars": [], "cone": [], "meta": {"N": 0}, "stage": stage})

        P = np.vstack([p[:L] for p in paths])           # shape: [N, L]
        w = np.array(weights, dtype=float)[:P.shape[0]] # [N]
        s = float(w.sum())
        if not np.isfinite(s) or s <= 0:
            w = np.ones_like(w) / max(1.0, float(w.shape[0]))
        else:
            w = w / s
        Wcol = w.reshape(-1, 1)                         # [N,1]

        # Weighted mean/sd (keep for backward compatibility)
        mu = (Wcol * P).sum(axis=0)
        var = (Wcol * (P - mu)**2).sum(axis=0)
        sd = np.sqrt(np.maximum(var, 0.0))

        # Robust cone: (median, p25, p75)
        med = np.nanmedian(P, axis=0)
        p25 = np.nanpercentile(P, 25, axis=0)
        p75 = np.nanpercentile(P, 75, axis=0)

        # -------- 5) stats on structure --------
        stage = "stats"

        # slope on median path (per 5-minute step)
        x = np.arange(L, dtype=float)
        slope = float(np.polyfit(x, med, 1)[0]) if L >= 2 else 0.0
        # visual angle (scale=1 keeps angle comparable between sets)
        angle_deg = float(np.degrees(np.arctan(slope)))

        # end dispersion (robust) & bias
        end_med = float(med[-1]) if L else 0.0
        end_iqr = float(np.percentile(P[:, -1], 75) - np.percentile(P[:, -1], 25)) if P.shape[0] else 0.0
        bias = "BULL" if end_med > 0 else ("BEAR" if end_med < 0 else "NEUTRAL")

        # consistency: % days with final sign + time-in-sign
        Pend = P[:, -1]
        sign_end = np.sign(Pend)
        end_pos = float((Pend > 0).mean()) if P.shape[0] else 0.0

        time_in_sign = []
        for i in range(P.shape[0]):
            sgn = np.sign(Pend[i])
            if sgn == 0:
                time_in_sign.append(0.0)
                continue
            hits = np.mean(np.sign(P[i, :]) == sgn)
            time_in_sign.append(float(hits))
        time_in_sign = float(np.mean(time_in_sign)) if time_in_sign else 0.0
        consistency = 0.6 * end_pos + 0.4 * time_in_sign  # 0..1

        # whipsaw/smoothness via zero crossings (lower crossings → smoother)
        xings = []
        for i in range(P.shape[0]):
            r = P[i, :]
            xings.append(float(np.sum(r[:-1] * r[1:] < 0)))
        if xings:
            xc_rate = np.median(xings) / max(1.0, L - 1)
            smoothness = float(max(0.0, 1.0 - xc_rate))  # 0..1
        else:
            smoothness = 0.0

        # early alignment (09:40→10:30 ≈ first 10 bars)
        early_idx = min(10, L - 1)
        early = P[:, early_idx] if L > 1 else Pend
        if np.std(early) > 1e-9 and np.std(Pend) > 1e-9:
            early_corr = float(np.corrcoef(early, Pend)[0, 1])
        else:
            early_corr = 0.0

        # effective sample & recency share
        Neff = float(1.0 / np.sum(w**2)) if w.size else 0.0
        six_months_ago = now - pd.Timedelta(days=182)
        recent_mask = np.array([(d >= six_months_ago) for d in path_dates], dtype=bool)
        recent_share = float((w * recent_mask).sum())

        # TP-before-SL (simple 1:2 RR proxy using % thresholds)
        TP = 1.0   # +1.0%
        SL = -0.5  # -0.5%
        tp_hits = []
        ttp_minutes = []
        mae_list, mfe_list = [], []

        for i in range(P.shape[0]):
            r = P[i, :]
            # first-passage
            hit_tp = None
            for k, val in enumerate(r):
                if val >= TP:
                    hit_tp = ("TP", k)
                    break
                if val <= SL:
                    hit_tp = ("SL", k)
                    break
            if hit_tp is None:
                tp_hits.append(0.5)  # count as neutral half
            else:
                tp_hits.append(1.0 if hit_tp[0] == "TP" else 0.0)
                if hit_tp[0] == "TP":
                    ttp_minutes.append(int(hit_tp[1]) * 5)

            mae_list.append(float(np.min(r)))
            mfe_list.append(float(np.max(r)))

        pr_tp_first = float(np.mean(tp_hits)) if tp_hits else 0.5
        ttp_med = int(np.median(ttp_minutes)) if ttp_minutes else None
        mae_med = float(np.median(mae_list)) if mae_list else 0.0
        mfe_med = float(np.median(mfe_list)) if mfe_list else 0.0

        # confidence blend (bounded 0..100)
        # robust 'Z' at close using IQR
        Z_end = 0.0
        if end_iqr > 1e-6:
            Z_end = min(5.0, abs(end_med) / end_iqr)  # cap to avoid runaway
        Z_end = 20.0 * Z_end  # scale 0..100

        Cons = 100.0 * consistency
        Smooth = 100.0 * smoothness
        TPedge = 100.0 * max(0.0, (pr_tp_first - 0.5) * 2.0)  # 50%→0, 75%→50, 100%→100
        Recency = 100.0 * min(1.0, Neff / 50.0) * max(0.0, min(1.0, recent_share))

        conf = 0.30 * Z_end + 0.20 * Cons + 0.15 * Smooth + 0.25 * TPedge + 0.10 * Recency
        conf = int(round(max(0.0, min(100.0, conf))))

        # labels
        stage = "labels"
        times = []
        m0 = 40  # minutes from 09:00 (09:40)
        for i in range(L):
            mm = 9*60 + m0 + 5*i
            hh, mn = divmod(mm, 60)
            times.append(f"{hh:02d}:{mn:02d}")

        # payloads
        bars = [{"t": times[i], "mean": round(float(mu[i]), 4), "std": round(float(sd[i]), 4)} for i in range(L)]
        cone = [{"t": times[i], "med": round(float(med[i]), 4), "p25": round(float(p25[i]), 4), "p75": round(float(p75[i]), 4)} for i in range(L)]

        meta = {
            "N": int(P.shape[0]),
            "N_eff": round(Neff, 1),
            "recent_share": round(100.0 * recent_share, 1),
            "bias": bias,
            "end_median": round(end_med, 3),
            "end_iqr": round(end_iqr, 3),
            "slope_per_bar": round(slope, 4),
            "angle_deg": round(angle_deg, 1),
            "consistency_pct": int(round(100.0 * consistency)),
            "smoothness_pct": int(round(100.0 * smoothness)),
            "early_corr": round(early_corr, 2),
            "tp_first_pct": int(round(100.0 * pr_tp_first)),
            "ttp_median_min": ttp_med,
            "mae_med": round(mae_med, 3),
            "mfe_med": round(mfe_med, 3),
            "half_life_days": int(HALF_LIFE_DAYS),
            "confidence": conf,
        }

        return JSONResponse({"bars": bars, "cone": cone, "meta": meta, "stage": "ok"})

    except Exception as e:
        return JSONResponse(
            {"bars": [], "cone": [], "meta": {"N": 0}, "error": f"{type(e).__name__}: {e}", "stage": stage},
            status_code=200,
        )



@app.get("/api/journal/daily")
async def journal_daily():
    """
    Daily journal view for the JS Journal page.

    Uses the same logic as the Streamlit journal:
      parse_tradebook_csv  → daily P&L from tradebook.csv
      load_master          → tags from master CSV
      merge_and_process_data → join + compute pnl_final & day_R_net
    """
    # 0) If the module import failed, surface that cleanly
    if journal_mod is None:
        return JSONResponse(
            {
                "rows": [],
                "error": f"journal_tab import failed: {JOURNAL_IMPORT_ERROR or 'unknown error'}",
            },
            status_code=200,
        )

    try:
        # 1) Derive paths in the SAME way as journal_tab

        # journal_tab.CONFIG is already env-aware:
        #   LATEST_DIR  → where tradebook.csv lives
        #   MASTER_DEFAULT_PATH → master tags CSV
        cfg = getattr(journal_mod, "CONFIG", {}) or {}

        # Resolve to absolute paths with safe fallbacks
        latest_dir_cfg = cfg.get("LATEST_DIR")
        master_path_cfg = cfg.get("MASTER_DEFAULT_PATH")
        
        latest_dir = Path(latest_dir_cfg) if latest_dir_cfg else (ROOT / "data" / "latest")
        if not latest_dir.is_absolute():
            latest_dir = (ROOT / latest_dir).resolve()
        
        master_path = Path(master_path_cfg) if master_path_cfg else (ROOT / "data" / "masters" / "TataMotors_Master.csv")
        if not master_path.is_absolute():
            master_path = (ROOT / master_path).resolve()
        risk_unit = float(cfg.get("RISK_UNIT", 10000))

        tradebook_path = latest_dir / "tradebook.csv"

        if not tradebook_path.exists():
            return JSONResponse(
                {
                    "rows": [],
                    "error": f"tradebook.csv not found at {tradebook_path}",
                },
                status_code=200,
            )

        # 2) Reuse the SAME helpers as Streamlit
        tb_all = journal_mod.parse_tradebook_csv(tradebook_path)
        if tb_all is None or tb_all.empty:
            return JSONResponse(
                {
                    "rows": [],
                    "error": "No valid trades parsed from tradebook.csv",
                },
                status_code=200,
            )

        master = journal_mod.load_master(str(master_path))
        j_all = journal_mod.merge_and_process_data(master, tb_all, risk_unit)

        if j_all is None or j_all.empty:
            return JSONResponse(
                {
                    "rows": [],
                    "error": "merge_and_process_data returned an empty journal",
                },
                status_code=200,
            )

        # --- HARD NORMALIZATION for frontend ---
        j_all = j_all.copy()

        tb_rows = int(len(tb_all))
        master_rows = int(len(master))
        j_rows = int(len(j_all))

        if j_rows == 0:
            return JSONResponse(
                {"rows": [], "error": f"Empty journal after merge (tb={tb_rows}, master={master_rows}, j={j_rows}). Check symbol/date alignment and CSV headers."},
                status_code=200,
            )



        # Ensure ISO date strings (YYYY-MM-DD)
        for col in ("Date", "trade_date"):
            if col in j_all.columns:
                j_all[col] = pd.to_datetime(j_all[col], errors="coerce").dt.strftime("%Y-%m-%d")

        # Ensure symbol_std is consistent
        if "symbol_std" in j_all.columns:
            j_all["symbol_std"] = j_all["symbol_std"].astype(str).str.upper().str.strip()

        # Fill tag columns to avoid undefineds in UI
        for tag in ["PrevDayContext","GapType","OpenLocation","FirstCandleType","OpeningTrend","RangeStatus","Result"]:
            if tag in j_all.columns:
                j_all[tag] = j_all[tag].fillna("").astype(str).str.strip()

        # ---- SANITIZE for JSON (NaN/Inf → None) ----
        # Coerce numerics (these commonly appear), then replace ±Inf with NaN, then NaN → None
        num_cols = ["pnl_final","day_R_net","trades_n","buy_value","sell_value","session_ok"]
        for c in num_cols:
            if c in j_all.columns:
                j_all[c] = pd.to_numeric(j_all[c], errors="coerce")

        j_all.replace([np.inf, -np.inf], np.nan, inplace=True)
        j_all = j_all.where(pd.notna(j_all), None)

        # 3) Serialize for JS
        rows = j_all.to_dict(orient="records")
        return JSONResponse({"rows": rows}, status_code=200)


    except Exception as e:
        # If anything unexpected happens, send the error string to the frontend
        return JSONResponse(
            {
                "rows": [],
                "error": f"journal build failed: {e}",
            },
            status_code=200,
        )

@app.get("/api/journal/debug")
async def journal_debug():
    if journal_mod is None:
        return JSONResponse({"ok": False, "stage": "import", "error": JOURNAL_IMPORT_ERROR or "import failed"}, status_code=200)
    try:
        cfg = getattr(journal_mod, "CONFIG", {}) or {}
        latest_dir = Path(cfg.get("LATEST_DIR") or (ROOT / "data" / "latest"))
        if not latest_dir.is_absolute(): latest_dir = (ROOT / latest_dir).resolve()
        master_path = Path(cfg.get("MASTER_DEFAULT_PATH") or (ROOT / "data" / "masters" / "TataMotors_Master.csv"))
        if not master_path.is_absolute(): master_path = (ROOT / master_path).resolve()
        risk_unit = float(cfg.get("RISK_UNIT", 10000))
        tradebook_path = latest_dir / "tradebook.csv"

        info = {
            "LATEST_DIR": str(latest_dir),
            "MASTER_DEFAULT_PATH": str(master_path),
            "tradebook_exists": tradebook_path.exists(),
            "tradebook_size": tradebook_path.stat().st_size if tradebook_path.exists() else 0,
        }
        if not tradebook_path.exists():
            info["ok"] = False
            info["stage"] = "missing_tradebook"
            return JSONResponse(info, status_code=200)

        tb_all = journal_mod.parse_tradebook_csv(tradebook_path)
        info["tb_rows"] = int(len(tb_all))
        if not tb_all.empty:
            info["tb_dates_min"] = str(tb_all["trade_date"].min())
            info["tb_dates_max"] = str(tb_all["trade_date"].max())
            info["tb_symbols"] = sorted(list(set(tb_all["symbol_std"].dropna().astype(str))))
            info["tb_sample"] = tb_all.head(3).to_dict(orient="records")

        master = journal_mod.load_master(str(master_path))
        info["master_rows"] = int(len(master))
        if not master.empty:
            info["master_dates_min"] = str(master["Date"].min())
            info["master_dates_max"] = str(master["Date"].max())
            info["master_symbols"] = sorted(list(set(master["symbol_std"].dropna().astype(str))))
            info["master_sample"] = master.head(3).to_dict(orient="records")

        j_all = journal_mod.merge_and_process_data(master, tb_all, risk_unit)
        info["journal_rows"] = int(len(j_all))
        if not j_all.empty:
            info["journal_dates_min"] = str(j_all["Date"].min())
            info["journal_dates_max"] = str(j_all["Date"].max())
            info["journal_symbols"] = sorted(list(set(j_all["symbol_std"].dropna().astype(str))))
            info["journal_sample"] = j_all.head(3).to_dict(orient="records")

        info["ok"] = True
        info["stage"] = "done"
        return JSONResponse(info, status_code=200)
    except Exception as e:
        return JSONResponse({"ok": False, "stage": "exception", "error": f"{type(e).__name__}: {e}"}, status_code=200)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9010, reload=False)
