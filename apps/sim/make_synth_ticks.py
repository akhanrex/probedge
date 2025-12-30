# apps/sim/make_synth_ticks.py
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

def _parse_days(days_arg: str) -> List[str]:
    # accepts: "2025-12-12" or "2025-12-12,2025-12-13"
    return [x.strip() for x in str(days_arg).split(",") if x.strip()]

def _find_intraday_files(src_data_dir: Path) -> Dict[str, Path]:
    intr = src_data_dir / "data" / "intraday"
    files = sorted(intr.glob("*_5minute.csv"))
    out = {}
    for fp in files:
        trad = fp.name.replace("_5minute.csv", "").upper()
        out[trad] = fp
    return out

def _load_tm5_flex(fp: Path) -> pd.DataFrame:
    df = pd.read_csv(fp)
    df.columns = [str(c).strip() for c in df.columns]
    # normalize lower-case lookup
    lower = {c.lower(): c for c in df.columns}

    # build dt_ist
    if "datetime" in lower:
        dt = pd.to_datetime(df[lower["datetime"]], errors="coerce")
    elif ("date" in lower) and ("time" in lower):
        dt = pd.to_datetime(
            df[lower["date"]].astype(str).str.strip() + " " + df[lower["time"]].astype(str).str.strip(),
            errors="coerce"
        )
    elif "date" in lower:
        dt = pd.to_datetime(df[lower["date"]], errors="coerce")
    else:
        # last resort: first column
        dt = pd.to_datetime(df.iloc[:, 0], errors="coerce")

    # localize/convert to IST
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize(IST)
    else:
        dt = dt.dt.tz_convert(IST)

    df["_dt_ist"] = dt
    return df

def _get_col(df: pd.DataFrame, name: str) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    return lower.get(name.lower())

def _ticks_from_bar(o: float, h: float, l: float, c: float, n: int, rng: np.random.Generator) -> List[float]:
    out = []
    span = max(0.0, float(h) - float(l))
    for i in range(n):
        a = (i / (n - 1)) if n > 1 else 1.0
        base = float(o) + (float(c) - float(o)) * a
        noise = rng.normal(0.0, 0.10 * span) if span > 0 else 0.0
        px = base + noise
        if span > 0:
            px = min(float(h), max(float(l), px))
        out.append(float(px))
    return out

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-data-dir", required=True)
    ap.add_argument("--out-data-dir", required=True)
    ap.add_argument("--days", required=True, help="YYYY-MM-DD or comma-separated list")
    ap.add_argument("--tick-seconds", type=int, default=10, help="tick step in seconds (e.g., 10 => 30 ticks/bar)")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    src = Path(args.src_data_dir).expanduser().resolve()
    out = Path(args.out_data_dir).expanduser().resolve()
    days = _parse_days(args.days)

    tick_seconds = int(args.tick_seconds)
    if tick_seconds <= 0 or tick_seconds > 300:
        raise SystemExit("tick-seconds must be in 1..300")
    ticks_per_bar = int(300 // tick_seconds)
    if ticks_per_bar <= 0:
        raise SystemExit("tick-seconds too large")

    rng = np.random.default_rng(int(args.seed))

    intr_files = _find_intraday_files(src)
    if not intr_files:
        raise SystemExit(f"No intraday *_5minute.csv files under {src}/data/intraday")

    trad_symbols = sorted(intr_files.keys())
    # If both exist, TMPV is the truth; drop logical duplicate to avoid double ticks.
    if "TMPV" in intr_files and "TATAMOTORS" in intr_files:
        intr_files.pop("TATAMOTORS", None)

    sym_offset = {s: i for i, s in enumerate(trad_symbols)}  # tiny ordering offset

    written = 0
    for day in days:
        out_day_dir = out / "data" / "ticks" / day
        out_day_dir.mkdir(parents=True, exist_ok=True)

        for trad, fp in intr_files.items():
            df = _load_tm5_flex(fp)
            df = df.dropna(subset=["_dt_ist"]).sort_values("_dt_ist").reset_index(drop=True)

            # keep only requested day
            day_norm = pd.to_datetime(day).tz_localize(IST).normalize()
            df = df[df["_dt_ist"].dt.normalize() == day_norm].copy()
            if df.empty:
                continue

            # If time got lost (all midnight), reconstruct 09:15, 09:20, ...
            if (df["_dt_ist"].dt.hour.eq(0) & df["_dt_ist"].dt.minute.eq(0) & df["_dt_ist"].dt.second.eq(0)).all():
                base = pd.to_datetime(f"{day} 09:15:00").tz_localize(IST)
                df["_dt_ist"] = [base + pd.Timedelta(minutes=5*i) for i in range(len(df))]

            col_o = _get_col(df, "open")
            col_h = _get_col(df, "high")
            col_l = _get_col(df, "low")
            col_c = _get_col(df, "close")
            col_v = _get_col(df, "volume")

            if not all([col_o, col_h, col_l, col_c]):
                raise SystemExit(f"{fp} missing OHLC columns")

            rows = []
            off = sym_offset.get(trad, 0) * 0.001  # <= few ms, avoids dt=0 ties across symbols

            for r_i, r in enumerate(df.itertuples(index=False)):
                # Build IST wall-time using row "time" and force the requested SIM day
                t = str(getattr(r, "time", "")).strip()
                if not t:
                    raise RuntimeError("Row missing time column; expected schema date,time,open,high,low,close,volume")
                dt0 = datetime.fromisoformat(f"{day} {t}").replace(tzinfo=IST)
                # dt0 is tz-aware IST
                o = float(getattr(r, col_o))
                h = float(getattr(r, col_h))
                l = float(getattr(r, col_l))
                c = float(getattr(r, col_c))
                v = int(getattr(r, col_v)) if col_v else 0
                per_vol = int(v / ticks_per_bar) if v > 0 else 0

                prices = _ticks_from_bar(o, h, l, c, ticks_per_bar, rng)
                for j, px in enumerate(prices):
                    t = dt0 + pd.Timedelta(seconds=j * tick_seconds + off)
                    rows.append((float(t.timestamp()), trad, float(px), int(per_vol)))

            if not rows:
                continue

            out_df = pd.DataFrame(rows, columns=["ts_epoch", "symbol", "ltp", "vol"])
            out_df.to_parquet(out_day_dir / f"{trad}.parquet", index=False)
            written += 1

        # manifest
        man = {
            "day": day,
            "tick_seconds": tick_seconds,
            "ticks_per_bar": ticks_per_bar,
            "seed": int(args.seed),
            "src_data_dir": str(src),
            "out_data_dir": str(out),
            "symbols_written": sorted([p.stem for p in out_day_dir.glob("*.parquet")]),
        }
        (out / "data" / "ticks" / "_synth_manifest.json").write_text(json.dumps(man, indent=2), encoding="utf-8")

    print(f"✅ Synthetic ticks created under: {out}/data/ticks")
    print(f"Days: {days}")
    print(f"Tick step: {tick_seconds}s | ticks/bar: {ticks_per_bar}")
    print(f"Manifest: {out}/data/ticks/_synth_manifest.json")

if __name__ == "__main__":
    main()
