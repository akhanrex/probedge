import json
from pathlib import Path
import pandas as pd

# Imports from our codebase
from probedge.decision.tags_engine import _read_intraday, _read_master
import probedge.core.classifiers as C

ROOT = Path(".")
SYMBOL_MAP = json.loads((ROOT / "config/symbol_map.json").read_text())
OUT_DIR = ROOT / "data/masters"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def ensure_date_col(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee a normalized Date column exists (timezone-naive, yyyy-mm-dd)."""
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    else:
        # If missing entirely, create an empty Date column
        df = df.copy()
        df["Date"] = pd.Series(dtype="datetime64[ns]")
    return df

def safe_existing_master(symbol: str, add_cols) -> pd.DataFrame:
    """Read master; if bad/missing, return empty frame with right columns."""
    try:
        m = _read_master(symbol)
        m = ensure_date_col(m)
        if "Date" not in m.columns:
            m = pd.DataFrame(columns=list(add_cols))
        return m
    except Exception:
        return pd.DataFrame(columns=list(add_cols))

def main():
    for s in SYMBOL_MAP.keys():
        i = _read_intraday(s)
        # If no intraday, write an empty master skeleton and continue
        if i is None or i.empty or "Date" not in i.columns:
            out = pd.DataFrame(columns=["Date","OpeningTrend","OpenLocation","PrevDayContext","Result"])
            (OUT_DIR / f"{s}_5MINUTE_MASTER.csv").write_text(out.to_csv(index=False))
            print(f"[{s}] master rows now = 0 (no intraday)")
            continue

        # Last 120 trading dates in this intraday file
        days = sorted(pd.to_datetime(i["Date"].unique()))[-120:]
        by_day = {d: g.sort_values("DateTime") for d, g in i.groupby("Date")}

        rows = []
        for d in days:
            try:
                prev = C.prev_trading_day_ohlc(i, d)
                pdc  = C.compute_prevdaycontext_robust(prev)
                ol   = C.compute_openlocation_from_df(i, d, prev)
                ot   = C.compute_openingtrend_robust(i, d)
                lab,_= C.compute_result_0940_1505(by_day.get(d, pd.DataFrame()))
                rows.append({
                    "Date": pd.to_datetime(d).normalize(),
                    "OpeningTrend": ot,
                    "OpenLocation": ol,
                    "PrevDayContext": pdc,
                    "Result": lab
                })
            except Exception as e:
                print(f"[{s}] {pd.to_datetime(d).date()} ERR {e}")

        add = pd.DataFrame(rows, columns=["Date","OpeningTrend","OpenLocation","PrevDayContext","Result"])
        add = ensure_date_col(add)

        # Existing master (safe)
        m = safe_existing_master(s, add.columns)

        if not m.empty and "Date" in m.columns:
            keep = m[~m["Date"].isin(add["Date"])]
            out = pd.concat([keep, add], ignore_index=True)
        else:
            out = add

        out = out.sort_values("Date").reset_index(drop=True)
        # Write
        path = OUT_DIR / f"{s}_5MINUTE_MASTER.csv"
        path.write_text(out.to_csv(index=False))
        print(f"[{s}] master rows now = {len(out)} → {path}")

if __name__ == "__main__":
    main()
