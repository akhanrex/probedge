import pandas as pd
from pathlib import Path

from probedge.infra.settings import SETTINGS
from probedge.decision.tags_engine import _read_intraday, _read_master
from probedge.core import classifiers as C

# MASTER dir (same convention as rest of repo)
MAST = Path(getattr(SETTINGS.paths, "master", "data/masters"))
MAST.mkdir(parents=True, exist_ok=True)

# How many recent trading days to rebuild for each symbol
N_DAYS = 120

for s in SETTINGS.symbols:
    sym = s.upper()
    print(f"\n=== {sym} ===")

    # --- Intraday (TM5) ---
    i = _read_intraday(sym)
    if i.empty:
        print(f"[{sym}] intraday empty; skip")
        continue

    # Normalise Date to tz-naive date
    if "Date" not in i.columns:
        # some loaders store DateTime only
        i["Date"] = pd.to_datetime(i["DateTime"], errors="coerce").dt.tz_localize(None).dt.normalize()
    else:
        i["Date"] = pd.to_datetime(i["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()

    # --- Existing MASTER ---
    m = _read_master(sym)
    if not m.empty and "Date" in m.columns:
        m["Date"] = pd.to_datetime(m["Date"], errors="coerce").dt.tz_localize(None).dt.normalize()

    # Last N trading days from intraday
    days = sorted(i["Date"].dropna().unique())[-N_DAYS:]
    if not len(days):
        print(f"[{sym}] no days found in intraday; skip")
        continue

    by_day = {d: g.sort_values("DateTime") for d, g in i.groupby("Date")}

    rows = []
    for d in days:
        try:
            # Prev-day OHLC, tags + result using the SAME classifiers as backtest
            prev = C.prev_trading_day_ohlc(i, d)
            pdc = C.compute_prevdaycontext_robust(prev)
            ol = C.compute_openlocation_from_df(i, d, prev)
            ot = C.compute_openingtrend_robust(i, d)
            lab, _ = C.compute_result_0940_1505(by_day.get(d, pd.DataFrame()))

            rows.append(
                {
                    "Date": d,
                    "OpeningTrend": ot,
                    "OpenLocation": ol,
                    "PrevDayContext": pdc,
                    "Result": lab,
                }
            )
        except Exception as e:
            print(f"[{sym}] {pd.Timestamp(d).date()} ERR {e}")

    add = pd.DataFrame(rows)
    if add.empty:
        print(f"[{sym}] no new rows built; skip write")
        continue

    # Merge with existing MASTER: keep old rows where Date not in "add", then append "add"
    if m.empty:
        out = add.copy()
    else:
        out = pd.concat(
            [m[~m["Date"].isin(add["Date"])], add],
            ignore_index=True,
        ).sort_values("Date").reset_index(drop=True)

    # Ensure Date is written as 'YYYY-MM-DD' (like your existing masters)
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Final column order: keep whatever exists, adding any new tag columns if needed
    # (pandas concat already handles this; we just write out)
    dest = MAST / f"{sym}_5MINUTE_MASTER.csv"
    out.to_csv(dest, index=False)
    print(f"[{sym}] master rebuilt/updated: added {len(add)} rows → {dest}")
