import pandas as pd
from pathlib import Path
from probedge.infra.settings import SETTINGS
from probedge.decision.tags_engine import _read_intraday, _read_master
from probedge.core import classifiers as C

MAST = Path(getattr(SETTINGS.paths, "master", "data/masters")); MAST.mkdir(parents=True, exist_ok=True)

for s in SETTINGS.symbols:
    i=_read_intraday(s)
    if i.empty:
        print(f"[{s}] intraday empty; skip"); continue
    i["Date"]=pd.to_datetime(i["Date"]).dt.tz_localize(None)
    m=_read_master(s)
    if not m.empty: m["Date"]=pd.to_datetime(m["Date"]).dt.tz_localize(None)

    days = sorted(i["Date"].unique())[-120:]
    by_day = {d: g.sort_values("DateTime") for d,g in i.groupby("Date")}
    rows=[]
    for d in days:
        try:
            prev = C.prev_trading_day_ohlc(i, d)
            pdc  = C.compute_prevdaycontext_robust(prev)
            ol   = C.compute_openlocation_from_df(i, d, prev)
            ot   = C.compute_openingtrend_robust(i, d)
            lab,_= C.compute_result_0940_1505(by_day.get(d, pd.DataFrame()))
            rows.append({"Date":d, "OpeningTrend":ot, "OpenLocation":ol, "PrevDayContext":pdc, "Result":lab})
        except Exception as e:
            print(f"[{s}] {d.date()} ERR {e}")

    add = pd.DataFrame(rows)
    out = add if m.empty else (
        pd.concat([m[~m["Date"].isin(add["Date"])], add], ignore_index=True)
          .sort_values("Date").reset_index(drop=True)
    )
    out.to_csv(MAST / f"{s}_5MINUTE_MASTER.csv", index=False)
    print(f"[{s}] master rebuilt {len(add)} rows")
