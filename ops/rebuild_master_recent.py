from pathlib import Path
import pandas as pd
from probedge.infra.settings import SETTINGS
from probedge.decision.tags_engine import _read_intraday, _read_master
from probedge.core import classifiers as C

ROOT = Path(__file__).resolve().parents[1]
MAST = ROOT / "data" / "masters"
MAST.mkdir(parents=True, exist_ok=True)

N_DAYS = 120

for s in SETTINGS.symbols:
    i = _read_intraday(s)
    if i.empty:
        print(f"[{s}] intraday empty, skip"); continue
    days = sorted(pd.to_datetime(i["Date"].unique()))[-N_DAYS:]
    by_day = {d: g.sort_values("DateTime") for d,g in i.groupby("Date")}
    rows=[]
    for d in days:
        prev = C.prev_trading_day_ohlc(i, d)
        pdc  = C.compute_prevdaycontext_robust(prev)
        ol   = C.compute_openlocation_from_df(i, d, prev)
        ot   = C.compute_openingtrend_robust(i, d)
        lab,_= C.compute_result_0940_1505(by_day.get(d, pd.DataFrame()))
        rows.append({"Date":d.normalize(), "OpeningTrend":ot, "OpenLocation":ol, "PrevDayContext":pdc, "Result":lab})
    add = pd.DataFrame(rows)
    m = _read_master(s)
    out = add if m.empty else (
        pd.concat([m[~pd.to_datetime(m["Date"]).isin(add["Date"])], add], ignore_index=True)
          .sort_values("Date").reset_index(drop=True)
    )
    path = MAST / f"{s}_5MINUTE_MASTER.csv"
    out.to_csv(path, index=False)
    print(f"[{s}] master rebuilt {len(add)} rows → {path}")
