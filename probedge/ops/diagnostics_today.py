import json
from pathlib import Path
import pandas as pd
from probedge.infra.settings import SETTINGS
from probedge.decision.tags_engine import _read_intraday, _read_master

st = json.loads(Path("data/state/live_state.json").read_text())
d = pd.to_datetime(st["date"])
print("STATE DATE:", d.date())

def cnt(i, m0, m1):
    w=i[i["Date"]==d]
    if w.empty: return 0
    mins=w["DateTime"].dt.hour*60+w["DateTime"].dt.minute
    return int(((mins>=m0)&(mins<=m1)).sum())

for s in SETTINGS.symbols:
    i=_read_intraday(s)
    print(f"{s}: ORB={cnt(i,9*60+15,9*60+35)}, Trade={cnt(i,9*60+40,15*60+5)}")

rows=[]
for s in SETTINGS.symbols:
    m=_read_master(s); r=m.loc[m["Date"]==d]
    PDCm = r["PrevDayContext"].iloc[0] if not r.empty else None
    OLm  = r["OpenLocation"].iloc[0] if not r.empty else None
    OTm  = r["OpeningTrend"].iloc[0] if not r.empty else None
    C = st["tags"].get(s, {})
    rows.append([s, C.get("PDC"), C.get("OL"), C.get("OT"), PDCm, OLm, OTm])
print(pd.DataFrame(rows, columns=["Symbol","PDC_calc","OL_calc","OT_calc","PDC_master","OL_master","OT_master"]))
