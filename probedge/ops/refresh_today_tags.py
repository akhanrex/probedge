import json, pandas as pd
from pathlib import Path
from probedge.infra.settings import SETTINGS
from probedge.decision.tags_engine import _read_intraday
from probedge.core import classifiers as C

SYMS = SETTINGS.symbols
today = pd.Timestamp.today(tz='Asia/Kolkata').normalize().tz_localize(None)
tags={}
rows=[]

for s in SYMS:
    i=_read_intraday(s)
    i["Date"]=pd.to_datetime(i["Date"]).dt.tz_localize(None)
    if today not in set(i["Date"].unique()):
        tags[s]={"PDC":"GAP","OL":"GAP","OT":"GAP"}
        rows.append([s,"MISS_DAY",None]); continue
    prev = C.prev_trading_day_ohlc(i, today)
    tags[s] = {
      "PDC": C.compute_prevdaycontext_robust(prev),
      "OL":  C.compute_openlocation_from_df(i, today, prev),
      "OT":  C.compute_openingtrend_robust(i, today)
    }
    rows.append([s,"OK",None])

state={"date":str(today.date()),"symbols":SYMS,"steps":[{"ts":"now","step":"FORCE_TAGS","note":"today refresh"}],
       "status":"armed","tags":tags}
Path("data/state").mkdir(parents=True, exist_ok=True)
Path("data/state/live_state.json").write_text(json.dumps(state, indent=2))
print("OK →", state["date"])
