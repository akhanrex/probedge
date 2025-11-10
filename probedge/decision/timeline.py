import asyncio, json
from datetime import datetime
import zoneinfo
from pathlib import Path
from probedge.infra.settings import SETTINGS

IST = zoneinfo.ZoneInfo("Asia/Kolkata")
STATE_PATH = Path("data/state/live_state.json")
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

def ist_now():
    return datetime.now(tz=IST)

def today_at(h, m, s=0):
    n = ist_now()
    return n.replace(hour=h, minute=m, second=s, microsecond=0)

async def sleep_until(dt):
    delay = (dt - ist_now()).total_seconds()
    if delay > 0:
        await asyncio.sleep(delay)

async def write_state(obj):
    STATE_PATH.write_text(json.dumps(obj, indent=2, default=str))

async def run_timeline():
    while True:
        t_pdc = today_at(9,25)
        t_ol  = today_at(9,30)
        t_ot  = today_at(9,39,50)
        t_arm = today_at(9,40)

        plan = {
            "date": ist_now().date().isoformat(),
            "symbols": SETTINGS.symbols,
            "steps": [],
            "status": "init"
        }

        # 09:25 PDC
        await sleep_until(t_pdc)
        plan["steps"].append({"ts": ist_now().isoformat(), "step": "PDC", "note": "prev-day context pending"})
        await write_state(plan)

        # 09:30 OL
        await sleep_until(t_ol)
        plan["steps"].append({"ts": ist_now().isoformat(), "step": "OL", "note": "open location pending"})
        await write_state(plan)

        # 09:39:50 OT
        await sleep_until(t_ot)
        plan["steps"].append({"ts": ist_now().isoformat(), "step": "OT", "note": "opening trend pending"})
        await write_state(plan)

        # 09:40 ARM
        await sleep_until(t_arm)
        plan["steps"].append({"ts": ist_now().isoformat(), "step": "ARM", "note": "entries logic pending"})
        plan["status"] = "armed"
        await write_state(plan)

        # keep task alive
        await asyncio.sleep(60)
