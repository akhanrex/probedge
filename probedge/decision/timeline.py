import asyncio, json
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from probedge.infra.settings import SETTINGS
from probedge.decision.tags_engine import compute_tags_for_day

IST = timezone(timedelta(hours=5, minutes=30))
STATE_PATH = Path("data/state/live_state.json")

def ist_now():    return datetime.now(IST)
def ist_today():  return ist_now().date()
def at_today(h,m,s=0): return datetime.combine(ist_today(), time(h, m, s, tzinfo=IST))

async def sleep_until(dt: datetime):
    dt = dt.astimezone(IST)
    sec = (dt - ist_now()).total_seconds()
    if sec > 0:
        await asyncio.sleep(sec)

def write_state_sync(obj: dict):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(obj, indent=2, default=str))

async def write_state(obj: dict):
    write_state_sync(obj)

async def run_timeline():
    syms = SETTINGS.symbols
    while True:
        today = ist_today()
        plan = {"date": str(today), "symbols": syms, "steps": [], "status": "init", "tags": {}}

        # schedule (IST)
        t_pdc = at_today(9,25,0)
        t_ol  = at_today(9,30,0)
        t_ot  = at_today(9,39,50)
        t_arm = at_today(9,40,0)

        async def do(step, key):
            plan["steps"].append({"ts": ist_now().isoformat(), "step": step, "note": "computed"})
            for sym in syms:
                try:
                    t = compute_tags_for_day(sym, date_target=today)
                    plan["tags"].setdefault(sym, {})[key] = t[key]
                except Exception as e:
                    plan["tags"].setdefault(sym, {})[key] = f"ERR:{e}"
            await write_state(plan)

        # Run steps in order, sleeping if we're before schedule; if we're past, do immediately.
        if ist_now() < t_pdc: await sleep_until(t_pdc)
        await do("PDC", "PDC")

        if ist_now() < t_ol: await sleep_until(t_ol)
        await do("OL", "OL")

        if ist_now() < t_ot: await sleep_until(t_ot)
        await do("OT", "OT")

        if ist_now() < t_arm: await sleep_until(t_arm)
        plan["steps"].append({"ts": ist_now().isoformat(), "step": "ARM", "note": "armed"})
        plan["status"] = "armed"
        await write_state(plan)

        # sleep to next day 09:00 IST
        next_morning = datetime.combine(today + timedelta(days=1), time(9,0,tzinfo=IST))
        await sleep_until(next_morning)
