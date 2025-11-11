from __future__ import annotations
from datetime import datetime, time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

def now_ist() -> datetime:
    return datetime.now(tz=IST)

def ist_time(h: int, m: int, s: int = 0) -> time:
    return time(hour=h, minute=m, second=s, tzinfo=IST)

def is_after_ist(t: time) -> bool:
    n = now_ist()
    today_t = datetime.combine(n.date(), t)
    return n >= today_t

# Trading cutovers (hard, non-negotiable)
T_0915 = ist_time(9, 15)
T_0925 = ist_time(9, 25)
T_0930 = ist_time(9, 30)
T_093950 = ist_time(9, 39, 50)
T_0940 = ist_time(9, 40)
T_1505 = ist_time(15, 5)
T_1530 = ist_time(15, 30)
