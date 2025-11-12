from __future__ import annotations
import pandas as pd

# Constants for session in IST 5-min bars
SESSION_START = (9, 15)
ORB_END = (9, 40)

def to_minutes(dt: pd.Series) -> pd.Series:
    return dt.dt.hour*60 + dt.dt.minute

def in_range(mins: pd.Series, start_hm, end_hm) -> pd.Series:
    s = start_hm[0]*60 + start_hm[1]
    e = end_hm[0]*60 + end_hm[1]
    return (mins >= s) & (mins <= e)
