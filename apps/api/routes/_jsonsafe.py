import pandas as pd
import numpy as np

def _to_json_safe(v):
    # None stays None
    if v is None:
        return None

    # Pandas NA/NaT
    try:
        # catches pd.NA, NaT, numpy.nan
        if pd.isna(v):
            return None
    except Exception:
        pass

    # Numpy scalars -> Python scalars
    if isinstance(v, (np.floating,)):
        # Drop inf/-inf
        if not np.isfinite(v):
            return None
        return float(v)
    if isinstance(v, (np.integer,)):
        return int(v)

    # Datetime-like -> ISO string
    if isinstance(v, (pd.Timestamp,)):
        if pd.isna(v):
            return None
        return v.strftime("%Y-%m-%d %H:%M:%S")

    # Strings: just return
    if isinstance(v, str):
        return v

    # Plain Python numbers
    if isinstance(v, (float, int, bool)):
        # guard non-finite floats
        if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
            return None
        return v

    # Fallback: leave as-is (lists/dicts) if JSON encodable, else str()
    try:
        # quick check: convert numpy types inside lists
        if isinstance(v, (list, tuple)):
            return [_to_json_safe(x) for x in v]
        if isinstance(v, dict):
            return {str(k): _to_json_safe(x) for k, x in v.items()}
    except Exception:
        pass
    return str(v)

def json_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    d = df.copy()

    # Normalize Date/DateTime columns to string (if present)
    for col in d.columns:
        lc = str(col).lower()
        if lc in ("datetime", "date_time", "timestamp", "date"):
            try:
                dt = pd.to_datetime(d[col], errors="coerce")
                d[col] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                # leave as-is; will be handled by _to_json_safe
                pass

    # Replace +/-inf -> NaN first so they become None later
    d = d.replace([np.inf, -np.inf], np.nan)

    # Apply per-cell sanitizer to guarantee strict JSON compatibility
    d = d.applymap(_to_json_safe)

    return d
