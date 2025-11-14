import pandas as pd
import numpy as np

def json_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    d = df.copy()

    # Normalize common datetime columns to string (if present)
    for col in ("DateTime", "Datetime", "DATE", "Date"):
        if col in d.columns:
            try:
                dt = pd.to_datetime(d[col], errors="coerce")
                d[col] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

    # Replace NaN/NaT and +/-inf → None
    d = d.replace([np.inf, -np.inf], np.nan)
    d = d.where(pd.notnull(d), None)
    return d
