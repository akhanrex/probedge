import pandas as pd
import numpy as np

def read_tm5_csv(path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # --- normalize DateTime ---
    cols_lower = {c.lower(): c for c in df.columns}
    dt = None
    for key in ("datetime", "date_time", "timestamp", "date"):
        if key in cols_lower:
            dt = pd.to_datetime(df[cols_lower[key]], errors="coerce")
            break
    if dt is None and ("date" in cols_lower and "time" in cols_lower):
        dt = pd.to_datetime(
            df[cols_lower["date"]].astype(str) + " " + df[cols_lower["time"]].astype(str),
            errors="coerce",
        )
    if dt is None:
        raise ValueError(f"No recognizable datetime column in {path}")

    if "DateTime" in df.columns:
        df["DateTime"] = dt
    else:
        df.insert(0, "DateTime", dt)

    # --- normalize OHLCV to canonical names ---
    def ensure_col(canon: str, *aliases: str):
        """
        Make sure df[canon] exists, using any of the aliases (case-insensitive).
        Example: ensure_col("Open", "open", "o")
        """
        if canon in df.columns:
            src = canon
        else:
            src = None
            for a in aliases:
                a_low = a.lower()
                if a_low in cols_lower:
                    src = cols_lower[a_low]
                    break
        if src is None:
            # For Volume we will catch this separately; for OHLC it is fatal
            raise KeyError(canon)
        if src != canon:
            df[canon] = pd.to_numeric(df[src], errors="coerce")
        else:
            df[canon] = pd.to_numeric(df[canon], errors="coerce")

    # required
    ensure_col("Open", "open", "o")
    ensure_col("High", "high", "h")
    ensure_col("Low", "low", "l")
    ensure_col("Close", "close", "c")

    # optional volume
    try:
        ensure_col("Volume", "volume", "vol")
    except KeyError:
        df["Volume"] = np.nan

    # final clean + derived fields
    df = (
        df.dropna(subset=["DateTime", "Open", "High", "Low", "Close"])
          .sort_values("DateTime")
          .reset_index(drop=True)
    )
    df["Date"] = df["DateTime"].dt.normalize()
    df["_mins"] = df["DateTime"].dt.hour * 60 + df["DateTime"].dt.minute
    return df


def by_day_map(df_tm5: pd.DataFrame):
    return {d: g.sort_values("DateTime").reset_index(drop=True) for d, g in df_tm5.groupby("Date")}
