from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import os


def build_master_from_kite_daily(
    kite, symbol: str, out_csv: str, *, years_back: int = 10
) -> bool:
    """
    Pulls daily candles in yearly chunks for `years_back` years up to today,
    concatenates, de-dupes, sorts, and writes CSV with a clean 'Date' column.
    Returns True if a non-empty file was written.
    """
    if kite is None or not symbol:
        return False

    today = date.today()
    start = (today - relativedelta(years=years_back)).replace(day=1)
    end = today

    frames = []
    cur_from = start
    while cur_from <= end:
        cur_to = min(cur_from + relativedelta(years=1) - timedelta(days=1), end)
        try:
            # Zerodha interval for EOD is usually "day" (not "1day")
            data = kite.historical_data(
                instrument_token=kite.ltp(symbol)[symbol][
                    "instrument_token"
                ],  # or resolve once above & reuse
                from_date=cur_from,
                to_date=cur_to,
                interval="day",
                continuous=False,
                oi=False,
            )
        except Exception as e:
            data = []
        df = pd.DataFrame(data)
        if not df.empty:
            # Standardize columns
            # Zerodha usually gives 'date','open','high','low','close','volume'
            if "date" in df.columns:
                df["Date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                df.drop(columns=[c for c in ["date"] if c in df.columns], inplace=True)
            elif "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
            else:
                # No date? skip this chunk
                df = pd.DataFrame()
        if not df.empty:
            frames.append(df)
        cur_from = cur_to + timedelta(days=1)

    if not frames:
        return False

    g = pd.concat(frames, ignore_index=True)
    # De-dupe on Date and keep the last (latest) occurrence
    if "Date" not in g.columns:
        return False
    g = g.dropna(subset=["Date"]).copy()
    g["Date"] = pd.to_datetime(g["Date"], errors="coerce").dt.date
    g = g.drop_duplicates(subset=["Date"]).sort_values("Date")

    # Make sure the columns your app expects are present
    for col in (
        "PrevDayContext",
        "OpenLocation",
        "FirstCandleType",
        "OpeningTrend",
        "RangeStatus",
        "Result",
    ):
        if col not in g.columns:
            g[col] = pd.NA

    # Write
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    out = g.copy()
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    out.to_csv(out_csv, index=False)
    return len(out) > 0
