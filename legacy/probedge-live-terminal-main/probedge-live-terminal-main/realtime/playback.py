import os, asyncio, time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT/"data"/"paper"

async def playback_tick_stream(symbols, date_str: str, bar_seconds: int = 300, speed: float = 3.0):
    """
    Async generator that yields batches of (symbol, ts, price) from pre-downloaded CSVs.
    For each 5-min bar we synthesize 4 ticks spaced within the interval so BarAggregator closes bars.
    """
    # Load per-symbol frames for date
    frames = {}
    for s in symbols:
        csv = DATA_DIR/date_str/f"{s}.csv"
        if not Path(csv).exists():
            frames[s] = pd.DataFrame()
            continue
        df = pd.read_csv(csv)
        # Expect end_ts present; if not, derive from DateTime
        if "end_ts" not in df.columns:
            if "DateTime" in df.columns:
                # assume DateTime is IST string; add bar_seconds
                ts = pd.to_datetime(df["DateTime"])  # naive is fine for relative
                df["start_ts"] = (ts.values.astype('datetime64[s]').astype('int64'))
                df["end_ts"] = df["start_ts"] + int(bar_seconds)
            else:
                raise RuntimeError(f"{csv} missing end_ts and DateTime")
        frames[s] = df.reset_index(drop=True)

    # Compute max bars available across symbols
    max_n = max((len(frames[s]) for s in symbols), default=0)
    if max_n == 0:
        # yield nothing but keep generator alive
        while True:
            await asyncio.sleep(1.0)
            yield []

    # Within-bar tick sequencing (4 steps): Open -> High -> Low -> Close
    step_frac = [0.10, 0.35, 0.65, 0.99]
    step_key  = ["Open", "High", "Low", "Close"]

    # Pacing: wall clock sleep between steps
    base_sleep = max(0.05, (bar_seconds / 4.0) / max(0.1, speed))

    for i in range(max_n):
        # For each of the 4 steps within the bar
        for j, (frac, k) in enumerate(zip(step_frac, step_key)):
            batch = []
            for s in symbols:
                df = frames[s]
                if i >= len(df):
                    continue
                row = df.iloc[i]
                start_ts = int(row.get("start_ts", int(row["end_ts"]) - int(bar_seconds)))
                ts = int(start_ts + frac * bar_seconds)
                price = float(row[k])
                batch.append((s, ts, price))
            # Emit one combined batch across symbols for this step
            yield batch
            await asyncio.sleep(base_sleep)
