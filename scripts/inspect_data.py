from __future__ import annotations

from pathlib import Path

import pandas as pd

from probedge.infra.settings import SETTINGS
from probedge.infra.loaders import read_tm5_csv
from probedge.storage import resolver


def summarize_tm5(path: Path) -> str:
    if not path.exists():
        return f"    TM5: MISSING at {path}"
    try:
        df = read_tm5_csv(path)
    except Exception as e:
        return f"    TM5: ERROR reading {path}: {e}"
    if df.empty:
        return f"    TM5: {path} (EMPTY)"
    first_dt = df["DateTime"].min()
    last_dt = df["DateTime"].max()
    n_rows = len(df)
    n_days = df["Date"].nunique()
    lines = [
        f"    TM5: {path}",
        f"      rows={n_rows}, unique_days={n_days}",
        f"      first_bar={first_dt}, last_bar={last_dt}",
    ]
    head = df.head(2)[["DateTime","Open","High","Low","Close"]]
    tail = df.tail(2)[["DateTime","Open","High","Low","Close"]]
    lines.append("      head_bars:")
    for _, r in head.iterrows():
        lines.append(f"        {r['DateTime']} O={r['Open']} H={r['High']} L={r['Low']} C={r['Close']}")
    lines.append("      tail_bars:")
    for _, r in tail.iterrows():
        lines.append(f"        {r['DateTime']} O={r['Open']} H={r['High']} L={r['Low']} C={r['Close']}")
    return "\n".join(lines)


def summarize_master(path: Path) -> str:
    if not path.exists():
        return f"    MASTER: MISSING at {path}"
    try:
        df = pd.read_csv(path)
    except Exception as e:
        return f"    MASTER: ERROR reading {path}: {e}"
    if df.empty:
        return f"    MASTER: {path} (EMPTY)"
    date_col = None
    for c in df.columns:
        if c.lower().startswith("date"):
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]
    try:
        dates = pd.to_datetime(df[date_col])
    except Exception:
        return f"    MASTER: {path} (could not parse date column '{date_col}')"
    first_date = dates.min().date()
    last_date = dates.max().date()
    n_rows = len(df)
    lines = [
        f"    MASTER: {path}",
        f"      rows={n_rows}, date_col={date_col}",
        f"      first_date={first_date}, last_date={last_date}",
        "      head_dates: " + ", ".join(str(d.date()) for d in dates.head(3)),
        "      tail_dates: " + ", ".join(str(d.date()) for d in dates.tail(3)),
    ]
    return "\n".join(lines)


def main() -> None:
    data_dir = SETTINGS.data_dir
    print("=== ProbEdge data inspection ===")
    print(f"DATA_DIR: {data_dir}")
    print(f"Symbols from SETTINGS: {SETTINGS.symbols}")
    print("")

    # Quick view of directory layout
    data_root = data_dir / "data"
    intraday_dir = data_root / "intraday"
    masters_dir = data_root / "masters"
    print(f"data/ exists: {data_root.exists()}")
    if data_root.exists():
        print("  contents of data/:")
        for child in sorted(data_root.iterdir()):
            if child.is_dir():
                print(f"    [DIR]  {child.name}")
            else:
                print(f"    [FILE] {child.name}")
    print("")
    print(f"data/intraday exists: {intraday_dir.exists()}")
    if intraday_dir.exists():
        print("  sample files in data/intraday/:")
        for child in sorted(intraday_dir.iterdir())[:20]:
            print(f"    {child.name}")
    print("")
    print(f"data/masters exists: {masters_dir.exists()}")
    if masters_dir.exists():
        print("  sample files in data/masters/:")
        for child in sorted(masters_dir.iterdir())[:20]:
            print(f"    {child.name}")
    print("")
    print("---- per-symbol coverage ----")
    print("")
    for sym in SETTINGS.symbols:
        sym = sym.upper()
        tm5_path = resolver.intraday_path(sym)
        master_path = resolver.master_path(sym)
        print(f"=== {sym} ===")
        print(summarize_tm5(tm5_path))
        print(summarize_master(master_path))
        print("")


if __name__ == "__main__":
    main()
