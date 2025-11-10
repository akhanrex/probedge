from fastapi import APIRouter
from pathlib import Path
import pandas as pd
import numpy as np

router = APIRouter()

JOURNAL_PATH = Path("data/journal/journal.csv")

def _load_portfolio_daily():
    if not JOURNAL_PATH.exists():
        return pd.Series(dtype=float)

    df = pd.read_csv(JOURNAL_PATH)
    if df.empty:
        return pd.Series(dtype=float)

    # normalize columns
    df.columns = [str(c).strip().lower() for c in df.columns]

    # date column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    elif "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"], errors="coerce").dt.date
    else:
        raise ValueError("journal.csv must have a 'Date' (or 'DateTime') column")

    # choose a returns field
    if "dr_rs" in df.columns:
        # already in R units per row; aggregate by day
        dR = df.groupby("date")["dr_rs"].sum()
    elif "pnl_r2" in df.columns:
        dR = df.groupby("date")["pnl_r2"].sum()
    elif "dr_pct" in df.columns:
        # percent series; aggregate by equal-weight mean and convert to decimal R
        dR = df.groupby("date")["dr_pct"].mean() / 100.0
    else:
        raise ValueError("journal.csv must have one of: dR_rs, PNL_R2, dR_pct")

    dR = dR.sort_index()
    dR = dR.replace([np.inf, -np.inf], np.nan).dropna()
    return dR

def _kpis(dR: pd.Series):
    Days = int(len(dR))
    if Days == 0:
        return dict(Days=0, Total_RS=0.0, Avg_Daily_RS=0.0, Sharpe_dR=0.0, Sortino_dR=0.0,
                    **{"%Green": 0.0}, LosingStreak_days=0, Annual_R=0.0, MaxDD_RS=0.0, Calmar=0.0, Ulcer=0.0)

    mu = float(dR.mean())
    sd = float(dR.std(ddof=1)) or 1e-9
    sharpe = (mu / sd) * np.sqrt(252.0)

    downside = dR[dR < 0]
    ds = float(downside.std(ddof=1)) or 1e-9
    sortino = (mu / ds) * np.sqrt(252.0)

    pct_green = float((dR > 0).mean())

    # longest losing streak (days)
    longest = 0
    cur = 0
    for x in dR:
        if x <= 0:
            cur += 1
            longest = max(longest, cur)
        else:
            cur = 0

    # drawdown metrics on equity curve of (1 + dR) compounding
    cr = (1.0 + dR).cumprod()
    peak = cr.cummax()
    dd = cr / peak - 1.0
    maxdd = float(dd.min())

    annual = float(mu * 252.0)
    calmar = float((annual / abs(maxdd)) if maxdd < 0 else np.inf)
    ulcer = float(np.sqrt(np.mean(np.minimum(0.0, dd.values * 100.0) ** 2)))

    return dict(
        Days=Days,
        Total_RS=float(dR.sum()),
        Avg_Daily_RS=mu,
        Sharpe_dR=float(sharpe),
        Sortino_dR=float(sortino),
        **{"%Green": round(pct_green, 3)},
        LosingStreak_days=int(longest),
        Annual_R=annual,
        MaxDD_RS=maxdd,
        Calmar=calmar,
        Ulcer=ulcer,
    )

@router.get("/api/journal/daily")
def api_journal_daily():
    dR = _load_portfolio_daily()
    metrics = _kpis(dR)
    series = [{"date": str(ix), "dR": float(val)} for ix, val in dR.items()]
    return {"metrics": metrics, "days": metrics["Days"], "series": series}
