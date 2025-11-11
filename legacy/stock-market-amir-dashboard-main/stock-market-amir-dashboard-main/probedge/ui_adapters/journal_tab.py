from __future__ import annotations
import os
import re
from datetime import date, time, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from html import escape as _html_escape

# =============================
# Paths & Config  (env-aware; resolves to repo root)
# =============================
st.set_page_config(layout="wide")  # ERP-like wide layout

# This file lives under probedge/ui_adapters/... so we need parents[2] to reach the repo root
REPO_ROOT = Path(__file__).resolve().parents[2]


def _abs(p: str | Path) -> Path:
    p = Path(str(p))
    return p if p.is_absolute() else (REPO_ROOT / p)


# Allow environment overrides from run.py (we set PE_JOURNAL_DATA_DIR there)
DEFAULT_ROOT = REPO_ROOT
DEFAULT_LATEST = _abs("data/latest")
DEFAULT_MASTER = _abs("data/masters/TataMotors_Master.csv")

DATA_DIR = Path(os.environ.get("PE_JOURNAL_DATA_DIR", str(DEFAULT_LATEST))).resolve()
MASTER_CSV = Path(os.environ.get("PE_JOURNAL_MASTER", str(DEFAULT_MASTER))).resolve()

CONFIG: Dict[str, object] = {
    "RISK_UNIT": 10000,
    "SESSION_START": "09:15",
    "SESSION_END": "15:05",
    "MASTER_DEFAULT_PATH": str(MASTER_CSV),
    "LATEST_DIR": str(DATA_DIR),
    "START_CAPITAL": 0,
    "ANNUAL_TRADING_DAYS": 252,
}

# Tiny hint for debugging path resolution (remove if you like)
try:
    st.caption(f"Journal data dir: `{CONFIG['LATEST_DIR']}`")
except Exception:
    pass

# ===============
# Themes / Styles (Professional Aesthetic)
# ===============
THEMES: Dict[str, Dict[str, str]] = {
    "light": {
        "bg": "#f8f9fa",  # Lighter background
        "card": "#ffffff",
        "card_hover": "#f3f6fb",
        "border": "#e5e7eb",
        "text": "#0f172a",
        "muted": "#64748b",
        "grid": "#e5e7eb",
        "primary": "#7C3AED",  # Purple accent
        "primary2": "#EC4899",  # Pink accent
        "green": "#10b981",
        "red": "#ef4444",
        "shadow": "0 4px 12px rgba(0, 0, 0, .05), 0 1px 3px rgba(0, 0, 0, .03)",  # Softer shadow
        "shadow_hover": "0 10px 20px rgba(0, 0, 0, .08), 0 4px 8px rgba(0, 0, 0, .05)",
    },
}
# Initialize TZ globally to avoid errors outside the main function, although it will be set in render_journal.
try:
    TZ = THEMES[st.session_state.get("_pe_theme_name", "light")]
except:
    TZ = THEMES["light"]


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
          .stApp {{ background: {TZ['bg']}; font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }}
          
          ::-webkit-scrollbar {{ width: 8px; }}
          ::-webkit-scrollbar-track {{ background: {TZ['bg']}; }}
          ::-webkit-scrollbar-thumb {{ background: {TZ['border']}; border-radius: 4px; }}
          
          .app-title {{ font-size: 28px; font-weight: 800; letter-spacing: .2px;
            background: linear-gradient(135deg,{TZ['primary']},{TZ['primary2']}); -webkit-background-clip: text; background-clip: text; color: transparent; margin-bottom: 6px; }}
          .app-subtle {{ color:{TZ['muted']}; font-size: 13px; margin-bottom: 24px; }}
          
          /* General card style */
          .pe-card {{ background:{TZ['card']}; border:1px solid {TZ['border']}; border-radius:12px; padding:20px;
            box-shadow:{TZ['shadow']}; transition: all 180ms ease-in-out; }}

          /* KPI metric card style */
          .metric-card {{
            background:{TZ['card']}; border:1px solid {TZ['border']}; border-radius:12px; padding:16px;
            text-align:left; margin:0; height:auto; width:100%;
            aspect-ratio:1/1;
            max-width: clamp(150px, 18vw, 230px);
            max-height: clamp(150px, 18vw, 230px);
            display:flex; flex-direction:column; justify-content:space-between; gap:4px;
            box-shadow:{TZ['shadow']}; transition: all 180ms ease-in-out;
            overflow:hidden;
            align-items:stretch;
          }}
          .metric-card:hover {{ box-shadow: {TZ['shadow_hover']}; }}
          .metric-card .label .emoji {{ margin-right: 6px; font-size: 14px; }}
          .metric-card .label {{ color:{TZ['muted']}; font-size: clamp(10px, 1.0vw, 12px); margin:0; font-weight:600; display:flex; align-items:center; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
          .metric-card .value {{ color:{TZ['text']}; font-weight:800; line-height:1.1; font-size: clamp(16px, 1.7vw, 22px); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
          .metric-card .sub-value {{ color:{TZ['muted']}; font-size: clamp(9px, 0.95vw, 11px); margin:0; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
          .metric-card * {{ min-width:0; }}
          
          /* Calendar styling */
          .tz-badge-green {{ background:#10b98115; color:#10b981; border-radius:4px; padding: clamp(1px, 0.25vw, 3px) clamp(4px, 0.6vw, 6px); font-size: clamp(8px, 0.85vw, 10px); font-weight:600; display:inline-block; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
          .tz-badge-red   {{ background:#ef444415; color:#ef4444; border-radius:4px; padding: clamp(1px, 0.25vw, 3px) clamp(4px, 0.6vw, 6px); font-size: clamp(8px, 0.85vw, 10px); font-weight:600; display:inline-block; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
            border:1px solid {TZ['border']}; border-radius:8px; padding:6px;
            background:{TZ['card']};
            width:100%;
            aspect-ratio:1/1;
            max-width: clamp(48px, 6.5vw, 92px);
            max-height: clamp(48px, 6.5vw, 92px);
            margin:0 auto;
            display:flex; flex-direction:column; justify-content:space-between;
            overflow:hidden;
          }}
          .cal-day-header {{ font-size: clamp(9px, 0.9vw, 11px); font-weight:600; color:{TZ['muted']}; text-align:right; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
          
          /* Streamlit widget tweaks */
          .stTabs [data-baseweb="tab-list"] {{ gap: 12px; }}
          .stTabs [data-baseweb="tab"] {{ border-radius: 8px; background-color: {TZ['card_hover']}; border: 1px solid {TZ['border']}; }}
          
          .analysis-range {{ color:{TZ['muted']}; font-size:12px; margin-bottom: 8px; }}
          .metric-row {{ display:flex; gap:12px; flex-wrap:wrap; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ===========================
# Helpers & Data Parsing (Standard)
# ===========================
_DATE_PAT = re.compile(r"\d")


def _to_dt_any(x: pd.Series) -> pd.Series:
    s = x.astype(str).str.strip()
    has_digit = s.str.contains(_DATE_PAT).fillna(False)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if not has_digit.any():
        return out
    t = s.where(has_digit, None)
    p1 = pd.to_datetime(t, errors="coerce")
    out = p1.copy()
    need_dayfirst = out.isna() & has_digit
    if need_dayfirst.any():
        p2 = pd.to_datetime(t[need_dayfirst], errors="coerce", dayfirst=True)
        out.loc[need_dayfirst] = p2
    need_excel = out.isna() & s.str.match(r"^\d{5,}$").fillna(False)
    if need_excel.any():
        p3 = pd.to_datetime(
            s[need_excel].astype(float), unit="D", origin="1899-12-30", errors="coerce"
        )
        out.loc[need_excel] = p3
    return out


def _ensure_date_col(series: pd.Series) -> pd.Series:
    dt = _to_dt_any(series)
    return dt.dt.tz_localize(None, nonexistent="NaT", ambiguous="NaT").dt.date


def _session_mask(ts: pd.Series, start="09:15", end="15:05") -> pd.Series:
    dt = _to_dt_any(ts)
    sh, sm = map(int, start.split(":"))
    eh, em = map(int, end.split(":"))

    def ok(x):
        if pd.isna(x):
            return False
        tt = x.time()
        return time(sh, sm) <= tt <= time(eh, em)

    return dt.apply(ok)


def _find_tradebook(latest_dir: Path) -> Optional[Path]:
    exact = latest_dir / "tradebook.csv"
    if exact.exists():
        return exact
    cands = sorted(
        latest_dir.glob("tradebook*.csv"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    return cands[0] if cands else None


def _std_symbol(sym: str) -> str:
    if pd.isna(sym):
        return ""
    s = str(sym).upper().strip()
    s = re.sub(r"[\s,_\-]+", "", s)
    s = re.sub(r"(EQ|BE|BZ|BL|SM|NW|PP)$", "", s)
    if s in {"TATAMOTORSLTD", "TATAMOTORSLIMITED", "TATAMOTORS"}:
        return "TATAMOTORS"
    return s


@st.cache_data
def parse_tradebook_csv(path: Path) -> pd.DataFrame:
    # Logic to parse tradebook and group into daily P&L
    empty = pd.DataFrame(
        columns=[
            "trade_date",
            "symbol_std",
            "trades_n",
            "sell_value",
            "buy_value",
            "pnl_gross_est",
            "session_ok",
        ]
    )
    if not path or not path.exists():
        return empty
    df = pd.read_csv(
        path, dtype=str, encoding="utf-8", na_filter=False, low_memory=False
    )
    cols = {c: c.strip() for c in df.columns}
    low = {c: c.strip().lower() for c in df.columns}
    alias = {
        "tradingsymbol": "symbol",
        "trading symbol": "symbol",
        "symbol": "symbol",
        "instrument": "symbol",
        "scrip": "symbol",
        "scrip name": "symbol",
        "exchange time": "order_execution_time",
        "exchange_time": "order_execution_time",
        "exchange timestamp": "order_execution_time",
        "exchange_timestamp": "order_execution_time",
        "time": "order_execution_time",
        "timestamp": "order_execution_time",
        "date": "trade_date",
        "trade date": "trade_date",
        "trade_date": "trade_date",
        "dates": "trade_date",
        "order date": "trade_date",
        "order_date": "trade_date",
        "trade type": "trade_type",
        "transaction type": "trade_type",
        "buy/sell": "trade_type",
        "avg. price": "price",
        "average price": "price",
        "average_price": "price",
        "price": "price",
        "quantity": "quantity",
        "qty.": "quantity",
        "qty": "quantity",
        "order id": "order_id",
        "order_id": "order_id",
        "trade id": "trade_id",
        "trade_id": "trade_id",
        "segment": "segment",
        "product": "series",
        "exchange": "exchange",
    }
    df = df.rename(columns={c: alias.get(low[c], cols[c]) for c in df.columns})

    def txt(name: str) -> pd.Series:
        return (
            df[name]
            if name in df.columns
            else pd.Series([""] * len(df), index=df.index, dtype="object")
        )

    bt = pd.DataFrame(index=df.index)
    broker_ts = _to_dt_any(txt("order_execution_time"))
    trade_dt = _to_dt_any(txt("trade_date"))
    if trade_dt.isna().all() and broker_ts.notna().any():
        trade_dt = broker_ts
    bt["broker_ts"] = broker_ts
    bt["trade_date"] = trade_dt.dt.date
    bt["symbol_std"] = txt("symbol").astype(str).apply(_std_symbol)
    side_raw = txt("trade_type").astype(str).str.upper().str.strip()
    bt["side"] = np.where(
        side_raw.str.startswith("S"),
        "SELL",
        np.where(side_raw.str.startswith("B"), "BUY", side_raw),
    )
    bt["order_id"] = txt("order_id").astype(str)
    bt["trade_id"] = txt("trade_id").astype(str)

    def num(name: str) -> pd.Series:
        s = txt(name).str.replace(",", "", regex=False).str.strip()
        return pd.to_numeric(s.replace({"": np.nan, "—": "0"}), errors="coerce")

    bt["qty"] = num("quantity").abs()
    bt["price"] = num("price")
    val = (bt["qty"] * bt["price"]).fillna(0.0)
    bt["signed_value"] = np.where(bt["side"] == "SELL", val, -val)
    bt["in_session"] = _session_mask(
        txt("order_execution_time"),
        str(CONFIG["SESSION_START"]),
        str(CONFIG["SESSION_END"]),
    )
    bt = bt[bt["trade_date"].notna()]
    bt = bt[(bt["qty"].fillna(0) > 0) & (bt["price"].fillna(0) > 0)]
    bt = bt[(bt["trade_date"] >= date(2019, 1, 1)) & (bt["trade_date"] <= date.today())]
    bt = bt.drop_duplicates(
        subset=[
            "trade_id",
            "order_id",
            "broker_ts",
            "symbol_std",
            "side",
            "qty",
            "price",
        ],
        keep="last",
    )
    if bt.empty:
        return empty
    grp = (
        bt.groupby(["trade_date", "symbol_std"], dropna=False)
        .agg(
            trades_n=("trade_id", "count"),
            sell_value=("signed_value", lambda s: float(s[s > 0].sum())),
            buy_value=("signed_value", lambda s: float(-s[s < 0].sum())),
            pnl_gross_est=("signed_value", "sum"),
            session_ok=(
                "in_session",
                lambda s: float(np.mean(s)) if len(s) > 0 else np.nan,
            ),
        )
        .reset_index()
    )
    return grp


@st.cache_data
def load_master(path: str) -> pd.DataFrame:
    # Logic to load master tag file
    usecols = [
        "Date",
        "PrevDayContext",
        "GapType",
        "OpenLocation",
        "FirstCandleType",
        "OpeningTrend",
        "RangeStatus",
        "Result",
    ]
    try:
        m = pd.read_csv(path, usecols=usecols, low_memory=False, dtype=str)
    except (FileNotFoundError, ValueError):
        # Always return the expected schema to keep merges safe
        m = pd.DataFrame(columns=usecols)

    # Ensure schema & types
    if "Date" not in m.columns:
        m["Date"] = pd.NaT
    m["Date"] = _ensure_date_col(m["Date"])

    for c in [
        "PrevDayContext",
        "GapType",
        "OpenLocation",
        "FirstCandleType",
        "OpeningTrend",
        "RangeStatus",
        "Result",
    ]:
        if c not in m.columns:
            m[c] = "N/A"
        else:
            m[c] = m[c].astype(str).str.strip().str.upper().replace("", "N/A")

    if "symbol_std" not in m.columns:
        m["symbol_std"] = "TATAMOTORS"
    else:
        m["symbol_std"] = m["symbol_std"].astype(str).str.strip().str.upper()

    return m

def kpis(df: pd.DataFrame, risk_unit: float) -> Dict:
    # Calculate fundamental KPIs
    if df.empty:
        return {k: 0 for k in ["net_p", "net_r", "win_rate", "expectancy", "days"]} | {
            k: np.nan for k in ["pf", "avg_win_r", "avg_loss_r", "best", "worst"]
        }
    pnl = pd.to_numeric(df["pnl_final"], errors="coerce").fillna(0.0)
    r_net = pnl / risk_unit
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    n_wins, n_losses = len(wins), len(losses)
    total_days = len(df[df["day_R_net"] != 0])  # Count days with non-zero PNL
    days_in_period = df["Date"].nunique()

    # Use total days with non-zero P&L for win rate calculation
    total_trades = n_wins + n_losses
    gross_win = wins.sum()
    gross_loss = losses.sum()

    return {
        "net_p": pnl.sum(),
        "net_r": r_net.sum(),
        "days": days_in_period,  # Total unique days in period
        "win_rate": 100 * n_wins / total_trades if total_trades > 0 else 0.0,
        "pf": gross_win / abs(gross_loss) if gross_loss != 0 else np.inf,
        "expectancy": r_net.mean() if total_trades > 0 else 0.0,
        "avg_win_r": r_net[r_net > 0].mean() if n_wins > 0 else np.nan,
        "avg_loss_r": abs(r_net[r_net < 0].mean()) if n_losses > 0 else np.nan,
        "best": pnl.max() if not pnl.empty else 0.0,
        "worst": pnl.min() if not pnl.empty else 0.0,
    }


# =====================
# Professional Indicators (Alpha Metrics)
# =====================


def calculate_pro_indicators(df: pd.DataFrame, risk_unit: float) -> Dict:
    """Calculates Sharpe, Sortino, and Calmar Ratios based on daily R-multiples."""
    if df.empty or len(df) < 5:
        return {"sharpe": np.nan, "sortino": np.nan, "calmar": np.nan, "mdd_r": np.nan}

    daily_returns = df["day_R_net"].fillna(0)
    T = float(CONFIG["ANNUAL_TRADING_DAYS"])

    # Daily Equity Curve (based on R-multiples)
    equity_r = daily_returns.cumsum()

    # Sharpe Ratio (Assumes Risk-Free Rate of 0)
    avg_daily_return = daily_returns.mean()
    std_dev = daily_returns.std()
    sharpe = (avg_daily_return * np.sqrt(T)) / std_dev if std_dev != 0 else np.nan

    # Sortino Ratio (Assumes Target Return of 0)
    downside_returns = daily_returns[daily_returns < 0]
    downside_std = downside_returns.std()
    sortino = (
        (avg_daily_return * np.sqrt(T)) / downside_std if downside_std != 0 else np.nan
    )

    # Calmar Ratio & MDD
    peak = equity_r.expanding(min_periods=1).max()
    drawdown = peak - equity_r
    max_drawdown = drawdown.max()

    min_date = df["Date"].min()
    max_date = df["Date"].max()
    total_days_span = (max_date - min_date).days
    years = total_days_span / 365.25 if total_days_span > 0 else 1

    total_r = equity_r.iloc[-1]
    # Simple annualized return proxy for Calmar Numerator (Net R / Years)
    cagr_r = total_r / years

    calmar = cagr_r / max_drawdown if max_drawdown > 0 else np.nan

    return {
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "mdd_r": max_drawdown,
    }


# =====================
# Weekly Alpha Analysis Function
# =====================
def calculate_weekly_alpha_score(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates key weekly metrics and assigns an Alpha Score."""
    if df.empty:
        return pd.DataFrame()

    df_wk = df.copy()
    # Ensure Date column is a datetime object for resample/groupby
    df_wk["Date"] = pd.to_datetime(df_wk["Date"])

    # 1. Calculate the actual start date of each week's data. (FIXED KEYERROR)
    # Group by week end date and apply a lambda to find the minimum date of the index (which is 'Date').
    # We use 'day_R_net' as a column to apply the lambda function to, as 'Date' is the index.
    weekly_start_dates = (
        df_wk.set_index("Date")
        .groupby(pd.Grouper(freq="W"))["day_R_net"]
        .apply(lambda x: x.index.min().date())
        .rename("Week_Start")
    )

    # 2. Group by calendar week and calculate weekly metrics
    weekly_group = (
        df_wk.set_index("Date")
        .resample("W")
        .agg(
            net_r=("day_R_net", "sum"),
            std_r=("day_R_net", "std"),
            days_n=("day_R_net", "count"),
            max_r_day=("day_R_net", "max"),
            min_r_day=("day_R_net", "min"),
        )
        .reset_index()
    )

    # 3. Merge the calculated start dates
    weekly_group = (
        weekly_group.set_index("Date")
        .merge(
            weekly_start_dates.to_frame(), left_index=True, right_index=True, how="left"
        )
        .reset_index()
    )

    weekly_group["Week_End"] = weekly_group["Date"].dt.date

    # 4. Create the requested "Start Date - End Date" range label (MM/DD - MM/DD)
    def format_date_range(row):
        start_date = row["Week_Start"]
        if isinstance(start_date, pd.Timestamp):
            start_date = start_date.date()
        # Handle cases where Week_Start might be NaT due to no trades in that week
        if pd.isna(start_date):
            return f"{row['Week_End'].strftime('%m/%d')} (No trades)"

        return f"{start_date.strftime('%m/%d')} - {row['Week_End'].strftime('%m/%d')}"

    weekly_group["Week_Label"] = weekly_group.apply(format_date_range, axis=1)

    # Calculate Weekly MDD (Max Drawdown within the week)
    def calc_weekly_mdd(daily_r_series):
        if daily_r_series.empty:
            return 0.0
        equity = daily_r_series.cumsum()
        peak = equity.expanding(min_periods=1).max()
        return (peak - equity).max()

    weekly_mdd_r = (
        df_wk.set_index("Date")
        .groupby(pd.Grouper(freq="W"))["day_R_net"]
        .apply(calc_weekly_mdd)
        .reset_index(name="MDD_R")
    )
    weekly_group = weekly_group.merge(weekly_mdd_r, on="Date")

    # Calculate Weekly Calmar-like score (Net R / MDD R)
    weekly_group["R_to_MDD"] = np.where(
        weekly_group["MDD_R"] > 0, weekly_group["net_r"] / weekly_group["MDD_R"], np.nan
    )

    # Assign Alpha Score (0-5)
    def assign_score(row):
        if row["net_r"] >= 3.0 and row["R_to_MDD"] >= 1.5 and row["MDD_R"] <= 1.0:
            return "5 Star (Elite Alpha ⭐⭐⭐⭐⭐)"
        if row["net_r"] >= 1.5 and row["R_to_MDD"] >= 1.0:
            return "4 Star (Pro Trader ⭐⭐⭐⭐)"
        if row["net_r"] > 0.0 and row["R_to_MDD"] > 0.0:
            return "3 Star (Profitable & Managed ⭐⭐⭐)"
        if row["net_r"] >= -1.0:
            return "2 Star (Developing & Learning ⭐⭐)"
        return "1 Star (Review Required ⭐)"

    weekly_group["Alpha_Score"] = weekly_group.apply(assign_score, axis=1)

    return weekly_group.sort_values("Date", ascending=False).drop(
        columns=["Date", "std_r", "days_n", "Week_Start"]
    )


def render_weekly_alpha_analysis(jv: pd.DataFrame):
    if jv.empty:
        st.info("Insufficient data to perform weekly Alpha analysis.")
        return

    weekly_df = calculate_weekly_alpha_score(jv)

    start_dt = jv["Date"].min().strftime("%d %b %Y")
    end_dt = jv["Date"].max().strftime("%d %b %Y")

    st.markdown("### 🌟 Weekly Alpha Review (Hedge Fund Comparison)")
    st.markdown(
        f"""
    <div class="analysis-range">Analysis Range: {start_dt} to {end_dt}</div>
    This analysis breaks down performance week-by-week using risk-adjusted metrics (R/MDD ratio)
    and assigns an **Alpha Score** to benchmark your weekly results.
    """,
        unsafe_allow_html=True,
    )

    def color_score(score):
        if "Elite" in score:
            return f'background-color: {TZ["green"]}20; color: {TZ["green"]}; font-weight: 700;'
        if "Pro" in score:
            return f'background-color: {TZ["primary"]}20; color: {TZ["primary"]}; font-weight: 700;'
        if "Profitable" in score:
            return f'background-color: {TZ["muted"]}20; color: {TZ["muted"]};'
        if "Developing" in score:
            return f"background-color: #f59e0b20; color: #f59e0b;"
        return f'background-color: {TZ["red"]}20; color: {TZ["red"]};'

    def color_r_to_mdd(val):
        if val >= 1.5:
            return f'color: {TZ["green"]}; font-weight: 700;'
        if val >= 1.0:
            return f'color: {TZ["primary"]}; font-weight: 700;'
        if val > 0:
            return f'color: {TZ["muted"]};'
        return f'color: {TZ["red"]};'

    st.dataframe(
        weekly_df.rename(
            columns={
                "Week_End": "Week End Date",
                "Week_Label": "Week Range",
                "net_r": "Weekly Net R",
                "MDD_R": "Max Drawdown (R)",
                "R_to_MDD": "R/MDD Ratio",
                "max_r_day": "Best R Day",
                "min_r_day": "Worst R Day",
            }
        )
        .style.format(
            {
                "Weekly Net R": "{:.2f} R",
                "Max Drawdown (R)": "{:.2f} R",
                "R/MDD Ratio": "{:.2f}",
                "Best R Day": "{:+.2f} R",
                "Worst R Day": "{:+.2f} R",
            }
        )
        .applymap(color_score, subset=["Alpha_Score"])
        .applymap(color_r_to_mdd, subset=["R/MDD Ratio"]),
        use_container_width=True,
    )


# =====================
# Top Pattern Analysis Indicator
# =====================
def render_best_patterns_analysis(df: pd.DataFrame):
    if df.empty or len(df) < 10:
        st.info("Insufficient data to analyze best-performing patterns.")
        return

    # Identify the top 10% of best-performing days by Net R
    top_quantile = df["day_R_net"].quantile(0.90)
    best_days = df[df["day_R_net"] >= top_quantile].copy()

    if best_days.empty:
        top_quantile = df["day_R_net"].max()
        best_days = df[df["day_R_net"] >= top_quantile].copy()

    tag_cols = ["OpeningTrend", "Result", "GapType", "FirstCandleType"]

    st.markdown("### 🏆 Top Pattern Analysis (Best 10% of Days)")
    st.markdown(
        f"""
    This scorecard identifies **Master Tag** combinations most present during your **highest-performing trading days** (Net R $\ge$ **{top_quantile:.2f} R**).
    The Score (%) is the frequency of that tag within this high-performance subset.
    """
    )

    analysis_data = {}
    for tag in tag_cols:
        if tag in best_days.columns:
            tag_counts = best_days[tag].value_counts(normalize=True) * 100
            tag_avg_r = best_days.groupby(tag)["day_R_net"].mean()

            for index, score in tag_counts.items():
                if index != "N/A" and score > 0:
                    analysis_data[(tag, index)] = {
                        "Score (%)": score,
                        "Avg R": tag_avg_r.get(index, 0.0),
                        "Tag Type": tag,
                    }

    if not analysis_data:
        st.info("No significant patterns found in the best-performing days.")
        return

    analysis_df = pd.DataFrame.from_dict(analysis_data, orient="index").reset_index()
    analysis_df.columns = ["Tag Key", "Tag Value", "Score (%)", "Avg R", "Tag Type"]
    analysis_df = analysis_df.sort_values(by="Score (%)", ascending=False)

    display_df = analysis_df[["Tag Type", "Tag Value", "Score (%)", "Avg R"]].copy()

    def color_score(val):
        if val > 50:
            return f'background-color: {TZ["green"]}15; color: {TZ["green"]}; font-weight: 700;'
        if val > 20:
            return f'background-color: {TZ["primary"]}10; color: {TZ["primary"]}; font-weight: 700;'
        return f'color: {TZ["muted"]};'

    st.dataframe(
        display_df.rename(columns={"Avg R": "Avg R on Best Days"})
        .style.format({"Score (%)": "{:.1f}%", "Avg R on Best Days": "{:+.2f} R"})
        .applymap(color_score, subset=["Score (%)"]),
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<hr style='margin: 1.5rem 0;'>", unsafe_allow_html=True)


# ============================
# UI Components (KPIs, Chart, Calendar)
# ============================
def render_kpi_card(
    label: str, value, sub_value: str, formatter: str = "{:,.0f}", emoji: str = "📈"
):
    val_str = "—"
    if value is not None and np.isfinite(value):
        try:
            val_str = formatter.format(value)
        except ValueError:
            val_str = str(value)

    # Custom color styling for key performance metrics (Net R, MDD, Calmar)
    color_style = ""
    if "Net R-Multiple" in label:
        if value > 0:
            color_style = f"color: {TZ['green']};"
        elif value < 0:
            color_style = f"color: {TZ['red']};"
    elif "Max Drawdown" in label:
        if value > 0:
            color_style = f"color: {TZ['red']};"
    elif "Calmar Ratio" in label or "Sharpe Ratio" in label or "Sortino Ratio" in label:
        if value > 1.0:
            color_style = f"color: {TZ['green']};"
        elif value > 0.0:
            color_style = f"color: {TZ['primary']};"

    st.markdown(
        f"""
    <div class="metric-card">
        <div class="label"><span class="emoji">{emoji}</span>{label}</div>
        <div class="value" style="{color_style}">{val_str}</div>
        <div class="sub-value">{sub_value}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_main_chart(df: pd.DataFrame):
    if df.empty:
        return

    # We need a unique date index for plotting daily R
    df_chart = df.copy()
    df_chart["Date"] = pd.to_datetime(df_chart["Date"])

    # Aggregate daily R-multiple if symbol filter is ALL
    daily_r = df_chart.groupby("Date")["day_R_net"].sum().reset_index()

    # Merge daily R back for cumsum (ensure one row per day for cumsum)
    equity_df = df_chart.groupby("Date")[["pnl_final"]].sum().reset_index()
    start_capital = float(CONFIG.get("START_CAPITAL", 0)) or 0.0
    equity_df["equity_rupees"] = start_capital + equity_df["pnl_final"].cumsum()

    # FIX: Convert hex color to an rgba string that Plotly understands
    hex_color = TZ["primary"].lstrip("#")
    r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    equity_fill_color = f"rgba({r}, {g}, {b}, 0.2)"

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.7, 0.3]
    )

    # Equity Curve
    fig.add_trace(
        go.Scatter(
            x=equity_df["Date"],
            y=equity_df["equity_rupees"],
            name="Equity Curve (₹)",
            mode="lines",
            line=dict(width=2, color=TZ["primary"]),
            fill="tozeroy",
            fillcolor=equity_fill_color,
            hovertemplate="<b>%{x|%d %b %Y}</b><br>Equity: ₹%{y:,.0f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # Daily R-Multiple Bars
    bar_colors = [TZ["green"] if r >= 0 else TZ["red"] for r in daily_r["day_R_net"]]
    fig.add_trace(
        go.Bar(
            x=daily_r["Date"],
            y=daily_r["day_R_net"],
            name="Daily Net R",
            marker_color=bar_colors,
            opacity=0.8,
            width=1.0,  # width=1.0 for daily "small spaces" look
            hovertemplate="<b>%{x|%d %b %Y}</b><br>Daily Net R: %{y:.2f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # Layout configuration
    fig.update_layout(
        height=500,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor=TZ["bg"],
        plot_bgcolor=TZ["card"],
        font=dict(color=TZ["text"]),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis_showticklabels=True,
        xaxis_gridcolor=TZ["grid"],
        yaxis_gridcolor=TZ["grid"],
        yaxis2_gridcolor=TZ["grid"],
        xaxis2_title="Date",
        yaxis_title="Equity (₹)",
        yaxis2_title="Daily Net R-Multiple",
        xaxis_rangeslider_visible=False,
        # Enforce month-level ticks and formatting on the shared X-axis
        xaxis=dict(dtick="M1", tickformat="%b", gridcolor=TZ["grid"]),
        xaxis2=dict(dtick="M1", tickformat="%b", gridcolor=TZ["grid"], title="Date"),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_calendar_board(df: pd.DataFrame, start: date, end: date):
    import calendar as calmod

    if df.empty:
        st.info("No trades in the selected period to display on the calendar.")
        return
    if start > end:
        start, end = end, start
    months = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        months.append((cur.year, cur.month))
        cur = date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)
    st.markdown("<div style='margin-bottom:12px'></div>", unsafe_allow_html=True)

    # Render months in columns (3 months per row)
    for i in range(0, len(months), 3):
        cols = st.columns(3)
        for j in range(3):
            if i + j < len(months):
                y, m = months[i + j]
                with cols[j]:
                    render_month_calendar(df, y, m)


def _color_for_r(r: float, vmax: float) -> str:
    # Generates a background color based on R-multiple magnitude
    vmax = max(float(vmax), 1e-9)
    x = max(-vmax, min(vmax, r)) / vmax
    if x >= 0:
        # Green gradient (Win)
        base = np.array([0xFF, 0xFF, 0xFF])
        tgt = np.array([0xDC, 0xFC, 0xE7])
        # Interpolate from white to light green
        rgb = (base - x * (base - tgt)).astype(int)
    else:
        # Red gradient (Loss)
        base = np.array([0xFF, 0xFF, 0xFF])
        tgt = np.array([0xFE, 0xE2, 0xE2])
        # Interpolate from white to light red (1+x is 0 to 1 for x=-1 to 0)
        rgb = (base - (1 + x) * (base - tgt)).astype(int)
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def render_month_calendar(df: pd.DataFrame, year: int, month: int):
    import calendar as calmod

    if df.empty:
        return
    dfi = df.copy()
    dfi["Date"] = pd.to_datetime(dfi["Date"], errors="coerce").dropna().dt.date

    first = date(year, month, 1)
    last = date(year, month, calmod.monthrange(year, month)[1])
    month_days = pd.Series(pd.date_range(start=first, end=last).date)

    mdf = (
        dfi[dfi["Date"].isin(month_days)]
        .groupby("Date", dropna=True)["day_R_net"]
        .sum()
        .reset_index()
        .set_index("Date")
        .copy()
    )

    if mdf.empty:
        return

    vals = mdf["day_R_net"].abs()
    vmax = float(np.percentile(vals, 95)) if not vals.empty and len(vals) > 1 else 1.0

    st.markdown(
        f"<div class='pe-card' style='padding:12px; margin-bottom:12px'>"
        f"<div style='font-weight:700;margin-bottom:8px;color:{TZ['text']}'>"
        f"🗓️ {calmod.month_name[month]} {year}</div>",
        unsafe_allow_html=True,
    )
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Day Names Header
    cols_dn = st.columns(7)
    for i, dn in enumerate(day_names):
        with cols_dn[i]:
            st.markdown(
                f"<div style='text-align:center;font-size:11px;font-weight:600;color:{TZ['muted']}'>{dn[0]}</div>",
                unsafe_allow_html=True,
            )

    # Calendar Grid
    for week in calmod.monthcalendar(year, month):
        cols = st.columns(7)
        for i, dom in enumerate(week):
            with cols[i]:
                if dom == 0:
                    # Placeholder for days outside the month
                    st.markdown(
                        "<div style='aspect-ratio:1/1; max-width: clamp(48px, 6.5vw, 92px); max-height: clamp(48px, 6.5vw, 92px); margin:0 auto;'></div>",
                        unsafe_allow_html=True,
                    )
                    continue
                d = date(year, month, dom)
                r = float(mdf.loc[d, "day_R_net"]) if d in mdf.index else 0.0

                # Apply color and style based on R-multiple
                bg = _color_for_r(r, vmax) if r != 0 else TZ["card"]
                border_style = (
                    f"2px solid {TZ['primary']}"
                    if d == date.today()
                    else f"1px solid {TZ['border']}"
                )

                content = f"<div class='cal-day-header' style='color:{TZ['text']}'>{d.day}</div>"
                if r > 0:
                    content += f"<div style='text-align:center;'><span class='tz-badge-green'>{r:+.1f} R</span></div>"
                elif r < 0:
                    content += f"<div style='text-align:center;'><span class='tz-badge-red'>{r:.1f} R</span></div>"
                else:
                    content += f"<div style='text-align:center;'><span style='font-size:10px; color:{TZ['muted']}'>—</span></div>"

                st.markdown(
                    f"<div class='cal-day' style='background:{bg}; border:{border_style};'>{content}</div>",
                    unsafe_allow_html=True,
                )
    st.markdown("</div>", unsafe_allow_html=True)


# =========
# Main App
# =========

def merge_and_process_data(
    master: pd.DataFrame, daily_tb: pd.DataFrame, risk_unit: float
) -> pd.DataFrame:
    # Merge daily P&L with master tags and calculate R-multiple
    df = daily_tb.copy()
    if df.empty:
        return pd.DataFrame()

    # ---------- HARDEN MASTER COLUMNS ----------
    master = master.copy()

    # normalize headers (catch 'date', 'DATE', etc.)
    lower_to_actual = {c.lower(): c for c in master.columns}
    if "Date" not in master.columns:
        if "date" in lower_to_actual:
            master.rename(columns={lower_to_actual["date"]: "Date"}, inplace=True)
        else:
            master["Date"] = pd.NaT

    if "symbol_std" not in master.columns:
        if "symbol_std" in lower_to_actual:
            master.rename(columns={lower_to_actual["symbol_std"]: "symbol_std"}, inplace=True)
        elif "symbol" in lower_to_actual:
            master.rename(columns={lower_to_actual["symbol"]: "symbol_std"}, inplace=True)
        elif "tradingsymbol" in lower_to_actual:
            master.rename(columns={lower_to_actual["tradingsymbol"]: "symbol_std"}, inplace=True)
        else:
            master["symbol_std"] = "TATAMOTORS"

    master["Date"] = pd.to_datetime(master["Date"], errors="coerce").dt.date
    master["symbol_std"] = master["symbol_std"].astype(str).str.upper().str.strip()

    for c in [
        "PrevDayContext","GapType","OpenLocation","FirstCandleType",
        "OpeningTrend","RangeStatus","Result",
    ]:
        if c not in master.columns:
            master[c] = "N/A"
        else:
            master[c] = master[c].astype(str).str.strip().replace("", "N/A")

    # ---------- LEFT (TRADEBOOK) ----------
    df["pnl_final"] = pd.to_numeric(df["pnl_gross_est"], errors="coerce")
    df["day_R_net"] = df["pnl_final"] / float(risk_unit)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["symbol_std"] = df["symbol_std"].astype(str).str.upper().str.strip()

    # ---------- MERGE ----------
    master_idx = master.set_index(["Date", "symbol_std"], drop=False)
    j = df.merge(
        master_idx,
        left_on=["trade_date", "symbol_std"],
        right_index=True,
        how="left",
    )
    
    # ---------- FINALIZE ----------
    j["Date"] = j["Date"].where(pd.notna(j["Date"]), j["trade_date"])
    j["Date"] = pd.to_datetime(j["Date"], errors="coerce").dt.date
    for c in [
        "PrevDayContext","GapType","OpenLocation","FirstCandleType",
        "OpeningTrend","RangeStatus","Result",
    ]:
        if c in j.columns:
            j[c] = j[c].fillna("N/A")

    return j
    
def render_journal() -> None:
    global TZ
    # Use session state to manage theme if available, otherwise default to light
    TZ = THEMES[st.session_state.get("_pe_theme_name", "light")]
    inject_css()

    st.markdown(
        "<div class='app-title'>Trading Performance Dashboard</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div class='app-subtle'>Professional-grade analysis of your gross P&L and Master Tagged patterns.</div>",
        unsafe_allow_html=True,
    )

    # --- Data Loading ---
    latest_dir = Path(str(CONFIG["LATEST_DIR"]))
    master_path = Path(str(CONFIG["MASTER_DEFAULT_PATH"]))
    tradebook_path = _find_tradebook(latest_dir)

    if not tradebook_path:
        st.error(
            f"Tradebook not found in `{latest_dir}`. Please add your `tradebook.csv` file."
        )
        return

    tb_all = parse_tradebook_csv(tradebook_path)
    master = load_master(str(master_path))

    if tb_all.empty:
        st.warning("No valid trades found in the tradebook file after parsing.")
        return

    j_all = merge_and_process_data(master, tb_all, float(CONFIG["RISK_UNIT"]))
    if j_all.empty:
        st.warning(
            "No data to display. This might happen if there are no trades or issues with date formats."
        )
        return

    # --- Filters ---
    st.markdown("#### Filter Trades")
    filter_container = st.container()
    with filter_container:
        c1, c2, c3 = st.columns([1.5, 2.5, 3])

        sym_list = sorted(j_all["symbol_std"].unique())
        symbol_choice = c1.selectbox("Symbol", ["ALL"] + sym_list, index=0)

        j_filtered = (
            j_all
            if symbol_choice == "ALL"
            else j_all[j_all["symbol_std"] == symbol_choice].copy()
        )

        dmin, dmax = j_filtered["Date"].min(), j_filtered["Date"].max()

        current_range = st.session_state.get(
            f"_date_range_{symbol_choice}", (dmin, dmax)
        )

        # Ensure the current range is within the min/max bounds after filtering by symbol
        current_range = (max(current_range[0], dmin), min(current_range[1], dmax))

        date_range = c2.date_input(
            "Time Period",
            value=current_range,
            min_value=dmin,
            max_value=dmax,
            key="_date_range_picker",
            help=f"Data available from {dmin.strftime('%d %b %Y')} to {dmax.strftime('%d %b %Y')}",
        )

        # --- Filtering Logic ---
        if len(date_range) == 2:
            start_date, end_date = sorted(date_range)
            # Persist the selected date range for the current symbol
            st.session_state[f"_date_range_{symbol_choice}"] = (start_date, end_date)

            jv = (
                j_filtered[
                    (j_filtered["Date"] >= start_date)
                    & (j_filtered["Date"] <= end_date)
                ]
                .copy()
                .sort_values("Date")
            )
        else:
            # Handle the case where the user might clear one date (use the full range for now)
            start_date, end_date = dmin, dmax
            jv = j_filtered.copy().sort_values("Date")

    if jv.empty:
        st.warning("No trades found matching the selected filters.")
        return

    # --- Calculate All Metrics for Filtered Data ---
    kpi_results = kpis(jv, float(CONFIG["RISK_UNIT"]))
    pro_metrics = calculate_pro_indicators(jv, float(CONFIG["RISK_UNIT"]))
    total_trades_n = jv["trades_n"].sum() if not jv.empty else 0
    all_metrics = kpi_results | pro_metrics | {"total_trades_n": total_trades_n}

    # --- Dashboard Tabs (Key Metrics, Equity Curve, Weekly Alpha, Pattern Analysis, Calendar, Daily Log) ---
    tab = st.tabs(["📊 All Indicators"])[0]

    with tab:
        st.markdown("### 📈 Core Performance Indicators")
        st.markdown("<div class='metric-row'>", unsafe_allow_html=True)

        # Row 1: P&L and Risk-Adjusted Metrics (6 columns)
        c_kpis = st.columns(6, gap="small")
        with c_kpis[0]:
            # Net P&L (Rupees)
            render_kpi_card(
                "Net P&L (Gross)",
                all_metrics["net_p"],
                f"Over {all_metrics['days']} trading days",
                "₹{:,.0f}",
                "💰",
            )
        with c_kpis[1]:
            # Net R-Multiple
            render_kpi_card(
                "Net R-Multiple",
                all_metrics["net_r"],
                "Total R-multiple achieved in period",
                "{:,.2f} R",
                "🎯",
            )
        with c_kpis[2]:
            # Win Rate (%)
            render_kpi_card(
                "Win Rate",
                all_metrics["win_rate"],
                f"Avg Daily Expectancy: {all_metrics['expectancy']:+.2f} R",
                "{:,.1f}%",
                "✅",
            )
        with c_kpis[3]:
            # Profit Factor
            render_kpi_card(
                "Profit Factor (PF)",
                all_metrics["pf"],
                f"Gross Win / Gross Loss Ratio",
                "{:,.2f}",
                "⚖️",
            )
        with c_kpis[4]:
            # Max Drawdown (R)
            render_kpi_card(
                "Max Drawdown (R)",
                all_metrics["mdd_r"],
                "Worst cumulative loss (R) from peak",
                "{:,.2f} R",
                "📉",
            )
        with c_kpis[5]:
            # Calmar Ratio
            render_kpi_card(
                "Calmar Ratio",
                all_metrics["calmar"],
                "CAGR (R) / Max Drawdown (R)",
                "{:,.2f}",
                "🔥",
            )

        # Row 2: Secondary & Volatility Metrics (6 columns)
        st.markdown("", unsafe_allow_html=True)
        c_pro = st.columns(6, gap="small")
        with c_pro[0]:
            # Sharpe Ratio
            render_kpi_card(
                "Sharpe Ratio",
                all_metrics["sharpe"],
                "Risk-adjusted return vs. volatility",
                "{:,.2f}",
                "🛡️",
            )
        with c_pro[1]:
            # Sortino Ratio
            render_kpi_card(
                "Sortino Ratio",
                all_metrics["sortino"],
                "Penalizes only downside volatility",
                "{:,.2f}",
                "⬇️",
            )
        with c_pro[2]:
            # Avg Win R
            render_kpi_card(
                "Avg Win R",
                all_metrics["avg_win_r"],
                f"Best Day: {all_metrics['best'] / float(CONFIG['RISK_UNIT']):+.2f} R",
                "{:,.2f} R",
                "⏫",
            )
        with c_pro[3]:
            # Avg Loss R
            render_kpi_card(
                "Avg Loss R",
                all_metrics["avg_loss_r"],
                f"Worst Day: {all_metrics['worst'] / float(CONFIG['RISK_UNIT']):+.2f} R",
                "{:,.2f} R",
                "⏬",
            )
        with c_pro[4]:
            # Total Trades Count
            render_kpi_card(
                "Total Trades",
                all_metrics["total_trades_n"],
                "Count of individual trades in period",
                "{:,.0f}",
                "🔄",
            )
        with c_pro[5]:
            # Risk Unit (R)
            risk_capital = float(CONFIG["RISK_UNIT"])
            render_kpi_card(
                "Risk Unit (R)",
                risk_capital,
                "Base risk amount used for calculation",
                "₹{:,.0f}",
                "🚨",
            )

        st.markdown("<hr style='margin: 1.5rem 0;'>", unsafe_allow_html=True)
        st.markdown("### 📊 Equity Curve & Monthly P&L")
        render_main_chart(jv)

    with tab:
        render_weekly_alpha_analysis(jv)

    with tab:
        render_best_patterns_analysis(jv)

    with tab:
        st.markdown("### 🗓️ Calendar Heatmap")
        st.markdown(
            "Each day is colored based on the Net R-multiple achieved. Green for wins, Red for losses. Intensity indicates magnitude."
        )
        render_calendar_board(jv, start_date, end_date)

    with tab:
        st.markdown("### 📜 Daily Trade Log (Filtered)")
        display_df = jv.sort_values("Date", ascending=False).copy()

        # Formatting
        format_dict = {
            "sell_value": "₹{:,.0f}",
            "buy_value": "₹{:,.0f}",
            "pnl_final": "₹{:,.0f}",
            "day_R_net": "{:+.2f} R",
            "trades_n": "{:,.0f}",
        }

        def color_pnl(val):
            color = TZ["green"] if val > 0 else TZ["red"] if val < 0 else TZ["muted"]
            return f"color: {color}; font-weight: 600;"

        display_cols = [
            "Date",
            "symbol_std",
            "pnl_final",
            "day_R_net",
            "trades_n",
            "OpeningTrend",
            "Result",
            "GapType",
            "PrevDayContext",
            "OpenLocation",
            "FirstCandleType",
        ]
        st.dataframe(
            display_df[display_cols]
            .style.format(format_dict)
            .applymap(color_pnl, subset=["pnl_final", "day_R_net"]),
            use_container_width=True,
        )

        csv_export = display_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Export Filtered Log (CSV)",
            csv_export,
            f"trading_journal_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv",
            "text/csv",
            key="download-csv",
        )


# Execute the main function if this file is run directly
if __name__ == "__main__":
    render_journal()
