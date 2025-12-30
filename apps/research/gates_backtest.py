import argparse, zipfile
import pandas as pd
import numpy as np

DAILY_COST = 500.0
SL_THRESHOLD = -9500  # for cluster stats only

def load_long(zip_path: str) -> pd.DataFrame:
    rows = []
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith("_backtest.csv")]
        if not names:
            raise RuntimeError("No *_BACKTEST.csv found. Run: unzip -l <zip> | grep -i _backtest.csv | head")
        for name in names:
            sym = name.split("/")[-1].replace("_BACKTEST.csv","")
            df = pd.read_csv(z.open(name))
            if "Date" not in df.columns:
                continue
            df["Date"] = pd.to_datetime(df["Date"])
            df["symbol"] = sym
            df["PNL_R2"] = pd.to_numeric(df["PNL_R2"], errors="coerce")
            df["Confidence%"] = pd.to_numeric(df.get("Confidence%", np.nan), errors="coerce")
            rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    required = {"Pick","Skip","PNL_R2","OpeningTrend","OpenLocation","PrevDayContext"}
    missing = required - set(out.columns)
    if missing:
        raise RuntimeError(f"Missing expected cols: {sorted(missing)}")
    return out

def build_daily(long: pd.DataFrame) -> pd.DataFrame:
    long["active"] = long["Pick"].isin(["BULL","BEAR"]) & long["Skip"].isna() & long["PNL_R2"].notna()
    long["sl_hit"] = long["active"] & (long["PNL_R2"] <= SL_THRESHOLD)
    long["ot_align"] = long["active"] & long["OpeningTrend"].isin(["BULL","BEAR"]) & (long["Pick"] == long["OpeningTrend"])

    g = long.groupby("Date", sort=True)
    d = pd.DataFrame(index=g.size().index)

    d["active_count"] = g["active"].sum()
    d["ot_align_count"] = g["ot_align"].sum()
    d["sl_count"] = g["sl_hit"].sum()

    # tag breadth counts
    for tag, vals in [
        ("OpeningTrend", ["BULL","BEAR","TR"]),
        ("OpenLocation", ["OOH","OOL","IOH","IOL"]),
        ("PrevDayContext", ["BULL","BEAR","NEUTRAL"]),
    ]:
        for v in vals:
            d[f"{tag}_{v}"] = g.apply(lambda x: (x["active"] & (x[tag]==v)).sum())

    # baseline portfolio pnl @10k risk model = mean per active symbol-day
    d["pnl_mean"] = g.apply(lambda x: x.loc[x["active"], "PNL_R2"].mean()).fillna(0.0)

    d["trade_day"] = (d["active_count"] > 0).astype(int)
    d["base_net"] = d["pnl_mean"] - np.where(d["trade_day"]==1, DAILY_COST, 0.0)  # ₹500/day cost
    d["base_R"] = d["base_net"] / 10000.0
    d["cluster5"] = (d["sl_count"] >= 5).astype(int)

    d["Year"] = pd.to_datetime(d.index).year
    return d

def gate_A(row):
    # Conservative throttle
    m = 1.0
    if row["active_count"] >= 9:
        m = 0.8
    elif row["active_count"] >= 8 and row["OpeningTrend_BEAR"] >= 5:
        m = 0.8
    elif row["active_count"] >= 8 and row["PrevDayContext_BULL"] >= 5:
        m = 0.8
    return m

def gate_B(row, boost=False):
    # Aggressive throttle
    m = 1.0
    if row["active_count"] == 8:
        m = 0.8
    if row["active_count"] >= 9:
        m = 0.6
    if row["active_count"] >= 8 and row["OpeningTrend_BEAR"] >= 5:
        m = min(m, 0.6)
    if row["active_count"] >= 8 and row["PrevDayContext_BULL"] >= 5:
        m = min(m, 0.6)

    if boost:
        if (row["ot_align_count"] >= 6) and (row["OpeningTrend_TR"] == 0) and (row["active_count"] >= 7):
            m = max(m, 1.05)

    return m

def apply_gate(d: pd.DataFrame, which: str, boost=False) -> pd.DataFrame:
    out = d.copy()
    if which == "A":
        out["mult"] = out.apply(gate_A, axis=1)
    elif which == "B":
        out["mult"] = out.apply(lambda r: gate_B(r, boost=boost), axis=1)
    else:
        raise ValueError("which must be A or B")

    # If mult == 0, we skipped day -> cost should be 0 because no trades.
    out["net"] = (out["pnl_mean"] * out["mult"]) - np.where((out["trade_day"]==1) & (out["mult"]>0), DAILY_COST, 0.0)
    out["R"] = out["net"] / 10000.0
    return out

def stats(series_R: pd.Series):
    r = series_R.values
    mu = float(np.mean(r))
    sd = float(np.std(r, ddof=1))
    sharpe = (mu / sd) * np.sqrt(252) if sd > 1e-12 else np.nan
    neg = r[r < 0]
    dd = float(np.std(neg, ddof=1)) if len(neg) > 1 else np.nan
    sortino = (mu / dd) * np.sqrt(252) if dd and dd > 1e-12 else np.nan
    return mu, sd, sharpe, sortino

def max_drawdown(cum):
    peak = -1e18
    mdd = 0.0
    for x in cum:
        peak = max(peak, x)
        mdd = min(mdd, x - peak)
    return float(mdd)

def report(d: pd.DataFrame, label: str):
    R = d["R"]
    mu, sd, sh, so = stats(R)
    cum = R.cumsum()
    mdd = max_drawdown(cum.values)
    win = float((R > 0).mean())
    p05 = float((R <= -0.5).mean())
    p07 = float((R <= -0.7).mean())
    cl5 = float(d["cluster5"].mean())
    traded = int(d["trade_day"].sum())
    # “top-day capture”: how much of baseline top 10% net days do we still participate in (mult>0)?
    base_top = d["base_R"].quantile(0.90)
    top_days = d["base_R"] >= base_top
    capture = float((d.loc[top_days, "mult"] > 0).mean())

    return {
        "Strategy": label,
        "Days": int(len(d)),
        "TradeDays": traded,
        "MeanR/day": mu,
        "Sharpe": sh,
        "Sortino": so,
        "MaxDD_R": mdd,
        "WinRate": win,
        "P(R<=-0.5)": p05,
        "P(R<=-0.7)": p07,
        "Cluster5Rate": cl5,
        "Top10%DayCapture": capture,
        "AvgMult": float(d["mult"].mean()),
    }

def main(zip_path: str, boost: bool):
    long = load_long(zip_path)
    daily = build_daily(long)

    base = daily.copy()
    base["mult"] = 1.0
    base["net"] = base["base_net"]
    base["R"] = base["base_R"]

    A = apply_gate(daily, "A", boost=False)
    B = apply_gate(daily, "B", boost=boost)

    rows = [report(base, "Baseline (net ₹500/day)"),
            report(A, "Gate A (Conservative throttle)"),
            report(B, f"Gate B (Aggressive throttle){' +Boost' if boost else ''}")]

    out = pd.DataFrame(rows)
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 50)

    print("\n=== FULL PERIOD (net of ₹500/day) ===")
    print(out.to_string(index=False))

    # Also show OOS recent window as a sanity check
    recent = daily[daily["Year"] >= 2024].copy()
    base_r = recent.copy(); base_r["mult"]=1.0; base_r["net"]=base_r["base_net"]; base_r["R"]=base_r["base_R"]
    A_r = apply_gate(recent, "A", boost=False)
    B_r = apply_gate(recent, "B", boost=boost)

    rows2 = [report(base_r, "Baseline 2024+"),
             report(A_r, "Gate A 2024+"),
             report(B_r, f"Gate B 2024+{' +Boost' if boost else ''}")]

    out2 = pd.DataFrame(rows2)
    print("\n=== 2024+ ONLY (net of ₹500/day) ===")
    print(out2.to_string(index=False))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True)
    ap.add_argument("--boost", action="store_true")
    args = ap.parse_args()
    main(args.zip, boost=args.boost)
