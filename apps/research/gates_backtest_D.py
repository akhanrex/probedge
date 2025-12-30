import argparse, zipfile
import pandas as pd
import numpy as np

DAILY_COST = 500.0
SL_THRESHOLD = -9500

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
            rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    required = {"Pick","Skip","PNL_R2"}
    missing = required - set(out.columns)
    if missing:
        raise RuntimeError(f"Missing expected cols: {sorted(missing)}")
    return out

def build_daily(long: pd.DataFrame) -> pd.DataFrame:
    long["active"] = long["Pick"].isin(["BULL","BEAR"]) & long["Skip"].isna() & long["PNL_R2"].notna()
    long["sl_hit"] = long["active"] & (long["PNL_R2"] <= SL_THRESHOLD)

    g = long.groupby("Date", sort=True)
    d = pd.DataFrame(index=g.size().index)

    d["active_count"] = g["active"].sum()
    d["sl_count"] = g["sl_hit"].sum()
    d["cluster5"] = (d["sl_count"] >= 5).astype(int)

    d["pnl_mean"] = g.apply(lambda x: x.loc[x["active"], "PNL_R2"].mean()).fillna(0.0)
    d["trade_day"] = (d["active_count"] > 0).astype(int)

    # baseline net (no gate)
    d["base_net"] = d["pnl_mean"] - np.where(d["trade_day"]==1, DAILY_COST, 0.0)
    d["base_R"] = d["base_net"] / 10000.0
    d["Year"] = pd.to_datetime(d.index).year
    return d

def ladder_mult(ddR: float) -> float:
    if ddR >= -0.5:
        return 1.0
    if ddR >= -1.5:
        return 0.8
    return 0.6

def apply_gate_D(d: pd.DataFrame, cooloff: bool) -> pd.DataFrame:
    out = d.copy()
    mults = []
    eq = 0.0
    peak = 0.0
    prev_cluster5 = 0

    for idx, row in out.iterrows():
        ddR = (eq - peak)  # both in R units
        m = ladder_mult(ddR)
        if cooloff and prev_cluster5 == 1:
            m = min(m, 0.6)

        # apply m to pnl_mean, cost only if we trade (m>0 and trade_day==1)
        net = (row["pnl_mean"] * m) - (DAILY_COST if (row["trade_day"]==1 and m>0) else 0.0)
        R = net / 10000.0

        eq = eq + R
        peak = max(peak, eq)

        mults.append((m, R))
        prev_cluster5 = int(row["cluster5"])

    out["mult"] = [m for m, _ in mults]
    out["R"] = [r for _, r in mults]
    out["net"] = out["R"] * 10000.0
    return out

def stats(R):
    r = R.values
    mu = float(np.mean(r))
    sd = float(np.std(r, ddof=1))
    sharpe = (mu/sd)*np.sqrt(252) if sd>1e-12 else np.nan
    neg = r[r<0]
    dsd = float(np.std(neg, ddof=1)) if len(neg)>1 else np.nan
    sortino = (mu/dsd)*np.sqrt(252) if dsd and dsd>1e-12 else np.nan
    return mu, sharpe, sortino

def maxdd(R):
    c = R.cumsum().values
    peak = -1e18
    mdd = 0.0
    for x in c:
        peak = max(peak, x)
        mdd = min(mdd, x-peak)
    return float(mdd)

def report(df, label, col="R"):
    mu, sh, so = stats(df[col])
    mdd = maxdd(df[col])
    win = float((df[col] > 0).mean())
    p05 = float((df[col] <= -0.5).mean())
    p07 = float((df[col] <= -0.7).mean())
    return {
        "Strategy": label,
        "Days": int(len(df)),
        "MeanR/day": mu,
        "Sharpe": sh,
        "Sortino": so,
        "MaxDD_R": mdd,
        "WinRate": win,
        "P(R<=-0.5)": p05,
        "P(R<=-0.7)": p07,
        "AvgMult": float(df["mult"].mean()) if "mult" in df.columns else 1.0,
    }

def main(zip_path: str):
    long = load_long(zip_path)
    daily = build_daily(long)

    base = daily.copy()
    base["R"] = base["base_R"]
    base["mult"] = 1.0

    d1 = apply_gate_D(daily, cooloff=False)
    d2 = apply_gate_D(daily, cooloff=True)

    out = pd.DataFrame([
        report(base, "Baseline (net ₹500/day)"),
        report(d1, "Gate D1 (DD ladder 1.0/0.8/0.6)"),
        report(d2, "Gate D2 (DD ladder + cluster cool-off)"),
    ])

    pd.set_option("display.width", 220)
    print("\n=== FULL PERIOD (net of ₹500/day) ===")
    print(out.to_string(index=False))

    # Recent window
    recent = daily[daily["Year"] >= 2024].copy()
    base_r = recent.copy(); base_r["R"] = base_r["base_R"]; base_r["mult"]=1.0
    d1_r = apply_gate_D(recent, cooloff=False)
    d2_r = apply_gate_D(recent, cooloff=True)

    out2 = pd.DataFrame([
        report(base_r, "Baseline 2024+"),
        report(d1_r, "Gate D1 2024+"),
        report(d2_r, "Gate D2 2024+"),
    ])
    print("\n=== 2024+ ONLY (net of ₹500/day) ===")
    print(out2.to_string(index=False))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True)
    args = ap.parse_args()
    main(args.zip)
