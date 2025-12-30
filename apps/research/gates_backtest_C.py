import argparse, zipfile
import pandas as pd
import numpy as np

DAILY_COST = 500.0
SL_THRESHOLD = -9500

def load_long(zip_path: str) -> pd.DataFrame:
    rows = []
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith("_backtest.csv")]
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
    required = {"Pick","Skip","PNL_R2","OpeningTrend","OpenLocation","PrevDayContext","Confidence%"}
    missing = required - set(out.columns)
    if missing:
        raise RuntimeError(f"Missing expected cols: {sorted(missing)}")
    return out

def risk_day(drow):
    # Same stable structural triggers as #10 (keep simple)
    if drow["active_count"] >= 9:
        return True
    if drow["OpeningTrend_BEAR"] >= 5 and drow["active_count"] >= 8:
        return True
    if drow["PrevDayContext_BULL"] >= 5 and drow["active_count"] >= 8:
        return True
    return False

def build_daily_features(long):
    long["active"] = long["Pick"].isin(["BULL","BEAR"]) & long["Skip"].isna() & long["PNL_R2"].notna()
    long["sl_hit"] = long["active"] & (long["PNL_R2"] <= SL_THRESHOLD)
    long["ot_align"] = long["active"] & long["OpeningTrend"].isin(["BULL","BEAR"]) & (long["Pick"] == long["OpeningTrend"])
    g = long.groupby("Date", sort=True)
    d = pd.DataFrame(index=g.size().index)
    d["active_count"] = g["active"].sum()
    d["sl_count"] = g["sl_hit"].sum()
    d["ot_align_count"] = g["ot_align"].sum()
    for tag, vals in [
        ("OpeningTrend", ["BULL","BEAR","TR"]),
        ("OpenLocation", ["OOH","OOL","IOH","IOL"]),
        ("PrevDayContext", ["BULL","BEAR","NEUTRAL"]),
    ]:
        for v in vals:
            d[f"{tag}_{v}"] = g.apply(lambda x: (x["active"] & (x[tag]==v)).sum())
    d = d.fillna(0)
    d["risk_day"] = d.apply(risk_day, axis=1)
    return d

def portfolio_net_for_day(day_df, traded_syms):
    # mean PNL across traded symbols (active only), minus ₹500 if traded
    sub = day_df[day_df["symbol"].isin(traded_syms) & day_df["active"]]
    if len(sub) == 0:
        return 0.0
    return float(sub["PNL_R2"].mean() - DAILY_COST)

def select_syms(day_df, mode, k):
    # day_df already filtered to this date
    active = day_df[day_df["active"]].copy()
    if len(active) <= k:
        return active["symbol"].tolist()

    if mode == "otalign":
        active["rank_key"] = active["ot_align"].astype(int)
        # break ties by Confidence%
        active["rank_key2"] = active["Confidence%"].fillna(0)
        active = active.sort_values(["rank_key","rank_key2"], ascending=False)
        return active["symbol"].head(k).tolist()

    if mode == "conf":
        active = active.sort_values("Confidence%", ascending=False)
        return active["symbol"].head(k).tolist()

    raise ValueError("mode must be 'otalign' or 'conf'")

def run(zip_path, k=6, mode="otalign"):
    long = load_long(zip_path)
    long["active"] = long["Pick"].isin(["BULL","BEAR"]) & long["Skip"].isna() & long["PNL_R2"].notna()
    long["ot_align"] = long["active"] & long["OpeningTrend"].isin(["BULL","BEAR"]) & (long["Pick"] == long["OpeningTrend"])

    daily_feat = build_daily_features(long)

    dates = sorted(long["Date"].unique())
    out = []
    for dt in dates:
        day = long[long["Date"] == dt].copy()
        feat = daily_feat.loc[dt]

        # baseline = all active symbols
        base_syms = day[day["active"]]["symbol"].tolist()
        base_net = portfolio_net_for_day(day, base_syms)
        base_R = base_net / 10000.0

        # gate C: if risk_day -> cap to k
        if bool(feat["risk_day"]):
            syms = select_syms(day, mode=mode, k=k)
        else:
            syms = base_syms

        gated_net = portfolio_net_for_day(day, syms)
        gated_R = gated_net / 10000.0

        out.append({
            "Date": dt,
            "risk_day": int(feat["risk_day"]),
            "base_R": base_R,
            "gated_R": gated_R,
            "base_active": len(base_syms),
            "gated_active": len(syms),
        })

    df = pd.DataFrame(out).sort_values("Date")
    return df

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

def main(zip_path, k, mode):
    df = run(zip_path, k=k, mode=mode)

    for label, col in [("Baseline", "base_R"), (f"GateC cap{k} ({mode})", "gated_R")]:
        mu, sh, so = stats(df[col])
        mdd = maxdd(df[col])
        p05 = float((df[col] <= -0.5).mean())
        p07 = float((df[col] <= -0.7).mean())
        print(f"\n=== {label} ===")
        print("MeanR/day:", mu, "Sharpe:", sh, "Sortino:", so, "MaxDD_R:", mdd)
        print("P(R<=-0.5):", p05, "P(R<=-0.7):", p07)

    # sanity: how often we cap
    capped = int((df["gated_active"] < df["base_active"]).sum())
    print("\nCapped days:", capped, "out of", len(df))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True)
    ap.add_argument("--k", type=int, default=6)
    ap.add_argument("--mode", choices=["otalign","conf"], default="otalign")
    args = ap.parse_args()
    main(args.zip, args.k, args.mode)
