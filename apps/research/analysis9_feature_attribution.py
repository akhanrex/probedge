import argparse, zipfile
import pandas as pd
import numpy as np

from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.inspection import permutation_importance
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

SL_THRESHOLD = -9500
DAILY_COST = 500.0

def load_long(zip_path: str) -> pd.DataFrame:
    rows = []
    with zipfile.ZipFile(zip_path) as z:
        # auto-detect all *_BACKTEST.csv anywhere in the zip
        names = [n for n in z.namelist() if n.lower().endswith("_backtest.csv")]
        if not names:
            raise RuntimeError("No *_BACKTEST.csv files found inside the zip. Run: unzip -l <zip> | head")

        for name in names:
            sym = name.split("/")[-1].replace("_BACKTEST.csv", "")
            df = pd.read_csv(z.open(name))
            if "Date" not in df.columns:
                continue
            df["Date"] = pd.to_datetime(df["Date"])
            df["symbol"] = sym
            for col in ["PNL_R2", "Confidence%"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            rows.append(df)

    out = pd.concat(rows, ignore_index=True)
    # basic sanity
    required = {"Pick","Skip","PNL_R2","OpeningTrend","OpenLocation","PrevDayContext","Confidence%"}
    missing = required - set(out.columns)
    if missing:
        raise RuntimeError(f"Missing expected columns in backtest CSVs: {sorted(missing)}")
    return out

def build_daily(long: pd.DataFrame) -> pd.DataFrame:
    long["active"] = long["Pick"].isin(["BULL","BEAR"]) & long["Skip"].isna() & long["PNL_R2"].notna()
    long["sl_hit"] = long["active"] & (long["PNL_R2"] <= SL_THRESHOLD)
    long["ot_align"] = long["active"] & long["OpeningTrend"].isin(["BULL","BEAR"]) & (long["Pick"] == long["OpeningTrend"])

    g = long.groupby("Date", sort=True)
    d = pd.DataFrame(index=g.size().index)

    d["active_count"] = g["active"].sum()
    d["sl_count"] = g["sl_hit"].sum()
    d["ot_align_count"] = g["ot_align"].sum()
    d["bull_count"] = g.apply(lambda x: (x["active"] & (x["Pick"]=="BULL")).sum())
    d["bear_count"] = g.apply(lambda x: (x["active"] & (x["Pick"]=="BEAR")).sum())

    d["conf_mean"] = g.apply(lambda x: x.loc[x["active"], "Confidence%"].mean())
    d["conf_std"]  = g.apply(lambda x: x.loc[x["active"], "Confidence%"].std())

    for tag, vals in [
        ("OpeningTrend", ["BULL","BEAR","TR"]),
        ("OpenLocation", ["OOH","OOL","IOH","IOL"]),
        ("PrevDayContext", ["BULL","BEAR","NEUTRAL"]),
    ]:
        for v in vals:
            d[f"{tag}_{v}"] = g.apply(lambda x: (x["active"] & (x[tag]==v)).sum())

    # Net portfolio return (₹10k/day risk model) - ₹500/day cost on trade days
    d["pnl_mean"] = g.apply(lambda x: x.loc[x["active"], "PNL_R2"].mean())
    d = d.fillna(0)
    d["pnl_net"] = d["pnl_mean"] - np.where(d["active_count"]>0, DAILY_COST, 0.0)
    d["r_net"] = d["pnl_net"] / 10000.0

    d["cluster5"] = (d["sl_count"] >= 5).astype(int)
    return d

def top_perm(model, X, y, n=12):
    pi = permutation_importance(model, X, y, n_repeats=10, random_state=42)
    imp = pd.Series(pi.importances_mean, index=X.columns).sort_values(ascending=False)
    return imp.head(n)

def main(zip_path: str):
    long = load_long(zip_path)
    daily = build_daily(long)

    # Pre-trade features only (no realized pnl columns)
    drop_cols = {"pnl_mean","pnl_net","r_net","cluster5","sl_count"}
    X = daily[[c for c in daily.columns if c not in drop_cols]]
    y_cl = daily["cluster5"]
    y_reg = daily["r_net"]

    # Time split: train up to 2023, test 2024+
    split = pd.Timestamp("2024-01-01")
    tr = daily.index < split
    te = ~tr

    Xtr, Xte = X.loc[tr], X.loc[te]
    ytr, yte = y_cl.loc[tr], y_cl.loc[te]

    print("\nRows (days):", len(daily), " Train:", tr.sum(), " Test:", te.sum())
    print("Cluster5 rate (test):", float(yte.mean()))

    # Cluster model (logit L1)
    logit = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(penalty="l1", solver="saga", C=0.6, class_weight="balanced", max_iter=3000))
    ])
    logit.fit(Xtr, ytr)
    p = logit.predict_proba(Xte)[:, 1]

    print("\n=== Cluster5 Pre-trade Logit ===")
    print("AUC:", roc_auc_score(yte, p))
    print("PR-AUC:", average_precision_score(yte, p))
    print("Brier:", brier_score_loss(yte, p))

    coefs = pd.Series(logit.named_steps["clf"].coef_[0], index=Xtr.columns)
    coefs = coefs[coefs != 0].sort_values(key=np.abs, ascending=False).head(20)
    print("\nTop signed drivers (|coef|):")
    print(coefs)

    # Nonlinear check
    hgbc = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.08, max_iter=250)
    hgbc.fit(Xtr, ytr)
    print("\n=== Cluster5 Pre-trade HGB Permutation (test) ===")
    print(top_perm(hgbc, Xte, yte, n=15))

    # Regression model (net R/day)
    ytrR, yteR = y_reg.loc[tr], y_reg.loc[te]
    ridge = Pipeline([("scaler", StandardScaler()), ("reg", Ridge(alpha=10.0))])
    ridge.fit(Xtr, ytrR)
    pred = ridge.predict(Xte)
    corr = float(np.corrcoef(pred, yteR)[0, 1])
    print("\n=== Expected R/day (net) Pre-trade Ridge ===")
    print("Corr(pred, actual):", corr)

    hgbR = HistGradientBoostingRegressor(max_depth=3, learning_rate=0.08, max_iter=400)
    hgbR.fit(Xtr, ytrR)
    print("\n=== Expected R/day (net) Pre-trade HGB Permutation (test) ===")
    print(top_perm(hgbR, Xte, yteR, n=15))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True)
    args = ap.parse_args()
    main(args.zip)
