import argparse, zipfile
import pandas as pd
import numpy as np

from sklearn.metrics import roc_auc_score, average_precision_score, brier_score_loss
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

SL_THRESHOLD = -9500
DAILY_COST = 500.0

def load_long(zip_path: str) -> pd.DataFrame:
    rows = []
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.lower().endswith("_backtest.csv")]
        if not names:
            raise RuntimeError("No *_BACKTEST.csv files found. Run: unzip -l <zip> | grep -i _backtest.csv | head")
        for name in names:
            sym = name.split("/")[-1].replace("_BACKTEST.csv","")
            df = pd.read_csv(z.open(name))
            if "Date" not in df.columns:
                continue
            df["Date"] = pd.to_datetime(df["Date"])
            df["symbol"] = sym
            df["PNL_R2"] = pd.to_numeric(df["PNL_R2"], errors="coerce")
            df["Confidence%"] = pd.to_numeric(df["Confidence%"], errors="coerce")
            rows.append(df)
    out = pd.concat(rows, ignore_index=True)
    required = {"Pick","Skip","PNL_R2","OpeningTrend","OpenLocation","PrevDayContext","Confidence%"}
    missing = required - set(out.columns)
    if missing:
        raise RuntimeError(f"Missing columns: {sorted(missing)}")
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

    d = d.fillna(0)
    d["pnl_mean"] = g.apply(lambda x: x.loc[x["active"], "PNL_R2"].mean()).fillna(0)
    d["pnl_net"] = d["pnl_mean"] - np.where(d["active_count"]>0, DAILY_COST, 0.0)
    d["r_net"] = d["pnl_net"] / 10000.0
    d["cluster5"] = (d["sl_count"] >= 5).astype(int)

    d["Year"] = pd.to_datetime(d.index).year
    return d

def top_decile_lift(y_true, y_prob):
    # fraction of all positives captured within top 10% predicted risk days
    if y_true.sum() == 0:
        return np.nan
    k = max(1, int(0.10 * len(y_true)))
    idx = np.argsort(-y_prob)[:k]
    return float(y_true.iloc[idx].sum() / y_true.sum())

def main(zip_path: str):
    long = load_long(zip_path)
    daily = build_daily(long)

    # Pre-trade features (exclude sl_count + pnl fields + targets)
    drop = {"pnl_mean","pnl_net","r_net","cluster5","sl_count"}
    feat_cols = [c for c in daily.columns if c not in drop and c != "Year"]
    X = daily[feat_cols]
    y_cl = daily["cluster5"]
    y_r  = daily["r_net"]
    years = sorted(daily["Year"].unique())

    print("\nYears:", years)
    print("Total days:", len(daily), "Cluster5 rate:", float(y_cl.mean()))

    # Walk-forward yearly
    rows = []
    coef_track = {}

    for y in years:
        # train < y, test == y
        tr = daily["Year"] < y
        te = daily["Year"] == y
        if tr.sum() < 200 or te.sum() < 30:
            continue

        Xtr, Xte = X.loc[tr], X.loc[te]
        ytr, yte = y_cl.loc[tr], y_cl.loc[te]
        rtr, rte = y_r.loc[tr], y_r.loc[te]

        # Cluster model: Logit L1
        logit = Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(penalty="l1", solver="saga", C=0.6,
                                       class_weight="balanced", max_iter=3000))
        ])
        logit.fit(Xtr, ytr)
        p = logit.predict_proba(Xte)[:,1]

        auc = roc_auc_score(yte, p) if len(np.unique(yte)) > 1 else np.nan
        pr  = average_precision_score(yte, p) if yte.sum() > 0 else np.nan
        br  = brier_score_loss(yte, p) if len(np.unique(yte)) > 1 else np.nan
        lift = top_decile_lift(yte, pd.Series(p, index=yte.index))

        coefs = pd.Series(logit.named_steps["clf"].coef_[0], index=feat_cols)
        for k in ["active_count","ot_align_count","OpeningTrend_TR","OpeningTrend_BEAR",
                  "OpenLocation_OOL","OpenLocation_OOH","PrevDayContext_BULL","PrevDayContext_BEAR",
                  "conf_mean","conf_std"]:
            if k in coefs.index:
                coef_track.setdefault(k, []).append((y, float(coefs[k])))

        # Expectancy model (pre-trade): Ridge
        ridge = Pipeline([("scaler", StandardScaler()), ("reg", Ridge(alpha=10.0))])
        ridge.fit(Xtr, rtr)
        pred = ridge.predict(Xte)
        corr = float(np.corrcoef(pred, rte)[0,1]) if np.std(pred) > 1e-9 and np.std(rte) > 1e-9 else np.nan

        # rank utility: realized mean of top 20% predicted days
        k20 = max(1, int(0.20 * len(pred)))
        top20_idx = np.argsort(-pred)[:k20]
        top20_mean = float(rte.iloc[top20_idx].mean())
        all_mean = float(rte.mean())

        rows.append({
            "Year": y,
            "TestDays": int(te.sum()),
            "Cluster5Count": int(yte.sum()),
            "AUC": auc,
            "PR_AUC": pr,
            "Brier": br,
            "Top10_Lift": lift,
            "RidgeCorr": corr,
            "Top20_R_mean": top20_mean,
            "All_R_mean": all_mean,
        })

    out = pd.DataFrame(rows).sort_values("Year")
    pd.set_option("display.width", 180)
    pd.set_option("display.max_columns", 50)

    print("\n=== Walk-forward yearly metrics ===")
    print(out.to_string(index=False))

    print("\n=== Coefficient sign stability (key features) ===")
    for k, vals in coef_track.items():
        arr = np.array([v for _, v in vals], dtype=float)
        pos = np.mean(arr > 0)
        neg = np.mean(arr < 0)
        print(f"{k:18s}  pos%={pos:0.2f}  neg%={neg:0.2f}  n={len(arr)}  last={arr[-1]:+.4f}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True)
    args = ap.parse_args()
    main(args.zip)
