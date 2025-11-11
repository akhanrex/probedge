# probedge/core/stats.py
import numpy as np
import pandas as pd
from typing import Tuple, Optional
from .rules import DEFAULT_TAG_COLS, NA_SENTINEL, _canon_tag_value


def _stable_ref_date(df_master: pd.DataFrame) -> pd.Timestamp:
    if not df_master.empty and "Date" in df_master.columns:
        return df_master["Date"].max().normalize()
    return pd.Timestamp.today().normalize()


def _norm_tags_frame(
    df: pd.DataFrame, cols=DEFAULT_TAG_COLS, na_value=NA_SENTINEL
) -> pd.DataFrame:
    g = df.copy()
    g[cols] = g[cols].astype(object).where(g[cols].notna(), na_value)
    return g


def _canonize_tags_frame(df: pd.DataFrame) -> pd.DataFrame:
    g = df.copy()
    for c in DEFAULT_TAG_COLS:
        if c in g.columns:
            g[c] = g[c].apply(lambda v: _canon_tag_value(c, v))
    return g


def _result_onehot(s: pd.Series) -> pd.DataFrame:
    r = s.fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
    
    # accept both old and new label styles
    bull = r.str.contains(r"(BULLLEG|BULLTREND|^BULL$)", regex=True, na=False)
    bear = r.str.contains(r"(BEARLEG|BEARTREND|^BEAR$)", regex=True, na=False)
    tr   = r.str.contains(r"(TRADINGRANGE|^RANGE$|^TR$)", regex=True, na=False)
    both = bull & bear
    bull = bull & ~both
    bear = bear & ~both
    bull_w = pd.Series(
        np.where(bull & ~tr, 1.00, np.where(bull & tr, 0.70, 0.0)),
        index=s.index,
        dtype=float,
    )
    bear_w = pd.Series(
        np.where(bear & ~tr, 1.00, np.where(bear & tr, 0.70, 0.0)),
        index=s.index,
        dtype=float,
    )
    pure_range = ~bull & ~bear & tr
    unknown = ~bull & ~bear & ~tr
    bull_w = bull_w.where(~pure_range, 0.45).where(~unknown, 0.50)
    bear_w = bear_w.where(~pure_range, 0.45).where(~unknown, 0.50)
    return pd.DataFrame({"bull_w": bull_w, "bear_w": bear_w}, index=s.index)


def _eff_weights_by_recency_from_ref(
    dates: pd.Series, half_life_days: float, ref_date: pd.Timestamp
) -> np.ndarray:
    if dates is None or dates.empty:
        return np.array([], dtype=float)
    age = (ref_date - dates.dt.normalize()).dt.days.clip(lower=0).astype(float)
    return np.power(0.5, age / half_life_days).values


def _neighbors_5(
    df_norm: pd.DataFrame, anchor_sig: Tuple[str, str, str, str, str], max_dist: int = 2
) -> pd.DataFrame:
    X = df_norm[DEFAULT_TAG_COLS].astype(str).apply(lambda s: s.str.upper()).to_numpy()
    anchor = np.array([s.upper() for s in anchor_sig], dtype=str)[None, :]
    hamm = (X != anchor).sum(axis=1)
    mask = hamm <= max_dist
    out = df_norm.loc[mask].copy()
    out["_hamm"] = hamm[mask]
    return out


def probedge_adv_from_results(
    df_view: pd.DataFrame,
    *,
    half_life_days: float = 365.0,
    max_dist: int = 2,
    alpha: float = 0.50,
    beta: float = 0.25,
    gamma: float = 0.15,
    ref_date: Optional[pd.Timestamp] = None,
) -> dict:
    need_cols = DEFAULT_TAG_COLS + ["Date", "Result"]
    if df_view.empty or any(c not in df_view.columns for c in need_cols):
        return {"bull_pct": 50.0, "bear_pct": 50.0, "n_eff": 0.0, "pB": 0.5, "pR": 0.5}

    g = _canonize_tags_frame(_norm_tags_frame(df_view))
    if ref_date is None:
        ref_date = _stable_ref_date(df_view)

    w_all = pd.Series(
        _eff_weights_by_recency_from_ref(g["Date"], half_life_days, ref_date),
        index=g.index,
    )
    sig_series = g[DEFAULT_TAG_COLS].astype(str).agg("|".join, axis=1)
    modes = sig_series.value_counts()
    anchor_key = modes.index[0]
    if len(modes) > 1 and modes.iloc[1] == modes.iloc[0]:
        top_freq = modes.iloc[0]
        top_keys = modes[modes == top_freq].index.tolist()
        best_key, best_w = anchor_key, -1.0
        for k in top_keys:
            s = float(w_all.loc[sig_series == k].sum())
            if s > best_w:
                best_w, best_key = s, k
        anchor_key = best_key
    anchor_sig = tuple(anchor_key.split("|"))

    neigh = _neighbors_5(g, anchor_sig, max_dist=max_dist)
    if len(neigh) == 0:
        neigh = g.assign(_hamm=2)
    kern = neigh["_hamm"].map({0: 1.0, 1: 0.5, 2: 0.2}).values
    w_neigh = w_all.loc[neigh.index].values * kern

    oh_all = _result_onehot(g["Result"])
    oh_neigh = oh_all.reindex(neigh.index).fillna({"bull_w": 0.50, "bear_w": 0.50})

    def _wr(oh_df, w, col):
        wins = float((w * oh_df[col].values).sum())
        n_eff = float(w.sum())
        p = wins / n_eff if n_eff > 0 else 0.5
        return p, n_eff

    p5_bull, n5 = _wr(oh_neigh, w_neigh, "bull_w")
    p5_bear, _ = _wr(oh_neigh, w_neigh, "bear_w")

    def _best_parent(col):
        best_p, best_n = 0.5, 0.0
        for i in range(5):
            m = pd.Series(True, index=g.index)
            for j, c in enumerate(DEFAULT_TAG_COLS):
                if j == i:
                    continue
                m &= g[c].astype(str) == anchor_sig[j]
            if not m.any():
                continue
            p, n = _wr(oh_all.loc[m], w_all.loc[m].values, col)
            if n > best_n:
                best_p, best_n = p, n
        return best_p, best_n

    p4_bull, n4 = _best_parent("bull_w")
    p4_bear, _ = _best_parent("bear_w")

    def _best_gp(col):
        idxs = range(5)
        best_p, best_n = 0.5, 0.0
        for i in idxs:
            for j in idxs:
                if j <= i:
                    continue
                m = pd.Series(True, index=g.index)
                for k, c in enumerate(DEFAULT_TAG_COLS):
                    if k in (i, j):
                        continue
                    m &= g[c].astype(str) == anchor_sig[k]
                if not m.any():
                    continue
                p, n = _wr(oh_all.loc[m], w_all.loc[m].values, col)
                if n > best_n:
                    best_p, best_n = p, n
        return best_p, best_n

    p3_bull, n3 = _best_gp("bull_w")
    p3_bear, _ = _best_gp("bear_w")

    pG_bull, nG = _wr(oh_all, w_all.values, "bull_w")
    pG_bear, _ = _wr(oh_all, w_all.values, "bear_w")

    def _blend(p5, n5, p4, n4, p3, n3, pG, nG):
        num = n5 * p5 + alpha * n4 * p4 + beta * n3 * p3 + gamma * nG * pG
        den = n5 + alpha * n4 + beta * n3 + gamma * nG
        return (num / den if den > 0 else 0.5, den)

    pB, _ = _blend(p5_bull, n5, p4_bull, n4, p3_bull, n3, pG_bull, nG)
    pR, _ = _blend(p5_bear, n5, p4_bear, n4, p3_bear, n3, pG_bear, nG)

    den2 = pB + pR
    if den2 <= 0:
        bull_pct, bear_pct = 50.0, 50.0
    else:
        bull_pct = 100.0 * (pB / den2)
        bear_pct = 100.0 * (pR / den2)

    return {
        "bull_pct": round(bull_pct, 1),
        "bear_pct": round(bear_pct, 1),
        "n_eff": float(n5 + alpha * n4 + beta * n3 + gamma * nG),
        "pB": float(pB),
        "pR": float(pR),
    }


def _wilson_ci(wins: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 0.0
    p = wins / n
    denom = 1 + z**2 / n
    centre = p + z**2 / (2 * n)
    adj = z * np.sqrt((p * (1 - p) + z**2 / (4 * n)) / n)
    low = max(0.0, (centre - adj) / denom) * 100
    high = min(1.0, (centre + adj) / denom) * 100
    return round(low, 2), round(high, 2)


def refined_quality_score_advanced(
    n_eff: float, completeness: float, p_for_dir: float
) -> float:
    size = min(1.0, float(n_eff) / 40.0)
    comp = float(np.clip(completeness, 0.0, 1.0))
    n_int = max(1, int(round(float(n_eff))))
    wins_int = int(round(float(np.clip(p_for_dir, 0.0, 1.0)) * n_int))
    lo, hi = _wilson_ci(wins_int, n_int)
    width = hi - lo
    stability = max(0.0, 1.0 - min(1.0, width / 40.0))
    return round(100.0 * (0.4 * size + 0.3 * comp + 0.3 * stability), 1)
