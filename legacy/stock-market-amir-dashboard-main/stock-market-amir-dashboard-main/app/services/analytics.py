# app/services/analytics.py
# Central analytics helpers (shared by Terminal + Live)

from __future__ import annotations
from typing import Dict, Optional
import pandas as pd

from app.config import HALF_LIFE_DAYS
from probedge.core.stats import (
    probedge_adv_from_results,
    _eff_weights_by_recency_from_ref,
    _stable_ref_date,
)


# ---------------------------------------------------------
# Fatigue trend (pp delta vs overall)
# ---------------------------------------------------------
def compute_fatigue_timeseries(
    df_view: pd.DataFrame,
    *,
    half_life_days: float = HALF_LIFE_DAYS,
    lookback_days: int = 120,
    min_points: int = 20,
    step: int = 6,
) -> pd.DataFrame:
    """
    Builds a time series of Bull/Bear 'fatigue' measured as the pp delta between
    last N days vs the overall (both recency-weighted).
    """
    if df_view is None or df_view.empty or "Date" not in df_view.columns:
        return pd.DataFrame()

    g = df_view.dropna(subset=["Date"]).sort_values("Date").copy()
    g["Date"] = pd.to_datetime(g["Date"], errors="coerce")
    g = g[g["Date"].notna()]
    if g.empty:
        return pd.DataFrame()

    uniq_dates = g["Date"].drop_duplicates().sort_values()
    rows = []
    for i, ref in enumerate(uniq_dates):
        if (i % max(1, step)) != 0:
            continue
        up_to_ref = g[g["Date"] <= ref]
        if len(up_to_ref) < min_points:
            continue

        overall = probedge_adv_from_results(
            up_to_ref, ref_date=ref, half_life_days=half_life_days
        )

        recent_cut = ref.normalize() - pd.Timedelta(days=lookback_days)
        recent = up_to_ref[up_to_ref["Date"] >= recent_cut]
        if recent.empty:
            continue

        recent_adv = probedge_adv_from_results(
            recent, ref_date=ref, half_life_days=half_life_days
        )

        fat_bull = 100.0 * (recent_adv["pB"] - overall["pB"])
        fat_bear = 100.0 * (recent_adv["pR"] - overall["pR"])
        rows.append(
            {
                "Date": ref.normalize(),
                "fat_bull_pp": round(fat_bull, 2),
                "fat_bear_pp": round(fat_bear, 2),
            }
        )

    return pd.DataFrame(rows)


# ---------------------------------------------------------
# One-shot fatigue delta for a filtered set (used by signals)
# ---------------------------------------------------------
def fatigue_delta_pp_adv(
    df_like: pd.DataFrame, side_txt: str, half_life_days: float = HALF_LIFE_DAYS
) -> float:
    """
    Return current fatigue in percentage points for 'Bull' or 'Bear' side:
    (6-month recency-weighted pSide - overall recency-weighted pSide) * 100
    """
    if df_like is None or df_like.empty or "Date" not in df_like.columns:
        return 0.0

    ref = _stable_ref_date(df_like)
    overall = probedge_adv_from_results(
        df_like, ref_date=ref, half_life_days=half_life_days
    )

    max_date = df_like["Date"].max().normalize()
    cut = max_date - pd.Timedelta(days=183)
    recent = df_like[df_like["Date"] >= cut]
    if recent.empty:
        return 0.0

    recent_adv = probedge_adv_from_results(
        recent, ref_date=ref, half_life_days=half_life_days
    )

    if str(side_txt).strip().lower().startswith("bull"):
        p_overall = overall["pB"]
        p_recent = recent_adv["pB"]
    else:
        p_overall = overall["pR"]
        p_recent = recent_adv["pR"]

    return round(100.0 * (p_recent - p_overall), 1)
