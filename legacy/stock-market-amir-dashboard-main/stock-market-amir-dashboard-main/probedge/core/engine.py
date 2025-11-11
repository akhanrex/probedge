# probedge/core/engine.py
import math
import numpy as np
from dataclasses import dataclass
from typing import Dict, Tuple

# Thresholds pulled via a small adapter you already have in run.py.
# We'll call _get_tm_thresholds() from run.py; for core isolation you can later
# pass thresholds explicitly.


@dataclass
class SignalResult:
    decision: str
    final_edge_abs: float
    initial_edge_abs: float
    total_discount: float
    aplus_bonus: float
    penalty_breakdown: Dict[str, float]


# --- Hard gates (same as before) ---
HARD_MIN_EDGE_PP = 4.0
HARD_MIN_EFF_N = 6.0
HARD_MIN_COMPLETENESS = 0.78
HARD_MIN_QSCORE = 55.0
HARD_MAX_HEADWIND_PP = 3.0
HARD_MIN_MATCH_RATIO = 0.50

# Penalty weights (same as before)
SAFETY_MIN_EFF_N = 8.0
SAFETY_MIN_COMP = 0.85
QUAL_BENCHMARK = 75.0
PEN_EFF_MAX = 15
PEN_COMP_MAX = 15
PEN_QUAL_MAX = 10
PEN_SAMPLE_MAX = 8
PEN_FAT_MAX = 6
BONUS_APLUS = 3


def _trip_hard_gates(
    *,
    edge_pp: float,
    n_eff: float,
    completeness: float,
    q_score: float,
    fatigue_headwind_pp: float,
    n_matches: int,
    match_target: int,
) -> Tuple[bool, list]:
    reasons = []
    if edge_pp < HARD_MIN_EDGE_PP:
        reasons.append(f"edge {edge_pp:.1f}pp < {HARD_MIN_EDGE_PP}pp")
    if n_eff < HARD_MIN_EFF_N:
        reasons.append(f"eff-N {n_eff:.1f} < {HARD_MIN_EFF_N}")
    if completeness < HARD_MIN_COMPLETENESS:
        reasons.append(f"completeness {completeness:.2f} < {HARD_MIN_COMPLETENESS}")
    if q_score < HARD_MIN_QSCORE:
        reasons.append(f"q-score {q_score:.0f} < {HARD_MIN_QSCORE:.0f}")
    if fatigue_headwind_pp > HARD_MAX_HEADWIND_PP:
        reasons.append(
            f"fatigue headwind {fatigue_headwind_pp:.1f}pp > {HARD_MAX_HEADWIND_PP:.1f}pp"
        )
    if match_target > 0:
        ratio = n_matches / float(match_target)
        if ratio < HARD_MIN_MATCH_RATIO:
            reasons.append(f"match ratio {ratio:.2f} < {HARD_MIN_MATCH_RATIO:.2f}")
    return (len(reasons) > 0, reasons)


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _soft_factor(
    x: float, *, thr: float, scale: float, higher_is_better: bool = True
) -> float:
    z = (x - thr) / max(1e-9, scale)
    if not higher_is_better:
        z = -z
    return _sigmoid(z)


def _geo_mean_factors(weighted_factors: list[tuple[float, float]]) -> float:
    eps = 1e-9
    W = sum(w for _, w in weighted_factors) or 1.0
    s = 0.0
    for f, w in weighted_factors:
        f = min(max(f, eps), 1.0)
        s += w * math.log(f)
    return math.exp(s / W)


def compute_tm_tiered_signal(
    *,
    thresholds: Dict[str, float],
    effective_n_for_penalties: float,
    completeness: float,
    q_score: float,
    aplus: bool,
    bull_pct_final: float,
    bear_pct_final: float,
    fatigue_overrides: Dict[str, float],
    edge_pp: float,
    n_matches: int,
    match_target: int,
) -> SignalResult:
    th = thresholds
    depth_norm = float(th.get("depth_norm", 25.0))
    fatigue_scale = float(th.get("fatigue_scale", 10.0))
    fatigue_cap = float(th.get("fatigue_cap", 0.2))

    signal_direction = "BULL" if bull_pct_final >= bear_pct_final else "BEAR"
    initial_edge_abs = abs(float(bull_pct_final) - float(bear_pct_final))

    fat_pp = float(fatigue_overrides.get(signal_direction, 0.0))
    headwind_pp = max(0.0, -fat_pp)

    f_eff = _soft_factor(
        effective_n_for_penalties, thr=8.0, scale=2.0, higher_is_better=True
    )
    f_comp = _soft_factor(completeness, thr=0.85, scale=0.05, higher_is_better=True)
    f_q = _soft_factor(q_score, thr=60.0, scale=5.0, higher_is_better=True)
    f_hw = _soft_factor(headwind_pp, thr=0.0, scale=1.5, higher_is_better=False)
    R = _geo_mean_factors([(f_eff, 0.35), (f_q, 0.35), (f_comp, 0.2), (f_hw, 0.10)])

    E = float(bull_pct_final - bear_pct_final)
    E_shrunk = R * E

    base_prob_bull = 50.0 + 0.5 * E_shrunk
    base_prob = (
        base_prob_bull if signal_direction == "BULL" else (100.0 - base_prob_bull)
    )
    base_prob = float(np.clip(base_prob, 0.0, 100.0))

    pen_eff = 0.0
    if effective_n_for_penalties < SAFETY_MIN_EFF_N:
        pen_eff = PEN_EFF_MAX * (1 - (effective_n_for_penalties / SAFETY_MIN_EFF_N))
        pen_eff = min(max(pen_eff, 0.0), PEN_EFF_MAX)

    comp = float(np.clip(completeness, 0.0, 1.0))
    pen_comp = 0.0
    if comp < SAFETY_MIN_COMP:
        pen_comp = PEN_COMP_MAX * (1 - (comp / SAFETY_MIN_COMP))
        pen_comp = min(max(pen_comp, 0.0), PEN_COMP_MAX)

    pen_qual = 0.0
    if q_score < QUAL_BENCHMARK:
        pen_qual = PEN_QUAL_MAX * (1 - (q_score / QUAL_BENCHMARK))
        pen_qual = min(max(pen_qual, 0.0), PEN_QUAL_MAX)

    depth_ratio = (
        0.0
        if depth_norm <= 0
        else min(effective_n_for_penalties, depth_norm) / depth_norm
    )
    pen_sample = min(PEN_SAMPLE_MAX, PEN_SAMPLE_MAX * (1.0 - depth_ratio))

    fat_frac = min(1.0, headwind_pp / max(1e-9, fatigue_scale))
    pen_fat = PEN_FAT_MAX * min(fat_frac, float(fatigue_cap))

    total_discount = float(pen_eff + pen_comp + pen_qual + pen_sample + pen_fat)
    aplus_bonus = float(BONUS_APLUS if aplus else 0.0)
    final_prob = float(np.clip(base_prob - total_discount + aplus_bonus, 0.0, 100.0))

    tripped, reasons = _trip_hard_gates(
        edge_pp=initial_edge_abs,
        n_eff=effective_n_for_penalties,
        completeness=completeness,
        q_score=q_score,
        fatigue_headwind_pp=headwind_pp,
        n_matches=n_matches,
        match_target=match_target,
    )
    if tripped:
        return SignalResult(
            "ABSTAIN", 0.0, initial_edge_abs, 0.0, 0.0, {"hard_gates": reasons}
        )

    enter_T1 = float(th.get("enter_threshold", 60.0))
    enter_T2 = max(0.0, enter_T1 - 5.0)
    TIER_1_MAX_TOTAL_PENALTY = 10
    TIER_2_MAX_TOTAL_PENALTY = 25

    decision = "ABSTAIN"
    if final_prob >= enter_T1 and total_discount <= TIER_1_MAX_TOTAL_PENALTY:
        decision = f"{signal_direction}_T1"
    elif final_prob >= enter_T2 and total_discount <= TIER_2_MAX_TOTAL_PENALTY:
        decision = f"{signal_direction}_T2"

    return SignalResult(
        decision=decision,
        final_edge_abs=final_prob,
        initial_edge_abs=initial_edge_abs,
        total_discount=total_discount,
        aplus_bonus=aplus_bonus,
        penalty_breakdown={
            "eff_n": round(pen_eff, 3),
            "comp": round(pen_comp, 3),
            "qual": round(pen_qual, 3),
            "sample_depth": round(pen_sample, 3),
            "fatigue": round(pen_fat, 3),
            "reliability_R": round(float(R), 4),
            "base_prob_before_penalties": round(base_prob, 2),
        },
    )


def compute_tm_posterior_signal(
    *,
    effective_n_for_penalties: float,
    completeness: float,
    aplus: bool,
    bull_pct_final: float,
    bear_pct_final: float,
    fatigue_overrides: Dict[str, float],
    indecisive_edge_pp: float | None = None,
) -> SignalResult:
    side = "BULL" if bull_pct_final >= bear_pct_final else "BEAR"
    p_base = max(bull_pct_final, bear_pct_final) / 100.0
    f_pp = float(fatigue_overrides.get(side, 0.0))
    beta_f = 0.08
    eps = 1e-6
    p_clip = min(max(p_base, eps), 1 - eps)
    L_tilt = math.log(p_clip / (1 - p_clip)) + beta_f * f_pp
    p_tilt = 1.0 / (1.0 + math.exp(-L_tilt))

    gamma = 1.2
    kappa = 20.0
    n_eff = max(0.0, float(effective_n_for_penalties))
    c = float(np.clip(completeness, 0.0, 1.0))
    nprime = n_eff * (c**gamma)
    if aplus:
        kappa *= 0.75
    w = nprime / (nprime + kappa) if (nprime + kappa) > 0 else 0.0
    p_final = 0.5 + w * (p_tilt - 0.5)

    enter_T1 = 0.60
    enter_T2 = 0.56
    decision = "ABSTAIN"
    if p_final >= enter_T1 and w >= 0.35:
        decision = f"{side}_T1"
    elif p_final >= enter_T2 and w >= 0.25:
        decision = f"{side}_T2"
    if indecisive_edge_pp is not None and indecisive_edge_pp < 2.0:
        decision = "ABSTAIN"

    return SignalResult(
        decision=decision,
        final_edge_abs=100.0 * float(p_final),
        initial_edge_abs=abs(bull_pct_final - bear_pct_final),
        total_discount=0.0,
        aplus_bonus=0.0,
        penalty_breakdown={
            "reliability_w": round(float(w), 3),
            "nprime": round(float(nprime), 2),
            "kappa": float(kappa),
            "fatigue_tilt_pp": round(float(f_pp), 2),
            "beta_f": float(beta_f),
            "base_prob_before_penalties": round(100.0 * float(p_base), 2),
        },
    )


def compute_tm_final_signal(
    *,
    thresholds: Dict[str, float],
    df_view,  # kept for compatibility; not used here beyond fatigue calc upstream
    effective_n_for_penalties: float,
    completeness: float,
    q_score: float,
    aplus: bool,
    bull_pct_final: float,
    bear_pct_final: float,
    n_matches: int,
    match_target: int,
    fatigue_overrides: Dict[str, float] | None = None,
) -> dict:
    th = thresholds
    if df_view is None or len(df_view) == 0:
        fatigue_bull = fatigue_overrides.get("BULL", 0.0) if fatigue_overrides else 0.0
        fatigue_bear = fatigue_overrides.get("BEAR", 0.0) if fatigue_overrides else 0.0
    else:
        fatigue_bull = fatigue_overrides.get("BULL", 0.0) if fatigue_overrides else 0.0
        fatigue_bear = fatigue_overrides.get("BEAR", 0.0) if fatigue_overrides else 0.0

    if bull_pct_final >= bear_pct_final:
        cand_side = "BULL"
        P0 = float(bull_pct_final)
        fatigue_pp = float(fatigue_bull)
    else:
        cand_side = "BEAR"
        P0 = float(bear_pct_final)
        fatigue_pp = float(fatigue_bear)

    pen_eff = (
        max(
            0.0,
            (th["safety_min_eff_n"] - float(effective_n_for_penalties))
            / th["safety_min_eff_n"],
        )
        * th["pen_eff_max"]
        if th["safety_min_eff_n"] > 0
        else 0.0
    )
    comp = float(np.clip(completeness, 0.0, 1.0))
    pen_comp = (
        max(0.0, (th["safety_min_comp"] - comp) / th["safety_min_comp"])
        * th["pen_comp_max"]
    )
    pen_qual = (
        max(0.0, (th["qual_benchmark"] - float(q_score)) / th["qual_benchmark"])
        * th["pen_qual_max"]
        if th["qual_benchmark"] > 0
        else 0.0
    )
    r = min(float(n_matches) / float(match_target), 1.0) if match_target > 0 else 0.0
    pen_sample = (
        max(0.0, (th["sample_bench"] - r) / th["sample_bench"]) * th["pen_sample_max"]
    )
    pen_fatigue = max(0.0, -float(fatigue_pp) / th["fatigue_scale"]) * th["pen_fat_max"]

    total_pen = pen_eff + pen_comp + pen_qual + pen_sample + pen_fatigue
    bonus = th["bonus_aplus"] if aplus else 0.0

    prob_pct = float(np.clip(P0 - total_pen + bonus, 0.0, 100.0))
    decision = "ENTER" if prob_pct >= th["enter_threshold"] else "ABSTAIN"
    side = cand_side if decision == "ENTER" else "—"
    color = (
        "#10b981"
        if (decision == "ENTER" and side == "BULL")
        else ("#ef4444" if (decision == "ENTER" and side == "BEAR") else "#cbd5e1")
    )
    subtitle = f"ENTER {side}" if decision == "ENTER" else "ABSTAIN"

    S_dir = float(np.clip((prob_pct - 50.0) / 20.0, 0.0, 1.0))
    S_depth = float(np.clip(effective_n_for_penalties / th["depth_norm"], 0.0, 1.0))
    S_qual = float(np.clip(q_score / 100.0, 0.0, 1.0))
    S_fat = float(
        np.clip(
            1.0 - np.clip((-fatigue_pp) / th["fatigue_scale"], 0.0, th["fatigue_cap"]),
            0.0,
            1.0,
        )
    )
    score = float(
        np.clip(
            0.5 * S_dir
            + 0.2 * S_depth
            + 0.2 * S_qual
            + 0.1 * S_fat
            + (0.05 if aplus else 0.0),
            0.0,
            1.0,
        )
    )

    return {
        "decision": decision,
        "side": side,
        "score": score,
        "prob_pct": prob_pct,
        "color": color,
        "subtitle": subtitle,
        "fatigue_pp": float(fatigue_pp),
        "bull_pct": float(bull_pct_final),
        "bear_pct": float(bear_pct_final),
    }
