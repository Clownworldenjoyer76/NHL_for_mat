#!/usr/bin/env python3

import math
from pathlib import Path

import numpy as np
import pandas as pd


TEMPLATE_FILE = Path(r"C:\Users\ntmal\Downloads\dc_soccer_pricing_engine (2).csv")
MLS_HISTORY_FILE = Path("mls_master_history.csv")
OUTPUT_FILE = Path("mls_soccer_pricing_engine.csv")

MAX_GOALS = 10


def poisson_probs(lmbda: float, max_goals: int = MAX_GOALS) -> np.ndarray:
    """
    Returns probabilities for goals 0..max_goals, where the final bucket
    absorbs the remaining tail mass.
    """
    probs = np.zeros(max_goals + 1, dtype=float)
    probs[0] = math.exp(-lmbda)

    for k in range(1, max_goals):
        probs[k] = probs[k - 1] * lmbda / k

    probs[max_goals] = max(0.0, 1.0 - probs[:max_goals].sum())
    return probs


def dc_tau(i: int, j: int, lam: float, mu: float, rho: float) -> float:
    """
    Dixon-Coles low-score adjustment.
    """
    if i == 0 and j == 0:
        return 1.0 - (lam * mu * rho)
    if i == 0 and j == 1:
        return 1.0 + (lam * rho)
    if i == 1 and j == 0:
        return 1.0 + (mu * rho)
    if i == 1 and j == 1:
        return 1.0 - rho
    return 1.0


def build_score_matrix(lam_home: float, lam_away: float, rho: float, max_goals: int = MAX_GOALS) -> np.ndarray:
    home_probs = poisson_probs(lam_home, max_goals=max_goals)
    away_probs = poisson_probs(lam_away, max_goals=max_goals)

    matrix = np.outer(home_probs, away_probs)

    for i in range(min(2, max_goals + 1)):
        for j in range(min(2, max_goals + 1)):
            matrix[i, j] *= dc_tau(i, j, lam_home, lam_away, rho)

    total = matrix.sum()
    if total <= 0:
        raise ValueError(f"Non-positive matrix total for lam_home={lam_home}, lam_away={lam_away}, rho={rho}")

    matrix /= total
    return matrix


def safe_fair_odds(prob: float) -> float:
    if prob <= 0:
        return np.nan
    return 1.0 / prob


def main() -> None:
    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(f"Template file not found: {TEMPLATE_FILE}")

    if not MLS_HISTORY_FILE.exists():
        raise FileNotFoundError(f"MLS history file not found: {MLS_HISTORY_FILE}")

    template = pd.read_csv(TEMPLATE_FILE)
    mls = pd.read_csv(MLS_HISTORY_FILE)

    required_template_cols = ["lambda_home", "lambda_away", "rho"]
    missing_template = [c for c in required_template_cols if c not in template.columns]
    if missing_template:
        raise ValueError(f"Template file missing columns: {missing_template}")

    required_mls_cols = ["FTHG", "FTAG"]
    missing_mls = [c for c in required_mls_cols if c not in mls.columns]
    if missing_mls:
        raise ValueError(f"MLS history file missing columns: {missing_mls}")

    mls = mls.dropna(subset=["FTHG", "FTAG"]).copy()
    mls["FTHG"] = pd.to_numeric(mls["FTHG"], errors="coerce")
    mls["FTAG"] = pd.to_numeric(mls["FTAG"], errors="coerce")
    mls = mls.dropna(subset=["FTHG", "FTAG"]).copy()

    if mls.empty:
        raise ValueError("MLS history file has no valid FTHG/FTAG rows after cleaning.")

    mls_home_mean = mls["FTHG"].mean()
    mls_away_mean = mls["FTAG"].mean()

    template_home_mean = template["lambda_home"].mean()
    template_away_mean = template["lambda_away"].mean()

    if template_home_mean <= 0 or template_away_mean <= 0:
        raise ValueError("Template lambda means must be positive.")

    home_scale = mls_home_mean / template_home_mean
    away_scale = mls_away_mean / template_away_mean

    print(f"MLS mean home goals: {mls_home_mean:.6f}")
    print(f"MLS mean away goals: {mls_away_mean:.6f}")
    print(f"Template mean lambda_home: {template_home_mean:.6f}")
    print(f"Template mean lambda_away: {template_away_mean:.6f}")
    print(f"Home scale: {home_scale:.6f}")
    print(f"Away scale: {away_scale:.6f}")

    out_rows = []

    for row in template.itertuples(index=False):
        lam_home = float(row.lambda_home) * home_scale
        lam_away = float(row.lambda_away) * away_scale
        rho = float(row.rho)

        matrix = build_score_matrix(lam_home, lam_away, rho, max_goals=MAX_GOALS)

        home_win = np.tril(matrix, k=-1).sum()
        draw = np.trace(matrix)
        away_win = np.triu(matrix, k=1).sum()

        over2_5 = 0.0
        under2_5 = 0.0
        btts_yes = 0.0
        btts_no = 0.0

        for i in range(matrix.shape[0]):
            for j in range(matrix.shape[1]):
                p = matrix[i, j]

                if i + j >= 3:
                    over2_5 += p
                else:
                    under2_5 += p

                if i >= 1 and j >= 1:
                    btts_yes += p
                else:
                    btts_no += p

        out_rows.append(
            {
                "lambda_home": lam_home,
                "lambda_away": lam_away,
                "lambda_total": lam_home + lam_away,
                "rho": rho,
                "home_win": home_win,
                "draw": draw,
                "away_win": away_win,
                "over2_5": over2_5,
                "under2_5": under2_5,
                "btts_yes": btts_yes,
                "btts_no": btts_no,
                "home_win_fair_odds": safe_fair_odds(home_win),
                "draw_fair_odds": safe_fair_odds(draw),
                "away_win_fair_odds": safe_fair_odds(away_win),
                "over2_5_fair_odds": safe_fair_odds(over2_5),
                "under2_5_fair_odds": safe_fair_odds(under2_5),
                "btts_yes_fair_odds": safe_fair_odds(btts_yes),
                "btts_no_fair_odds": safe_fair_odds(btts_no),
            }
        )

    out = pd.DataFrame(
        out_rows,
        columns=[
            "lambda_home",
            "lambda_away",
            "lambda_total",
            "rho",
            "home_win",
            "draw",
            "away_win",
            "over2_5",
            "under2_5",
            "btts_yes",
            "btts_no",
            "home_win_fair_odds",
            "draw_fair_odds",
            "away_win_fair_odds",
            "over2_5_fair_odds",
            "under2_5_fair_odds",
            "btts_yes_fair_odds",
            "btts_no_fair_odds",
        ],
    )

    out.to_csv(OUTPUT_FILE, index=False)

    print(f"Created: {OUTPUT_FILE}")
    print(out.shape)
    print(out.head())


if __name__ == "__main__":
    main()