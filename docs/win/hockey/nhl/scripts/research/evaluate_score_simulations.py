#!/usr/bin/env python3
"""
Item 14 â€” NHL Monte Carlo / distributional simulation research.

Destination:
    docs/win/hockey/nhl/scripts/research/evaluate_score_simulations.py

Purpose
-------
Test alternative score-generating distributions around the SAME D-Ratings pregame
home/away projected goal means used by the current NHL pipeline. This isolates the
distributional assumption from the upstream mean model.

Current production baseline:
    independent Poisson home/away scoring
    -> Skellam puck-line probabilities
    -> Poisson total probabilities

Candidates:
    1. independent_poisson
    2. bivariate_poisson (common Poisson scoring component, when supported)
    3. negative_binomial (independent overdispersed scoring)
    4. empirical_residual (paired, centered empirical score residuals)

Validation:
    - predeclared chronological 75% train / 25% holdout split by unique game date
    - all fitted distribution parameters use TRAIN rows only
    - holdout outcomes are never used in fitting or simulation-iteration selection
    - simulation iteration stability is chosen from holdout COVARIATES only

Important:
    The 2025-26 season has been used elsewhere in the project. Therefore this is an
    Item-14 held-out block for distribution fitting, NOT a globally untouched season
    and NOT a production-promotion test.

Research only. No production files are modified.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCRIPT_VERSION = "ITEM14-NHL-SIMULATION-2026-09-12-v4"

REPO_REL = Path("docs") / "win" / "hockey" / "nhl"

DISTRIBUTIONS = (
    "independent_poisson",
    "bivariate_poisson",
    "negative_binomial",
    "empirical_residual",
)

DEFAULT_ITERATION_GRID = (5_000, 10_000, 25_000, 50_000, 100_000)
DEFAULT_BASE_SEED = 20260912
DEFAULT_HOLDOUT_FRACTION = 0.25
DEFAULT_STABILITY_GAMES = 24
DEFAULT_STABILITY_DELTA = 0.005
DEFAULT_MAX_95_HALF_WIDTH = 0.005
EPS = 1e-12


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve()]
    seen: set[Path] = set()
    for start in starts:
        for candidate in [start, *start.parents]:
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / REPO_REL).is_dir():
                return candidate
    raise RuntimeError(
        f"Could not find repository root containing {REPO_REL.as_posix()}"
    )


REPO_ROOT = find_repo_root()
NHL_ROOT = REPO_ROOT / REPO_REL

DEFAULT_PREDICTIONS = NHL_ROOT / "season_master" / "2025_2026" / "predictions.csv"
DEFAULT_SPORTSBOOK = NHL_ROOT / "season_master" / "2025_2026" / "sportsbook.csv"
DEFAULT_SCORES = NHL_ROOT / "archive" / "2025_26" / "05_final_scores" / "final_scores"
DEFAULT_OUTPUT = (
    NHL_ROOT
    / "research"
    / "model_search"
    / "item14_score_simulations"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Item 14 NHL Monte Carlo / score-distribution evaluation."
    )
    parser.add_argument("--predictions", type=Path, default=DEFAULT_PREDICTIONS)
    parser.add_argument("--sportsbook", type=Path, default=DEFAULT_SPORTSBOOK)
    parser.add_argument("--scores-dir", type=Path, default=DEFAULT_SCORES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--holdout-fraction",
        type=float,
        default=DEFAULT_HOLDOUT_FRACTION,
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_BASE_SEED,
    )
    parser.add_argument(
        "--stability-games",
        type=int,
        default=DEFAULT_STABILITY_GAMES,
    )
    parser.add_argument(
        "--stability-delta",
        type=float,
        default=DEFAULT_STABILITY_DELTA,
    )
    parser.add_argument(
        "--max-95-half-width",
        type=float,
        default=DEFAULT_MAX_95_HALF_WIDTH,
    )
    parser.add_argument(
        "--iteration-grid",
        type=str,
        default=",".join(str(v) for v in DEFAULT_ITERATION_GRID),
        help="Comma-separated increasing simulation counts.",
    )
    return parser.parse_args()


def require(path: Path, label: str) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing {label}: {path}")


def normalize_game_id(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def normalize_date(series: pd.Series) -> pd.Series:
    text = (
        series.astype(str)
        .str.strip()
        .str.replace("_", "-", regex=False)
    )
    return pd.to_datetime(text, errors="coerce").dt.normalize()


def finite_numeric(series: pd.Series) -> pd.Series:
    out = pd.to_numeric(series, errors="coerce")
    return out.where(np.isfinite(out), np.nan)


def parse_iteration_grid(value: str) -> tuple[int, ...]:
    values: list[int] = []
    for token in str(value).split(","):
        token = token.strip()
        if not token:
            continue
        n = int(token)
        if n < 1000:
            raise ValueError("Every simulation count must be at least 1000.")
        values.append(n)
    values = sorted(set(values))
    if len(values) < 2:
        raise ValueError("Need at least two simulation counts for stability testing.")
    return tuple(values)


def load_predictions(path: Path) -> pd.DataFrame:
    require(path, "season-master predictions")
    frame = pd.read_csv(path, dtype={"game_id": str}, encoding="utf-8-sig")
    required = {
        "game_id",
        "game_date",
        "home_team",
        "away_team",
        "home_prob_moneyline",
        "home_projected_goals",
        "away_projected_goals",
        "total_projected_goals",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"predictions.csv missing columns: {missing}")

    frame = frame.copy()
    frame["game_id"] = normalize_game_id(frame["game_id"])
    frame["_date"] = normalize_date(frame["game_date"])

    for col in (
        "home_prob_moneyline",
        "home_projected_goals",
        "away_projected_goals",
        "total_projected_goals",
    ):
        frame[col] = finite_numeric(frame[col])

    if frame["game_id"].duplicated().any():
        raise RuntimeError("predictions.csv contains duplicate game_id values.")
    if frame["_date"].isna().any():
        raise RuntimeError("predictions.csv contains invalid game_date values.")

    positive = (
        (frame["home_projected_goals"] > 0)
        & (frame["away_projected_goals"] > 0)
    )
    frame = frame[positive].copy()

    projected_sum = (
        frame["home_projected_goals"]
        + frame["away_projected_goals"]
    )
    frame["_total_mean_gap"] = (
        projected_sum - frame["total_projected_goals"]
    ).abs()

    # Use the home/away means directly because score simulation requires both.
    frame["_mu_home"] = frame["home_projected_goals"]
    frame["_mu_away"] = frame["away_projected_goals"]
    frame["_mu_total"] = projected_sum
    frame["_mu_margin"] = (
        frame["_mu_home"] - frame["_mu_away"]
    )
    return frame


def load_sportsbook(path: Path) -> pd.DataFrame:
    require(path, "season-master sportsbook")
    frame = pd.read_csv(path, dtype={"game_id": str}, encoding="utf-8-sig")
    required = {
        "game_id",
        "home_puck_line",
        "away_puck_line",
        "total",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"sportsbook.csv missing columns: {missing}")

    frame = frame.copy()
    frame["game_id"] = normalize_game_id(frame["game_id"])
    for col in ("home_puck_line", "away_puck_line", "total"):
        frame[col] = finite_numeric(frame[col])

    if frame["game_id"].duplicated().any():
        raise RuntimeError("sportsbook.csv contains duplicate game_id values.")

    both = (
        frame["home_puck_line"].notna()
        & frame["away_puck_line"].notna()
    )
    frame["puck_line_pair_consistent"] = False
    frame.loc[both, "puck_line_pair_consistent"] = (
        (
            frame.loc[both, "home_puck_line"]
            + frame.loc[both, "away_puck_line"]
        ).abs()
        <= 1e-9
    )
    return frame[
        [
            "game_id",
            "home_puck_line",
            "away_puck_line",
            "total",
            "puck_line_pair_consistent",
        ]
    ].copy()


def load_scores(scores_dir: Path) -> pd.DataFrame:
    require(scores_dir, "archived final-score directory")
    files = sorted(scores_dir.glob("*_NHL_final_scores.csv"))
    if not files:
        raise RuntimeError(f"No archived final-score CSVs found in {scores_dir}")

    frames: list[pd.DataFrame] = []
    for path in files:
        frame = pd.read_csv(path, dtype={"game_id": str})
        if frame.empty:
            continue
        required = {
            "game_id",
            "game_date",
            "home_score",
            "away_score",
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise RuntimeError(f"{path.name} missing score columns: {missing}")
        frame = frame[
            ["game_id", "game_date", "home_score", "away_score"]
        ].copy()
        frames.append(frame)

    if not frames:
        raise RuntimeError("Archived score files contain no rows.")

    scores = pd.concat(frames, ignore_index=True)
    scores["game_id"] = normalize_game_id(scores["game_id"])
    scores["_score_date"] = normalize_date(scores["game_date"])
    scores["home_score"] = finite_numeric(scores["home_score"])
    scores["away_score"] = finite_numeric(scores["away_score"])
    scores = scores.dropna(
        subset=["game_id", "_score_date", "home_score", "away_score"]
    ).copy()

    if scores["game_id"].duplicated().any():
        dupes = (
            scores.loc[
                scores["game_id"].duplicated(keep=False),
                "game_id",
            ]
            .unique()
            .tolist()
        )
        raise RuntimeError(
            "Archived final scores contain duplicate game_id values: "
            + ", ".join(sorted(dupes)[:20])
        )

    scores["_actual_home"] = scores["home_score"].astype(int)
    scores["_actual_away"] = scores["away_score"].astype(int)
    scores["_actual_total"] = (
        scores["_actual_home"] + scores["_actual_away"]
    )
    scores["_actual_margin"] = (
        scores["_actual_home"] - scores["_actual_away"]
    )
    scores["_actual_home_win"] = (
        scores["_actual_home"] > scores["_actual_away"]
    ).astype(int)

    tied = scores["_actual_home"] == scores["_actual_away"]
    if tied.any():
        raise RuntimeError(
            "Archived NHL final scores contain tied final scores, which violates "
            "the expected official final-score contract."
        )

    return scores[
        [
            "game_id",
            "_score_date",
            "_actual_home",
            "_actual_away",
            "_actual_total",
            "_actual_margin",
            "_actual_home_win",
        ]
    ].copy()


def build_dataset(
    predictions: pd.DataFrame,
    sportsbook: pd.DataFrame,
    scores: pd.DataFrame,
) -> pd.DataFrame:
    frame = predictions.merge(
        sportsbook,
        on="game_id",
        how="left",
        validate="one_to_one",
    )
    frame = frame.merge(
        scores,
        on="game_id",
        how="inner",
        validate="one_to_one",
    )
    frame["puck_line_pair_consistent"] = (
        frame["puck_line_pair_consistent"]
        .fillna(False)
        .astype(bool)
    )

    if frame.empty:
        raise RuntimeError("No overlap among predictions, sportsbook, and scores.")

    date_mismatch = frame[
        frame["_date"] != frame["_score_date"]
    ]
    if not date_mismatch.empty:
        raise RuntimeError(
            f"Prediction/score date mismatch for {len(date_mismatch)} games."
        )

    frame = frame.sort_values(["_date", "game_id"]).reset_index(drop=True)
    return frame


def chronological_split(
    frame: pd.DataFrame,
    holdout_fraction: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if not (0.10 <= holdout_fraction <= 0.50):
        raise ValueError("--holdout-fraction must be between 0.10 and 0.50.")

    dates = np.array(sorted(frame["_date"].dropna().unique()))
    if len(dates) < 20:
        raise RuntimeError("Insufficient distinct dates for chronological holdout.")

    holdout_count = max(1, int(math.ceil(len(dates) * holdout_fraction)))
    holdout_dates = dates[-holdout_count:]
    holdout_start = pd.Timestamp(holdout_dates[0])

    train = frame[frame["_date"] < holdout_start].copy()
    holdout = frame[frame["_date"] >= holdout_start].copy()

    if train.empty or holdout.empty:
        raise RuntimeError("Chronological split produced empty train or holdout.")
    if train["_date"].max() >= holdout["_date"].min():
        raise RuntimeError("Leakage assertion failed: train reaches holdout period.")

    split = {
        "rule": "earliest 75% of unique dates fit; latest 25% validate",
        "holdout_fraction_requested": float(holdout_fraction),
        "unique_dates_total": int(len(dates)),
        "unique_dates_train": int(train["_date"].nunique()),
        "unique_dates_holdout": int(holdout["_date"].nunique()),
        "train_rows": int(len(train)),
        "holdout_rows": int(len(holdout)),
        "train_first_date": train["_date"].min().date().isoformat(),
        "train_last_date": train["_date"].max().date().isoformat(),
        "holdout_first_date": holdout["_date"].min().date().isoformat(),
        "holdout_last_date": holdout["_date"].max().date().isoformat(),
        "leakage_assertion": "max(train_date) < min(holdout_date)",
        "holdout_outcomes_used_in_fit": False,
        "holdout_outcomes_used_in_iteration_selection": False,
    }
    return train, holdout, split


def fit_nb_alpha(actual: np.ndarray, mean: np.ndarray) -> float:
    actual = np.asarray(actual, dtype=float)
    mean = np.asarray(mean, dtype=float)
    valid = np.isfinite(actual) & np.isfinite(mean) & (mean > 0)
    actual = actual[valid]
    mean = mean[valid]
    numerator = float(np.sum((actual - mean) ** 2 - mean))
    denominator = float(np.sum(mean ** 2))
    if denominator <= 0:
        return 0.0
    return max(0.0, numerator / denominator)


def fit_distribution_parameters(train: pd.DataFrame) -> dict[str, Any]:
    mu_h = train["_mu_home"].to_numpy(float)
    mu_a = train["_mu_away"].to_numpy(float)
    y_h = train["_actual_home"].to_numpy(float)
    y_a = train["_actual_away"].to_numpy(float)

    resid_h = y_h - mu_h
    resid_a = y_a - mu_a
    resid_cov = float(np.mean(resid_h * resid_a))

    min_mu = np.minimum(mu_h, mu_a)
    positive_min = min_mu[np.isfinite(min_mu) & (min_mu > 0)]
    support_cap = (
        float(np.quantile(positive_min, 0.01) * 0.95)
        if len(positive_min)
        else 0.0
    )
    common_lambda = max(0.0, min(resid_cov, support_cap))

    alpha_h = fit_nb_alpha(y_h, mu_h)
    alpha_a = fit_nb_alpha(y_a, mu_a)

    centered_h = resid_h - float(np.mean(resid_h))
    centered_a = resid_a - float(np.mean(resid_a))
    residual_pairs = np.column_stack([centered_h, centered_a])

    residual_corr = (
        float(np.corrcoef(resid_h, resid_a)[0, 1])
        if len(resid_h) > 1
        else np.nan
    )

    return {
        "independent_poisson": {
            "fitted": True,
            "parameters": {},
            "support_note": "No fitted dispersion/correlation parameters.",
        },
        "bivariate_poisson": {
            "fitted": common_lambda > 1e-12,
            "parameters": {
                "raw_training_residual_covariance": resid_cov,
                "common_lambda": common_lambda,
                "support_cap": support_cap,
            },
            "support_note": (
                "Supported only when training residual covariance is positive. "
                "A common Poisson component preserves game-specific means."
            ),
        },
        "negative_binomial": {
            "fitted": True,
            "parameters": {
                "alpha_home": alpha_h,
                "alpha_away": alpha_a,
                "variance_formula": "mu + alpha * mu^2",
            },
            "support_note": (
                "Method-of-moments overdispersion fitted on training scores only; "
                "alpha=0 degenerates to Poisson."
            ),
        },
        "empirical_residual": {
            "fitted": True,
            "parameters": {
                "residual_rows": int(len(residual_pairs)),
                "home_residual_mean_before_centering": float(np.mean(resid_h)),
                "away_residual_mean_before_centering": float(np.mean(resid_a)),
                "home_residual_std": float(np.std(resid_h, ddof=1)),
                "away_residual_std": float(np.std(resid_a, ddof=1)),
                "residual_correlation": residual_corr,
                "residuals_centered": True,
                "paired_resampling": True,
            },
            "support_note": (
                "Samples paired centered training residuals with replacement, "
                "then rounds/clips scores to nonnegative integers."
            ),
            "_residual_pairs": residual_pairs,
        },
    }


def deterministic_seed(base_seed: int, *parts: str) -> int:
    text = "|".join([str(base_seed), *map(str, parts)])
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little") % (2**32 - 1)


def sample_negative_binomial(
    rng: np.random.Generator,
    mu: float,
    alpha: float,
    n: int,
) -> np.ndarray:
    if alpha <= 1e-10:
        return rng.poisson(mu, size=n)
    shape = 1.0 / alpha
    p = shape / (shape + mu)
    return rng.negative_binomial(shape, p, size=n)


def simulate_scores(
    *,
    distribution: str,
    mu_home: float,
    mu_away: float,
    n: int,
    seed: int,
    fitted: dict[str, Any],
) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)

    if distribution == "independent_poisson":
        home = rng.poisson(mu_home, size=n)
        away = rng.poisson(mu_away, size=n)
        return home.astype(np.int16), away.astype(np.int16)

    if distribution == "bivariate_poisson":
        common_lambda = float(
            fitted["bivariate_poisson"]["parameters"]["common_lambda"]
        )
        common_lambda = min(
            common_lambda,
            max(0.0, min(mu_home, mu_away) * 0.95),
        )
        if common_lambda <= 1e-12:
            raise RuntimeError(
                "bivariate_poisson requested but positive correlation was not "
                "supportable from training residuals."
            )
        common = rng.poisson(common_lambda, size=n)
        home = (
            rng.poisson(max(mu_home - common_lambda, EPS), size=n)
            + common
        )
        away = (
            rng.poisson(max(mu_away - common_lambda, EPS), size=n)
            + common
        )
        return home.astype(np.int16), away.astype(np.int16)

    if distribution == "negative_binomial":
        params = fitted["negative_binomial"]["parameters"]
        home = sample_negative_binomial(
            rng,
            mu_home,
            float(params["alpha_home"]),
            n,
        )
        away = sample_negative_binomial(
            rng,
            mu_away,
            float(params["alpha_away"]),
            n,
        )
        return home.astype(np.int16), away.astype(np.int16)

    if distribution == "empirical_residual":
        residuals = fitted["empirical_residual"]["_residual_pairs"]
        if len(residuals) == 0:
            raise RuntimeError("No residual pairs available.")
        indices = rng.integers(0, len(residuals), size=n)
        drawn = residuals[indices]
        home = np.rint(mu_home + drawn[:, 0])
        away = np.rint(mu_away + drawn[:, 1])
        home = np.clip(home, 0, None).astype(np.int16)
        away = np.clip(away, 0, None).astype(np.int16)
        return home, away

    raise RuntimeError(f"Unknown distribution: {distribution}")


def classify_line(
    score_for: np.ndarray,
    score_against: np.ndarray,
    line: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    adjusted = score_for.astype(float) + float(line)
    against = score_against.astype(float)
    cover = adjusted > against
    push = np.isclose(adjusted, against, atol=1e-12)
    loss = ~(cover | push)
    return cover, push, loss


def classify_total(
    total_goals: np.ndarray,
    line: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    values = total_goals.astype(float)
    over = values > float(line)
    push = np.isclose(values, float(line), atol=1e-12)
    under = ~(over | push)
    return over, push, under


def bernoulli_se(p: float, n: int) -> float:
    if not np.isfinite(p) or n <= 0:
        return np.nan
    return math.sqrt(max(p * (1.0 - p), 0.0) / n)


def half_width_95(p: float, n: int) -> float:
    se = bernoulli_se(p, n)
    return 1.96 * se if np.isfinite(se) else np.nan


def simulation_summary(
    *,
    home: np.ndarray,
    away: np.ndarray,
    home_line: float | None,
    away_line: float | None,
    total_line: float | None,
    seed: int,
) -> dict[str, Any]:
    n = len(home)
    total = home + away
    margin = home - away

    home_ahead = home > away
    tie = home == away

    # NHL final games cannot tie. For score-draw ties, determine a winner with a
    # neutral 50/50 overtime/shootout coin. We intentionally do NOT alter the
    # sampled score because puck-line/total grading is based on the score draw.
    rng_ot = np.random.default_rng(
        deterministic_seed(seed, "ot_tie_resolution")
    )
    tie_home = rng_ot.random(n) < 0.5
    winner_home = home_ahead | (tie & tie_home)

    out: dict[str, Any] = {
        "simulations": int(n),
        "seed": int(seed),
        "sim_home_mean": float(np.mean(home)),
        "sim_away_mean": float(np.mean(away)),
        "sim_margin_mean": float(np.mean(margin)),
        "sim_total_mean": float(np.mean(total)),
        "score_tie_prob": float(np.mean(tie)),
        "home_win_prob": float(np.mean(winner_home)),
    }

    out["home_win_mc_se"] = bernoulli_se(out["home_win_prob"], n)
    out["home_win_95_half_width"] = half_width_95(
        out["home_win_prob"], n
    )

    if home_line is not None and np.isfinite(home_line):
        cover, push, loss = classify_line(home, away, float(home_line))
        out.update(
            {
                "home_pl_cover_prob": float(np.mean(cover)),
                "home_pl_push_prob": float(np.mean(push)),
                "home_pl_loss_prob": float(np.mean(loss)),
            }
        )
        for key in (
            "home_pl_cover_prob",
            "home_pl_push_prob",
            "home_pl_loss_prob",
        ):
            out[key.replace("_prob", "_mc_se")] = bernoulli_se(
                out[key], n
            )
            out[key.replace("_prob", "_95_half_width")] = half_width_95(
                out[key], n
            )
    else:
        for key in (
            "home_pl_cover_prob",
            "home_pl_push_prob",
            "home_pl_loss_prob",
            "home_pl_cover_mc_se",
            "home_pl_push_mc_se",
            "home_pl_loss_mc_se",
            "home_pl_cover_95_half_width",
            "home_pl_push_95_half_width",
            "home_pl_loss_95_half_width",
        ):
            out[key] = np.nan

    if away_line is not None and np.isfinite(away_line):
        cover, push, loss = classify_line(away, home, float(away_line))
        out.update(
            {
                "away_pl_cover_prob": float(np.mean(cover)),
                "away_pl_push_prob": float(np.mean(push)),
                "away_pl_loss_prob": float(np.mean(loss)),
            }
        )
    else:
        out["away_pl_cover_prob"] = np.nan
        out["away_pl_push_prob"] = np.nan
        out["away_pl_loss_prob"] = np.nan

    if total_line is not None and np.isfinite(total_line):
        over, push, under = classify_total(total, float(total_line))
        out.update(
            {
                "over_prob": float(np.mean(over)),
                "total_push_prob": float(np.mean(push)),
                "under_prob": float(np.mean(under)),
            }
        )
        for key in ("over_prob", "total_push_prob", "under_prob"):
            out[key.replace("_prob", "_mc_se")] = bernoulli_se(
                out[key], n
            )
            out[key.replace("_prob", "_95_half_width")] = half_width_95(
                out[key], n
            )
    else:
        for key in (
            "over_prob",
            "total_push_prob",
            "under_prob",
            "over_mc_se",
            "total_push_mc_se",
            "under_mc_se",
            "over_95_half_width",
            "total_push_95_half_width",
            "under_95_half_width",
        ):
            out[key] = np.nan

    return out


def tracked_probability_vector(summary: dict[str, Any]) -> np.ndarray:
    keys = (
        "home_win_prob",
        "home_pl_cover_prob",
        "home_pl_push_prob",
        "over_prob",
        "total_push_prob",
    )
    values = []
    for key in keys:
        value = summary.get(key, np.nan)
        values.append(float(value) if np.isfinite(value) else np.nan)
    return np.asarray(values, dtype=float)


def stability_rows_for_distribution(
    *,
    distribution: str,
    holdout: pd.DataFrame,
    fitted: dict[str, Any],
    iteration_grid: tuple[int, ...],
    base_seed: int,
    stability_games: int,
    stability_delta: float,
    max_95_half_width: float,
) -> tuple[pd.DataFrame, int]:
    if distribution == "bivariate_poisson":
        if not bool(fitted[distribution]["fitted"]):
            return (
                pd.DataFrame(
                    [
                        {
                            "distribution": distribution,
                            "iterations": np.nan,
                            "games_checked": 0,
                            "max_abs_probability_change_vs_prior": np.nan,
                            "max_95_half_width": np.nan,
                            "stable": False,
                            "selected": False,
                            "status": "unsupported_nonpositive_training_residual_covariance",
                        }
                    ]
                ),
                0,
            )

    if stability_games <= 0:
        raise ValueError("--stability-games must be positive.")

    count = min(stability_games, len(holdout))
    positions = np.linspace(
        0,
        len(holdout) - 1,
        num=count,
        dtype=int,
    )
    sample = holdout.iloc[np.unique(positions)].copy()

    previous: dict[str, np.ndarray] = {}
    rows: list[dict[str, Any]] = []
    selected_n = 0

    for n in iteration_grid:
        current: dict[str, np.ndarray] = {}
        max_half = 0.0

        for _, row in sample.iterrows():
            game_id = str(row["game_id"])
            # Use the same deterministic stream for every iteration count.
            # Larger runs therefore extend the same Monte Carlo sequence,
            # making convergence comparisons meaningful.
            seed = deterministic_seed(
                base_seed,
                "stability",
                distribution,
                game_id,
            )
            home, away = simulate_scores(
                distribution=distribution,
                mu_home=float(row["_mu_home"]),
                mu_away=float(row["_mu_away"]),
                n=n,
                seed=seed,
                fitted=fitted,
            )
            summary = simulation_summary(
                home=home,
                away=away,
                home_line=(
                    float(row["home_puck_line"])
                    if np.isfinite(row["home_puck_line"])
                    else None
                ),
                away_line=(
                    float(row["away_puck_line"])
                    if np.isfinite(row["away_puck_line"])
                    else None
                ),
                total_line=(
                    float(row["total"])
                    if np.isfinite(row["total"])
                    else None
                ),
                seed=seed,
            )
            vector = tracked_probability_vector(summary)
            current[game_id] = vector

            finite_probs = vector[np.isfinite(vector)]
            if len(finite_probs):
                halfs = [
                    half_width_95(float(p), n)
                    for p in finite_probs
                ]
                max_half = max(max_half, max(halfs))

        max_delta = np.nan
        if previous:
            deltas: list[float] = []
            for game_id, vector in current.items():
                prior = previous[game_id]
                valid = np.isfinite(vector) & np.isfinite(prior)
                if valid.any():
                    deltas.extend(
                        np.abs(vector[valid] - prior[valid]).tolist()
                    )
            if deltas:
                max_delta = float(np.max(deltas))

        stable = bool(
            previous
            and np.isfinite(max_delta)
            and max_delta <= stability_delta
            and max_half <= max_95_half_width
        )

        rows.append(
            {
                "distribution": distribution,
                "iterations": int(n),
                "games_checked": int(len(sample)),
                "max_abs_probability_change_vs_prior": max_delta,
                "max_95_half_width": float(max_half),
                "stable": stable,
                "selected": False,
                "status": "ok",
            }
        )

        if stable and selected_n == 0:
            selected_n = int(n)
            rows[-1]["selected"] = True
            break

        previous = current

    if selected_n == 0:
        selected_n = int(iteration_grid[-1])
        for row in rows:
            if int(row["iterations"]) == selected_n:
                row["selected"] = True
                row["status"] = "max_grid_selected_stability_not_met"
                break

    return pd.DataFrame(rows), selected_n


def grade_actual_line(
    score_for: int,
    score_against: int,
    line: float,
) -> int:
    adjusted = float(score_for) + float(line)
    against = float(score_against)
    if adjusted > against:
        return 0  # cover
    if math.isclose(adjusted, against, abs_tol=1e-12):
        return 1  # push
    return 2  # loss


def grade_actual_total(total_goals: int, line: float) -> int:
    value = float(total_goals)
    if value > float(line):
        return 0  # over
    if math.isclose(value, float(line), abs_tol=1e-12):
        return 1  # push
    return 2  # under


def multiclass_metrics(
    probabilities: np.ndarray,
    actual_index: np.ndarray,
) -> tuple[float, float]:
    p = np.asarray(probabilities, dtype=float)
    y = np.asarray(actual_index, dtype=int)
    if len(p) == 0:
        return np.nan, np.nan
    if p.ndim != 2 or p.shape[1] != 3:
        raise RuntimeError("Expected Nx3 multiclass probabilities.")

    clipped = np.clip(p, EPS, 1.0)
    clipped = clipped / clipped.sum(axis=1, keepdims=True)
    one_hot = np.eye(3)[y]

    # Multiclass Brier: sum squared errors across classes, averaged by row.
    brier = float(np.mean(np.sum((p - one_hot) ** 2, axis=1)))
    logloss = float(
        -np.mean(np.log(clipped[np.arange(len(y)), y]))
    )
    return brier, logloss


def binary_brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((np.asarray(p) - np.asarray(y)) ** 2))


def binary_logloss(y: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    return float(
        -np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
    )


def ece_binary(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    bucket = np.digitize(p, edges[1:-1], right=True)
    ece = 0.0
    for idx in range(bins):
        mask = bucket == idx
        if not mask.any():
            continue
        ece += (
            float(mask.mean())
            * abs(float(np.mean(p[mask])) - float(np.mean(y[mask])))
        )
    return float(ece)


def validation_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for distribution, group in predictions.groupby("distribution", sort=True):
        money = group.dropna(subset=["home_win_prob"]).copy()
        if not money.empty:
            y = money["actual_home_win"].to_numpy(int)
            p = money["home_win_prob"].to_numpy(float)
            rows.append(
                {
                    "distribution": distribution,
                    "event": "moneyline_home_win",
                    "rows": len(money),
                    "brier": binary_brier(y, p),
                    "log_loss": binary_logloss(y, p),
                    "ece_10": ece_binary(y, p),
                    "mean_predicted_probability": float(np.mean(p)),
                    "realized_rate": float(np.mean(y)),
                    "calibration_gap": float(np.mean(p) - np.mean(y)),
                    "push_preserved": False,
                }
            )

        puck = group[
            group["puck_line_pair_consistent"].eq(True)
            & group["home_puck_line"].notna()
            & group["home_pl_cover_prob"].notna()
        ].copy()
        if not puck.empty:
            actual = np.array(
                [
                    grade_actual_line(h, a, line)
                    for h, a, line in zip(
                        puck["actual_home_score"],
                        puck["actual_away_score"],
                        puck["home_puck_line"],
                    )
                ],
                dtype=int,
            )
            probs = puck[
                [
                    "home_pl_cover_prob",
                    "home_pl_push_prob",
                    "home_pl_loss_prob",
                ]
            ].to_numpy(float)
            brier, logloss = multiclass_metrics(probs, actual)
            rows.append(
                {
                    "distribution": distribution,
                    "event": "home_puck_line_3class",
                    "rows": len(puck),
                    "brier": brier,
                    "log_loss": logloss,
                    "ece_10": np.nan,
                    "mean_predicted_probability": np.nan,
                    "realized_rate": np.nan,
                    "calibration_gap": np.nan,
                    "push_preserved": True,
                }
            )

        totals = group[
            group["total_line"].notna()
            & group["over_prob"].notna()
        ].copy()
        if not totals.empty:
            actual = np.array(
                [
                    grade_actual_total(total, line)
                    for total, line in zip(
                        totals["actual_total"],
                        totals["total_line"],
                    )
                ],
                dtype=int,
            )
            probs = totals[
                ["over_prob", "total_push_prob", "under_prob"]
            ].to_numpy(float)
            brier, logloss = multiclass_metrics(probs, actual)
            rows.append(
                {
                    "distribution": distribution,
                    "event": "total_3class",
                    "rows": len(totals),
                    "brier": brier,
                    "log_loss": logloss,
                    "ece_10": np.nan,
                    "mean_predicted_probability": np.nan,
                    "realized_rate": np.nan,
                    "calibration_gap": np.nan,
                    "push_preserved": True,
                }
            )

    return pd.DataFrame(rows)


def clean_params_for_json(fitted: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for distribution, payload in fitted.items():
        clean[distribution] = {
            key: value
            for key, value in payload.items()
            if key != "_residual_pairs"
        }
    return clean


def main() -> int:
    args = parse_args()
    iteration_grid = parse_iteration_grid(args.iteration_grid)

    predictions = load_predictions(args.predictions.resolve())
    sportsbook = load_sportsbook(args.sportsbook.resolve())
    scores = load_scores(args.scores_dir.resolve())

    data = build_dataset(predictions, sportsbook, scores)
    train, holdout, split = chronological_split(
        data,
        args.holdout_fraction,
    )

    fitted = fit_distribution_parameters(train)
    supported = [
        name
        for name in DISTRIBUTIONS
        if bool(fitted[name]["fitted"])
    ]

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    stability_frames: list[pd.DataFrame] = []
    selected_iterations: dict[str, int] = {}

    for distribution in DISTRIBUTIONS:
        stability, selected_n = stability_rows_for_distribution(
            distribution=distribution,
            holdout=holdout,
            fitted=fitted,
            iteration_grid=iteration_grid,
            base_seed=args.seed,
            stability_games=args.stability_games,
            stability_delta=args.stability_delta,
            max_95_half_width=args.max_95_half_width,
        )
        stability_frames.append(stability)
        selected_iterations[distribution] = selected_n

    stability_all = pd.concat(
        stability_frames,
        ignore_index=True,
        sort=False,
    )

    rows: list[dict[str, Any]] = []

    for distribution in supported:
        n = int(selected_iterations[distribution])

        for _, row in holdout.iterrows():
            game_id = str(row["game_id"])
            seed = deterministic_seed(
                args.seed,
                "holdout",
                distribution,
                game_id,
            )
            home, away = simulate_scores(
                distribution=distribution,
                mu_home=float(row["_mu_home"]),
                mu_away=float(row["_mu_away"]),
                n=n,
                seed=seed,
                fitted=fitted,
            )
            summary = simulation_summary(
                home=home,
                away=away,
                home_line=(
                    float(row["home_puck_line"])
                    if np.isfinite(row["home_puck_line"])
                    else None
                ),
                away_line=(
                    float(row["away_puck_line"])
                    if np.isfinite(row["away_puck_line"])
                    else None
                ),
                total_line=(
                    float(row["total"])
                    if np.isfinite(row["total"])
                    else None
                ),
                seed=seed,
            )

            rows.append(
                {
                    "distribution": distribution,
                    "game_id": game_id,
                    "game_date": pd.Timestamp(row["_date"]).date().isoformat(),
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                    "mean_home_goals": float(row["_mu_home"]),
                    "mean_away_goals": float(row["_mu_away"]),
                    "mean_margin": float(row["_mu_margin"]),
                    "mean_total": float(row["_mu_total"]),
                    "direct_home_win_prob": float(row["home_prob_moneyline"]),
                    "home_puck_line": (
                        float(row["home_puck_line"])
                        if np.isfinite(row["home_puck_line"])
                        else np.nan
                    ),
                    "away_puck_line": (
                        float(row["away_puck_line"])
                        if np.isfinite(row["away_puck_line"])
                        else np.nan
                    ),
                    "puck_line_pair_consistent": bool(
                        row["puck_line_pair_consistent"]
                    ),
                    "total_line": (
                        float(row["total"])
                        if np.isfinite(row["total"])
                        else np.nan
                    ),
                    "actual_home_score": int(row["_actual_home"]),
                    "actual_away_score": int(row["_actual_away"]),
                    "actual_margin": int(row["_actual_margin"]),
                    "actual_total": int(row["_actual_total"]),
                    "actual_home_win": int(row["_actual_home_win"]),
                    **summary,
                }
            )

    holdout_predictions = pd.DataFrame(rows)
    if holdout_predictions.empty:
        raise RuntimeError("No supported simulation distributions were evaluated.")

    metrics = validation_metrics(holdout_predictions)

    # Context-only direct moneyline model on the exact same holdout rows.
    direct_money = holdout.dropna(
        subset=["home_prob_moneyline", "_actual_home_win"]
    )
    direct_context = {
        "rows": int(len(direct_money)),
        "brier": binary_brier(
            direct_money["_actual_home_win"].to_numpy(int),
            direct_money["home_prob_moneyline"].to_numpy(float),
        ),
        "log_loss": binary_logloss(
            direct_money["_actual_home_win"].to_numpy(int),
            direct_money["home_prob_moneyline"].to_numpy(float),
        ),
        "ece_10": ece_binary(
            direct_money["_actual_home_win"].to_numpy(int),
            direct_money["home_prob_moneyline"].to_numpy(float),
        ),
        "role": (
            "context only; direct D-Ratings moneyline probability is not a "
            "score-distribution simulation candidate"
        ),
    }

    rank_rows: list[dict[str, Any]] = []
    for event in (
        "moneyline_home_win",
        "home_puck_line_3class",
        "total_3class",
    ):
        part = metrics[metrics["event"] == event].sort_values(
            ["log_loss", "brier", "distribution"]
        )
        for rank, record in enumerate(
            part.to_dict("records"),
            start=1,
        ):
            rank_rows.append(
                {
                    "event": event,
                    "rank_by_log_loss_then_brier": rank,
                    **record,
                }
            )
    rankings = pd.DataFrame(rank_rows)

    total_mean_gap_max = float(data["_total_mean_gap"].max())
    inconsistent_puck_games = int(
        (
            data["home_puck_line"].notna()
            & data["away_puck_line"].notna()
            & ~data["puck_line_pair_consistent"]
        ).sum()
    )
    holdout_inconsistent_puck_games = int(
        (
            holdout["home_puck_line"].notna()
            & holdout["away_puck_line"].notna()
            & ~holdout["puck_line_pair_consistent"]
        ).sum()
    )

    prediction_path = output_dir / "item14_holdout_simulation_predictions.csv"
    metrics_path = output_dir / "item14_holdout_event_metrics.csv"
    stability_path = output_dir / "item14_simulation_stability.csv"
    rankings_path = output_dir / "item14_holdout_rankings.csv"
    params_path = output_dir / "item14_distribution_parameters.json"
    split_path = output_dir / "item14_holdout_split.json"
    report_path = output_dir / "item14_report.json"

    holdout_predictions.to_csv(prediction_path, index=False)
    metrics.to_csv(metrics_path, index=False)
    stability_all.to_csv(stability_path, index=False)
    rankings.to_csv(rankings_path, index=False)

    parameters_clean = clean_params_for_json(fitted)
    params_path.write_text(
        json.dumps(parameters_clean, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    split_path.write_text(
        json.dumps(split, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    report = {
        "script_version": SCRIPT_VERSION,
        "status": "COMPLETE",
        "purpose": (
            "Item 14 NHL score-distribution simulation research; no automatic "
            "production change."
        ),
        "production_baseline": (
            "Current build_juice_files.py uses independent Poisson / Skellam "
            "from D-Ratings projected goals for puck-line and total event probabilities."
        ),
        "mean_model_held_constant": (
            "D-Ratings season-master home_projected_goals and away_projected_goals"
        ),
        "candidate_distributions_requested": list(DISTRIBUTIONS),
        "candidate_distributions_supported": supported,
        "unsupported_distributions": [
            name for name in DISTRIBUTIONS if name not in supported
        ],
        "distribution_parameters": parameters_clean,
        "chronological_holdout": split,
        "holdout_is_globally_untouched": False,
        "holdout_scope_note": (
            "The split is held out from Item-14 distribution fitting. The 2025-26 "
            "season has been used by earlier project research and is not claimed as "
            "a globally untouched production test."
        ),
        "simulation_iteration_grid": list(iteration_grid),
        "selected_simulations_by_distribution": selected_iterations,
        "base_seed": int(args.seed),
        "stability_rule": {
            "games_checked": int(
                min(args.stability_games, len(holdout))
            ),
            "max_abs_probability_change_vs_prior": float(
                args.stability_delta
            ),
            "max_95_mc_half_width": float(
                args.max_95_half_width
            ),
            "tracked_probabilities": [
                "home_win_prob",
                "home_pl_cover_prob",
                "home_pl_push_prob",
                "over_prob",
                "total_push_prob",
            ],
            "uses_holdout_outcomes": False,
        },
        "tie_handling": (
            "Score-draw ties are resolved 50/50 for winner classification only; "
            "the score itself is not altered, so puck-line and total grading remain "
            "tied to the generated scoring outcomes."
        ),
        "push_handling": (
            "Puck-line and total simulations retain explicit cover/over, push, "
            "and loss/under probabilities. Push mass is never renormalized away."
        ),
        "puck_line_validation": {
            "rule": (
                "Validate home-side 3-class puck-line probabilities only when both "
                "historical side lines are present and are exact opposites. Simulated "
                "probabilities are still recorded when an individual line exists."
            ),
            "inconsistent_pair_games_all_data": inconsistent_puck_games,
            "inconsistent_pair_games_holdout": holdout_inconsistent_puck_games,
        },
        "projection_consistency": {
            "max_abs_total_projected_goals_minus_home_plus_away": total_mean_gap_max,
            "simulation_uses_home_plus_away_means_directly": True,
        },
        "direct_moneyline_context_same_holdout": direct_context,
        "validation_metrics_file": str(metrics_path),
        "rankings_file": str(rankings_path),
        "promotion_decision": None,
        "production_change": "none",
        "outputs": {
            "holdout_predictions": str(prediction_path),
            "event_metrics": str(metrics_path),
            "stability": str(stability_path),
            "rankings": str(rankings_path),
            "parameters": str(params_path),
            "split": str(split_path),
            "report": str(report_path),
        },
    }

    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(
        "Item 14 simulation evaluation complete: "
        f"rows={len(data)} "
        f"train={len(train)} "
        f"holdout={len(holdout)} "
        f"supported={','.join(supported)} "
        f"distributions={len(supported)}"
    )
    for distribution in DISTRIBUTIONS:
        print(
            f"SIMS {distribution}="
            f"{selected_iterations.get(distribution, 0)}"
        )
    print(f"Metrics: {metrics_path}")
    print(f"Rankings: {rankings_path}")
    print(f"Stability: {stability_path}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

