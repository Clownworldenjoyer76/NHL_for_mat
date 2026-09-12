#!/usr/bin/env python3
"""
Item 10 audit: secondary probability calibration implementation.

Repository path:
    docs/win/hockey/nhl/scripts/research/audit_secondary_probability_calibration.py

Purpose:
    - Verify the production leakage guard rejects same-day and future rows.
    - Reproduce chronological target-date fitting with training restricted to
      historical_date < target_date.
    - Compare raw-probability versus logit-transformed probability features.
    - Compare multiple L2 regularization strengths.
    - Require finite objectives, finite coefficients, and optimizer convergence.
    - Report results only. This script does not modify production selection logic
      and does not automatically promote any candidate.

Outputs:
    docs/win/hockey/nhl/research/calibration/
        item10_secondary_probability_calibration_predictions.csv
        item10_secondary_probability_calibration_summary.csv
        item10_secondary_probability_calibration_failures.csv
        item10_secondary_probability_calibration_report.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import yaml
from scipy.optimize import minimize


REPO_ROOT = Path(__file__).resolve().parents[6]
NHL_ROOT = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"
PRODUCTION_SCRIPT = (
    NHL_ROOT / "scripts" / "03_edges" / "build_secondary_model_signals.py"
)
HISTORY_ROOT = NHL_ROOT / "research" / "sdv_challenger"
CONFIG_PATH = NHL_ROOT / "config" / "markets.yaml"
OUTPUT_DIR = NHL_ROOT / "research" / "calibration"

PREDICTIONS_PATH = (
    OUTPUT_DIR / "item10_secondary_probability_calibration_predictions.csv"
)
SUMMARY_PATH = (
    OUTPUT_DIR / "item10_secondary_probability_calibration_summary.csv"
)
FAILURES_PATH = (
    OUTPUT_DIR / "item10_secondary_probability_calibration_failures.csv"
)
REPORT_PATH = (
    OUTPUT_DIR / "item10_secondary_probability_calibration_report.json"
)

EPS = 1e-6
DEFAULT_REGULARIZATION_STRENGTHS = (0.0, 1e-6, 1e-4, 1e-2)
TRANSFORMS = ("raw", "logit")

REQUIRED_COLUMNS = [
    "game_id",
    "game_date",
    "drat_home_win_prob",
    "sdv_home_win_prob",
    "actual_home_win",
]


@dataclass(frozen=True)
class LogisticFit:
    coefficients: np.ndarray
    objective: float
    iterations: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit NHL secondary probability calibration using strict "
            "chronological walk-forward fits."
        )
    )
    parser.add_argument(
        "--regularization-strengths",
        nargs="+",
        type=float,
        default=list(DEFAULT_REGULARIZATION_STRENGTHS),
        help=(
            "L2 strengths to test. Default: "
            + " ".join(str(v) for v in DEFAULT_REGULARIZATION_STRENGTHS)
        ),
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=None,
        help=(
            "Minimum prior rows required before fitting. Default: read "
            "markets.nhl.secondary_model.min_train_rows from markets.yaml."
        ),
    )
    parser.add_argument(
        "--self-test-only",
        action="store_true",
        help=(
            "Run leakage/convergence guard self-tests without reading "
            "historical comparison data."
        ),
    )
    return parser.parse_args()


def canonical_game_id(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def normalized_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        series.astype("string").str.replace("_", "-", regex=False),
        errors="coerce",
    ).dt.date


def clipped_prob(values: np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(values, dtype=float), EPS, 1.0 - EPS)


def brier_score(y: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y, dtype=float)
    p = clipped_prob(p)
    return float(np.mean((p - y) ** 2))


def log_loss(y: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y, dtype=float)
    p = clipped_prob(p)
    return float(
        -np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
    )


def validate_probability(values: np.ndarray, label: str) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    if not np.all(np.isfinite(values)):
        raise RuntimeError(f"{label} contains non-finite probability values")
    if np.any((values < 0.0) | (values > 1.0)):
        low = float(np.min(values))
        high = float(np.max(values))
        raise RuntimeError(
            f"{label} contains probability outside [0, 1]: min={low} max={high}"
        )
    return values


def transform_probability(
    values: np.ndarray,
    transform: str,
) -> np.ndarray:
    values = validate_probability(values, "probability input")
    if transform == "raw":
        return values
    if transform == "logit":
        p = clipped_prob(values)
        return np.log(p / (1.0 - p))
    raise ValueError(f"Unsupported transform: {transform}")


def probability_feature_matrix(
    drat_prob: np.ndarray,
    sdv_prob: np.ndarray,
    transform: str,
) -> np.ndarray:
    drat_prob = validate_probability(drat_prob, "D-Ratings probability")
    sdv_prob = validate_probability(sdv_prob, "SDV probability")
    disagreement = np.abs(sdv_prob - drat_prob)
    return np.column_stack(
        [
            transform_probability(drat_prob, transform),
            transform_probability(sdv_prob, transform),
            disagreement,
        ]
    )


def _stable_sigmoid(z: np.ndarray) -> np.ndarray:
    z = np.asarray(z, dtype=float)
    p = np.empty_like(z, dtype=float)
    positive = z >= 0.0
    p[positive] = 1.0 / (1.0 + np.exp(-z[positive]))
    exp_z = np.exp(z[~positive])
    p[~positive] = exp_z / (1.0 + exp_z)
    return p


def _logistic_objective(
    beta: np.ndarray,
    design: np.ndarray,
    y: np.ndarray,
    regularization: float,
) -> float:
    z = design @ beta
    n_rows = len(y)
    nll = np.sum(np.logaddexp(0.0, z) - y * z)
    penalty = regularization * np.sum(beta[1:] ** 2)
    return float((nll + penalty) / n_rows)


def _logistic_gradient(
    beta: np.ndarray,
    design: np.ndarray,
    y: np.ndarray,
    regularization: float,
) -> np.ndarray:
    p = _stable_sigmoid(design @ beta)
    n_rows = len(y)
    grad = design.T @ (p - y)
    grad[1:] += 2.0 * regularization * beta[1:]
    return np.asarray(grad / n_rows, dtype=float)


def _logistic_hessian(
    beta: np.ndarray,
    design: np.ndarray,
    regularization: float,
) -> np.ndarray:
    p = _stable_sigmoid(design @ beta)
    n_rows = design.shape[0]
    weights = p * (1.0 - p)
    hess = design.T @ (design * weights[:, None])
    hess = np.asarray(hess, dtype=float)
    if design.shape[1] > 1 and regularization > 0.0:
        hess[1:, 1:] += 2.0 * regularization * np.eye(design.shape[1] - 1)
    return hess / n_rows


def fit_logistic(
    x: np.ndarray,
    y: np.ndarray,
    *,
    regularization: float,
) -> LogisticFit:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)

    if regularization < 0.0 or not math.isfinite(regularization):
        raise ValueError(
            f"regularization must be finite and >= 0, got {regularization}"
        )
    if x.ndim == 1:
        x = x[:, None]
    if len(x) != len(y):
        raise ValueError("x and y row counts differ")
    if len(y) == 0:
        raise ValueError("cannot fit logistic model with zero rows")
    if np.unique(y).size < 2:
        raise ValueError("logistic training target must contain both classes")
    if not np.all(np.isfinite(x)):
        raise RuntimeError("logistic training features contain non-finite values")
    if not np.all(np.isfinite(y)):
        raise RuntimeError("logistic training target contains non-finite values")

    design = np.column_stack([np.ones(len(x)), x])
    stationarity_tol = 1e-6

    def objective(beta: np.ndarray) -> float:
        return _logistic_objective(beta, design, y, regularization)

    def gradient(beta: np.ndarray) -> np.ndarray:
        return _logistic_gradient(beta, design, y, regularization)

    initial = np.zeros(design.shape[1], dtype=float)
    attempts = [
        (
            "BFGS",
            {"gtol": 1e-7, "maxiter": 2000},
        ),
        (
            "L-BFGS-B",
            {"gtol": 1e-7, "ftol": 1e-12, "maxiter": 5000},
        ),
    ]
    failures: list[str] = []

    for method, options in attempts:
        result = minimize(
            objective,
            initial,
            jac=gradient,
            method=method,
            options=options,
        )
        grad = gradient(result.x)
        grad_inf_norm = float(np.linalg.norm(grad, ord=np.inf))
        hess = _logistic_hessian(result.x, design, regularization)
        finite_objective = bool(np.isfinite(result.fun))
        finite_coefficients = bool(np.all(np.isfinite(result.x)))
        finite_gradient = bool(np.all(np.isfinite(grad)))
        finite_hessian = bool(np.all(np.isfinite(hess)))
        stationary = finite_gradient and grad_inf_norm <= stationarity_tol

        if (
            finite_objective
            and finite_coefficients
            and finite_gradient
            and finite_hessian
            and stationary
        ):
            return LogisticFit(
                coefficients=np.asarray(result.x, dtype=float),
                objective=float(result.fun),
                iterations=int(getattr(result, "nit", 0)),
            )

        failures.append(
            f"{method}: success={bool(result.success)} status={result.status} "
            f"message={result.message!s} objective={result.fun} "
            f"grad_inf_norm={grad_inf_norm:.12g} "
            f"stationarity_tol={stationarity_tol:.12g} "
            f"finite_hessian={finite_hessian}"
        )
        if finite_coefficients:
            initial = np.asarray(result.x, dtype=float)
        else:
            initial = np.zeros(design.shape[1], dtype=float)

    raise RuntimeError(
        "audit logistic optimizer did not converge after fallback; "
        + " | ".join(failures)
    )


def apply_logistic(
    model: LogisticFit,
    x: np.ndarray,
) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    design = np.column_stack([np.ones(len(x)), x])
    return _stable_sigmoid(design @ model.coefficients)

def select_probability_weight(
    y: np.ndarray,
    p_drat: np.ndarray,
    p_sdv: np.ndarray,
) -> float:
    grid = np.linspace(0.0, 1.0, 101)
    losses = [
        log_loss(y, w * p_drat + (1.0 - w) * p_sdv)
        for w in grid
    ]
    return float(grid[int(np.argmin(losses))])


def load_production_module():
    if not PRODUCTION_SCRIPT.exists():
        raise RuntimeError(
            f"Missing production script: {PRODUCTION_SCRIPT}"
        )
    spec = importlib.util.spec_from_file_location(
        "nhl_secondary_signals_item10",
        PRODUCTION_SCRIPT,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Unable to import production script: {PRODUCTION_SCRIPT}"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def expect_runtime_error(
    fn,
    *args,
    contains: str,
) -> str:
    try:
        fn(*args)
    except RuntimeError as exc:
        text = str(exc)
        if contains not in text:
            raise AssertionError(
                f"Expected RuntimeError containing {contains!r}, got {text!r}"
            ) from exc
        return text
    raise AssertionError(
        f"Expected RuntimeError containing {contains!r}, but no error occurred"
    )


def run_production_leakage_guard_self_test(module) -> dict:
    guard = getattr(module, "assert_prior_only_training", None)
    if guard is None:
        raise RuntimeError(
            "Production script is missing assert_prior_only_training()"
        )

    target_day = date(2026, 1, 15)
    prior = pd.DataFrame(
        {
            "game_id": ["prior"],
            "_date": [date(2026, 1, 14)],
        }
    )
    same_day = pd.DataFrame(
        {
            "game_id": ["same"],
            "_date": [target_day],
        }
    )
    future = pd.DataFrame(
        {
            "game_id": ["future"],
            "_date": [date(2026, 1, 16)],
        }
    )

    guard(prior, target_day)
    same_message = expect_runtime_error(
        guard,
        same_day,
        target_day,
        contains="same-day outcomes",
    )
    future_message = expect_runtime_error(
        guard,
        future,
        target_day,
        contains="future outcomes",
    )

    return {
        "prior_only_accepted": True,
        "same_day_rejected": True,
        "future_rejected": True,
        "same_day_message": same_message,
        "future_message": future_message,
    }


def run_optimizer_self_test(module) -> dict:
    production_fit = getattr(module, "fit_logistic", None)
    production_apply = getattr(module, "apply_logistic", None)
    if production_fit is None or production_apply is None:
        raise RuntimeError(
            "Production script is missing fit_logistic()/apply_logistic()"
        )

    x = np.array(
        [0.10, 0.20, 0.30, 0.40, 0.60, 0.70, 0.80, 0.90],
        dtype=float,
    )
    y = np.array(
        [0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0],
        dtype=float,
    )
    model = production_fit(x, y)
    pred = production_apply(model, x)
    if not np.all(np.isfinite(pred)):
        raise AssertionError(
            "Production apply_logistic() produced non-finite probabilities"
        )
    if np.any((pred <= 0.0) | (pred >= 1.0)):
        raise AssertionError(
            "Production apply_logistic() produced probability outside (0, 1)"
        )

    return {
        "production_fit_converged": True,
        "prediction_min": float(np.min(pred)),
        "prediction_max": float(np.max(pred)),
    }



def run_derivative_consistency_self_test(module) -> dict:
    objective = getattr(module, "_logistic_objective", None)
    gradient = getattr(module, "_logistic_gradient", None)
    hessian = getattr(module, "_logistic_hessian", None)
    if objective is None or gradient is None or hessian is None:
        raise RuntimeError(
            "Production script is missing logistic derivative helpers"
        )

    design = np.array(
        [
            [1.0, -1.25, 0.20],
            [1.0, -0.50, 0.65],
            [1.0, 0.15, -0.40],
            [1.0, 0.80, 0.35],
            [1.0, 1.40, -0.15],
        ],
        dtype=float,
    )
    y = np.array([0.0, 0.0, 1.0, 1.0, 1.0], dtype=float)
    beta = np.array([0.10, -0.35, 0.55], dtype=float)
    regularization = 1e-4
    step = 1e-5

    analytic_grad = np.asarray(
        gradient(beta, design, y, regularization),
        dtype=float,
    )
    analytic_hess = np.asarray(
        hessian(beta, design, regularization),
        dtype=float,
    )

    numeric_grad = np.empty_like(beta)
    for idx in range(len(beta)):
        delta = np.zeros_like(beta)
        delta[idx] = step
        numeric_grad[idx] = (
            objective(beta + delta, design, y, regularization)
            - objective(beta - delta, design, y, regularization)
        ) / (2.0 * step)

    numeric_hess = np.empty_like(analytic_hess)
    for idx in range(len(beta)):
        delta = np.zeros_like(beta)
        delta[idx] = step
        grad_plus = np.asarray(
            gradient(beta + delta, design, y, regularization),
            dtype=float,
        )
        grad_minus = np.asarray(
            gradient(beta - delta, design, y, regularization),
            dtype=float,
        )
        numeric_hess[:, idx] = (grad_plus - grad_minus) / (2.0 * step)

    grad_max_abs_error = float(np.max(np.abs(analytic_grad - numeric_grad)))
    hess_max_abs_error = float(np.max(np.abs(analytic_hess - numeric_hess)))
    hess_symmetry_error = float(np.max(np.abs(analytic_hess - analytic_hess.T)))

    if grad_max_abs_error > 1e-7:
        raise AssertionError(
            "Production logistic gradient finite-difference check failed: "
            f"max_abs_error={grad_max_abs_error:.12g}"
        )
    if hess_max_abs_error > 1e-7:
        raise AssertionError(
            "Production logistic Hessian finite-difference check failed: "
            f"max_abs_error={hess_max_abs_error:.12g}"
        )
    if hess_symmetry_error > 1e-12:
        raise AssertionError(
            "Production logistic Hessian symmetry check failed: "
            f"max_abs_error={hess_symmetry_error:.12g}"
        )

    return {
        "passed": True,
        "finite_difference_step": step,
        "gradient_max_abs_error": grad_max_abs_error,
        "hessian_max_abs_error": hess_max_abs_error,
        "hessian_symmetry_max_abs_error": hess_symmetry_error,
    }

def load_min_train_rows() -> int:
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"Missing config: {CONFIG_PATH}")
    payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    try:
        value = int(payload["markets"]["nhl"]["secondary_model"]["min_train_rows"])
    except Exception as exc:
        raise RuntimeError(
            "Missing markets.nhl.secondary_model.min_train_rows "
            f"in {CONFIG_PATH}"
        ) from exc
    if value < 1:
        raise RuntimeError(
            f"Configured min_train_rows must be >= 1, got {value}"
        )
    return value


def load_history() -> tuple[pd.DataFrame, list[str]]:
    files = sorted(HISTORY_ROOT.glob("season_*/standalone_comparison.csv"))
    if not files:
        raise RuntimeError(
            f"No standalone comparison history found under {HISTORY_ROOT}"
        )

    frames: list[pd.DataFrame] = []
    used_files: list[str] = []

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": "string"})
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"{path} missing required columns: {missing}"
            )
        frame = df[REQUIRED_COLUMNS].copy()
        frame["_source_file"] = str(path)
        frames.append(frame)
        used_files.append(str(path))

    history = pd.concat(frames, ignore_index=True)
    history["game_id"] = history["game_id"].map(canonical_game_id)
    history["_date"] = normalized_date_series(history["game_date"])

    for col in [
        "drat_home_win_prob",
        "sdv_home_win_prob",
        "actual_home_win",
    ]:
        history[col] = pd.to_numeric(history[col], errors="coerce")

    history = history.dropna(
        subset=[
            "game_id",
            "_date",
            "drat_home_win_prob",
            "sdv_home_win_prob",
            "actual_home_win",
        ]
    ).copy()

    if history["game_id"].eq("").any():
        raise RuntimeError("Historical comparison contains blank game_id")

    history["drat_home_win_prob"] = validate_probability(
        history["drat_home_win_prob"].to_numpy(float),
        "historical D-Ratings probability",
    )
    history["sdv_home_win_prob"] = validate_probability(
        history["sdv_home_win_prob"].to_numpy(float),
        "historical SDV probability",
    )

    outcomes = sorted(history["actual_home_win"].unique().tolist())
    if any(value not in (0.0, 1.0) for value in outcomes):
        raise RuntimeError(
            f"actual_home_win contains non-binary values: {outcomes}"
        )

    history = history.sort_values(
        ["_date", "game_id", "_source_file"]
    )
    history = history.drop_duplicates("game_id", keep="last").reset_index(
        drop=True
    )

    return history, used_files


def assert_prior_only(
    train: pd.DataFrame,
    target_day: date,
) -> None:
    if train["_date"].isna().any():
        raise RuntimeError(
            "Audit leakage assertion failed: training contains invalid date"
        )
    same_day = train["_date"] == target_day
    if same_day.any():
        ids = train.loc[same_day, "game_id"].astype(str).head(10).tolist()
        raise RuntimeError(
            "Audit leakage detected: same-day outcomes entered training; "
            f"target_day={target_day.isoformat()} sample_game_ids={ids}"
        )
    future = train["_date"] > target_day
    if future.any():
        ids = train.loc[future, "game_id"].astype(str).head(10).tolist()
        raise RuntimeError(
            "Audit leakage detected: future rows entered training; "
            f"target_day={target_day.isoformat()} sample_game_ids={ids}"
        )
    if (train["_date"] >= target_day).any():
        raise RuntimeError(
            "Audit leakage detected: historical_date < target_date violated"
        )


def candidate_key(
    transform: str,
    regularization: float,
) -> str:
    return f"{transform}|lambda={regularization:.12g}"


def evaluate_walk_forward(
    history: pd.DataFrame,
    *,
    min_train_rows: int,
    regularization_strengths: Iterable[float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    predictions: list[dict] = []
    failures: list[dict] = []

    target_dates = sorted(history["_date"].unique())

    for target_day in target_dates:
        train = history[history["_date"] < target_day].copy()
        test = history[history["_date"] == target_day].copy()

        assert_prior_only(train, target_day)

        if train.empty or len(train) < min_train_rows:
            continue
        if train["actual_home_win"].nunique() < 2:
            continue
        if test.empty:
            continue

        history_max = max(train["_date"])
        if history_max >= target_day:
            raise RuntimeError(
                "Audit leakage detected after training selection: "
                f"history_max={history_max.isoformat()} "
                f"target_day={target_day.isoformat()}"
            )

        y_train = train["actual_home_win"].to_numpy(float)
        y_test = test["actual_home_win"].to_numpy(float)

        drat_train_raw = train["drat_home_win_prob"].to_numpy(float)
        sdv_train_raw = train["sdv_home_win_prob"].to_numpy(float)
        drat_test_raw = test["drat_home_win_prob"].to_numpy(float)
        sdv_test_raw = test["sdv_home_win_prob"].to_numpy(float)

        for transform in TRANSFORMS:
            drat_train = transform_probability(drat_train_raw, transform)
            sdv_train = transform_probability(sdv_train_raw, transform)
            drat_test = transform_probability(drat_test_raw, transform)
            sdv_test = transform_probability(sdv_test_raw, transform)

            meta_train = probability_feature_matrix(
                drat_train_raw,
                sdv_train_raw,
                transform,
            )
            meta_test = probability_feature_matrix(
                drat_test_raw,
                sdv_test_raw,
                transform,
            )

            for regularization in regularization_strengths:
                key = candidate_key(transform, regularization)
                try:
                    drat_model = fit_logistic(
                        drat_train,
                        y_train,
                        regularization=regularization,
                    )
                    sdv_model = fit_logistic(
                        sdv_train,
                        y_train,
                        regularization=regularization,
                    )
                    meta_model = fit_logistic(
                        meta_train,
                        y_train,
                        regularization=regularization,
                    )

                    drat_train_cal = apply_logistic(
                        drat_model,
                        drat_train,
                    )
                    sdv_train_cal = apply_logistic(
                        sdv_model,
                        sdv_train,
                    )
                    weight = select_probability_weight(
                        y_train,
                        drat_train_cal,
                        sdv_train_cal,
                    )

                    drat_test_cal = apply_logistic(
                        drat_model,
                        drat_test,
                    )
                    sdv_test_cal = apply_logistic(
                        sdv_model,
                        sdv_test,
                    )
                    weighted_test = (
                        weight * drat_test_cal
                        + (1.0 - weight) * sdv_test_cal
                    )
                    meta_test_prob = apply_logistic(
                        meta_model,
                        meta_test,
                    )

                    for row_index, (_, row) in enumerate(test.iterrows()):
                        predictions.append(
                            {
                                "target_date": target_day.isoformat(),
                                "game_id": str(row["game_id"]),
                                "candidate": key,
                                "transform": transform,
                                "regularization": regularization,
                                "current_production_setting": (
                                    transform == "raw"
                                    and math.isclose(
                                        regularization,
                                        1e-4,
                                        rel_tol=0.0,
                                        abs_tol=1e-15,
                                    )
                                ),
                                "train_rows": len(train),
                                "history_max_game_date": (
                                    history_max.isoformat()
                                ),
                                "drat_weight": weight,
                                "actual_home_win": float(y_test[row_index]),
                                "drat_raw_prob": float(
                                    drat_test_raw[row_index]
                                ),
                                "sdv_raw_prob": float(
                                    sdv_test_raw[row_index]
                                ),
                                "drat_cal_prob": float(
                                    drat_test_cal[row_index]
                                ),
                                "sdv_cal_prob": float(
                                    sdv_test_cal[row_index]
                                ),
                                "weighted_prob": float(
                                    weighted_test[row_index]
                                ),
                                "meta_prob": float(
                                    meta_test_prob[row_index]
                                ),
                            }
                        )
                except Exception as exc:
                    failures.append(
                        {
                            "target_date": target_day.isoformat(),
                            "candidate": key,
                            "transform": transform,
                            "regularization": regularization,
                            "train_rows": len(train),
                            "history_max_game_date": (
                                history_max.isoformat()
                            ),
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )

    prediction_columns = [
        "target_date",
        "game_id",
        "candidate",
        "transform",
        "regularization",
        "current_production_setting",
        "train_rows",
        "history_max_game_date",
        "drat_weight",
        "actual_home_win",
        "drat_raw_prob",
        "sdv_raw_prob",
        "drat_cal_prob",
        "sdv_cal_prob",
        "weighted_prob",
        "meta_prob",
    ]
    failure_columns = [
        "target_date",
        "candidate",
        "transform",
        "regularization",
        "train_rows",
        "history_max_game_date",
        "error_type",
        "error",
    ]
    return (
        pd.DataFrame(predictions, columns=prediction_columns),
        pd.DataFrame(failures, columns=failure_columns),
    )


def summarize_predictions(
    predictions: pd.DataFrame,
    failures: pd.DataFrame,
) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame(
            columns=[
                "candidate",
                "transform",
                "regularization",
                "output",
                "rows",
                "target_dates",
                "brier",
                "log_loss",
                "fit_failures",
                "current_production_setting",
            ]
        )

    failure_counts: dict[str, int] = {}
    if not failures.empty:
        failure_counts = (
            failures.groupby("candidate").size().astype(int).to_dict()
        )

    rows: list[dict] = []
    output_columns = [
        "drat_cal_prob",
        "sdv_cal_prob",
        "weighted_prob",
        "meta_prob",
    ]

    for candidate, part in predictions.groupby("candidate", sort=True):
        transform = str(part["transform"].iloc[0])
        regularization = float(part["regularization"].iloc[0])
        current_setting = bool(
            part["current_production_setting"].iloc[0]
        )
        y = part["actual_home_win"].to_numpy(float)

        for output in output_columns:
            p = part[output].to_numpy(float)
            rows.append(
                {
                    "candidate": candidate,
                    "transform": transform,
                    "regularization": regularization,
                    "output": output,
                    "rows": len(part),
                    "target_dates": int(
                        part["target_date"].nunique()
                    ),
                    "brier": brier_score(y, p),
                    "log_loss": log_loss(y, p),
                    "fit_failures": int(
                        failure_counts.get(candidate, 0)
                    ),
                    "current_production_setting": current_setting,
                }
            )

    return pd.DataFrame(rows).sort_values(
        [
            "output",
            "fit_failures",
            "brier",
            "log_loss",
            "transform",
            "regularization",
        ]
    ).reset_index(drop=True)


def ensure_regularization_grid(values: Iterable[float]) -> list[float]:
    grid = sorted(set(float(value) for value in values))
    if len(grid) < 2:
        raise ValueError(
            "Item 10 requires multiple regularization strengths; "
            f"received {grid}"
        )
    for value in grid:
        if value < 0.0 or not math.isfinite(value):
            raise ValueError(
                "Regularization strengths must be finite and >= 0; "
                f"received {value}"
            )
    if not any(
        math.isclose(value, 1e-4, rel_tol=0.0, abs_tol=1e-15)
        for value in grid
    ):
        raise ValueError(
            "Regularization grid must include current production "
            "strength 1e-4 for direct comparison"
        )
    return grid


def write_outputs(
    predictions: pd.DataFrame,
    summary: pd.DataFrame,
    failures: pd.DataFrame,
    report: dict,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(PREDICTIONS_PATH, index=False)
    summary.to_csv(SUMMARY_PATH, index=False)
    failures.to_csv(FAILURES_PATH, index=False)
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    regularization_strengths = ensure_regularization_grid(
        args.regularization_strengths
    )

    production = load_production_module()
    leakage_self_test = run_production_leakage_guard_self_test(
        production
    )
    optimizer_self_test = run_optimizer_self_test(production)
    derivative_self_test = run_derivative_consistency_self_test(production)

    if args.self_test_only:
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "leakage_guard": leakage_self_test,
                    "optimizer_guard": optimizer_self_test,
                    "derivative_consistency": derivative_self_test,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return

    min_train_rows = (
        int(args.min_train_rows)
        if args.min_train_rows is not None
        else load_min_train_rows()
    )
    if min_train_rows < 1:
        raise ValueError(
            f"min_train_rows must be >= 1, got {min_train_rows}"
        )

    history, history_files = load_history()
    predictions, failures = evaluate_walk_forward(
        history,
        min_train_rows=min_train_rows,
        regularization_strengths=regularization_strengths,
    )
    summary = summarize_predictions(predictions, failures)

    eligible_dates = (
        sorted(predictions["target_date"].unique().tolist())
        if not predictions.empty
        else []
    )

    current_summary = []
    if not summary.empty:
        current_rows = summary[
            summary["current_production_setting"]
        ]
        current_summary = current_rows.to_dict(orient="records")

    report = {
        "status": "COMPLETE",
        "purpose": (
            "Item 10 implementation audit; results are descriptive and "
            "do not automatically change production calibration."
        ),
        "production_script": str(PRODUCTION_SCRIPT),
        "history_files": history_files,
        "history_rows": len(history),
        "history_first_date": (
            min(history["_date"]).isoformat()
            if not history.empty
            else None
        ),
        "history_last_date": (
            max(history["_date"]).isoformat()
            if not history.empty
            else None
        ),
        "min_train_rows": min_train_rows,
        "transforms": list(TRANSFORMS),
        "regularization_strengths": regularization_strengths,
        "current_production_candidate": "raw|lambda=0.0001",
        "leakage_guard_self_test": leakage_self_test,
        "optimizer_guard_self_test": optimizer_self_test,
        "derivative_consistency_self_test": derivative_self_test,
        "walk_forward_rule": "historical_date < target_date",
        "eligible_target_dates": len(eligible_dates),
        "prediction_rows": len(predictions),
        "fit_failure_rows": len(failures),
        "current_production_metrics": current_summary,
        "outputs": {
            "predictions": str(PREDICTIONS_PATH),
            "summary": str(SUMMARY_PATH),
            "failures": str(FAILURES_PATH),
            "report": str(REPORT_PATH),
        },
        "promotion_decision": None,
    }

    write_outputs(
        predictions,
        summary,
        failures,
        report,
    )

    print(
        "Item 10 secondary probability calibration audit complete: "
        f"history_rows={len(history)} "
        f"prediction_rows={len(predictions)} "
        f"fit_failures={len(failures)}"
    )
    print(f"Summary: {SUMMARY_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
