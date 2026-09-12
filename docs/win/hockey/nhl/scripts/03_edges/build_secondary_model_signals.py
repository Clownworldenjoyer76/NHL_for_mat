#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/03_edges/build_secondary_model_signals.py

from __future__ import annotations

import math
import sys
import traceback
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from scipy.optimize import minimize


BASE_DIR = Path(__file__).resolve().parents[2]
EV_DIR = BASE_DIR / "03_edges" / "ev_kelly"
MERGE_DIR = BASE_DIR / "01_merge"
OUTPUT_DIR = BASE_DIR / "03_edges" / "secondary_signals"
HISTORY_ROOT = BASE_DIR / "research" / "sdv_challenger"
CONFIG_PATH = BASE_DIR / "config" / "markets.yaml"
ERROR_DIR = BASE_DIR / "errors" / "03_edges"
LOG_FILE = ERROR_DIR / "build_secondary_model_signals.txt"

SIGNAL_VERSION = "P6-production-v1"
EPS = 1e-6

SIGNAL_COLUMNS = [
    "drat_home_win_prob",
    "drat_exp_margin",
    "drat_exp_total",
    "sdv_home_win_prob",
    "sdv_exp_margin",
    "sdv_exp_total",
    "prob_disagreement",
    "margin_disagreement",
    "total_disagreement",
    "prob_disagreement_threshold_p75_prior",
    "margin_disagreement_threshold_p75_prior",
    "total_disagreement_threshold_p75_prior",
    "high_prob_disagreement_flag",
    "high_margin_disagreement_flag",
    "high_total_disagreement_flag",
    "ensemble_train_rows",
    "weighted_prob_drat_weight",
    "weighted_margin_drat_weight",
    "weighted_total_drat_weight",
    "weighted_home_win_prob",
    "weighted_exp_margin",
    "weighted_exp_total",
    "meta_home_win_prob",
    "meta_exp_margin",
    "meta_exp_total",
    "secondary_history_max_game_date",
    "secondary_model_status",
    "secondary_signal_version",
]

HISTORY_REQUIRED_COLUMNS = [
    "game_id",
    "game_date",
    "sdv_home_win_prob",
    "sdv_exp_margin",
    "sdv_exp_total",
    "drat_home_win_prob",
    "drat_exp_margin",
    "drat_exp_total",
    "actual_home_win",
    "actual_margin",
    "actual_total",
]

MERGED_REQUIRED_COLUMNS = [
    "game_id",
    "game_date",
    "home_prob_moneyline",
    "away_projected_goals",
    "home_projected_goals",
    "total_projected_goals",
    "sdv_home_win_prob",
    "sdv_exp_margin",
    "sdv_exp_total",
]


@dataclass(frozen=True)
class LinearModel:
    coefficients: np.ndarray


@dataclass(frozen=True)
class LogisticModel:
    coefficients: np.ndarray


@dataclass(frozen=True)
class ModelBundle:
    train_rows: int
    history_max_game_date: str
    prob_threshold: float
    margin_threshold: float
    total_threshold: float
    prob_weight: float
    margin_weight: float
    total_weight: float
    drat_cal: LogisticModel
    sdv_cal: LogisticModel
    prob_meta: LogisticModel
    margin_meta: LinearModel
    total_meta: LinearModel


def now() -> str:
    return datetime.now(UTC).isoformat()


def reset_log() -> None:
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(
        f"=== build_secondary_model_signals RUN {now()} ===\n",
        encoding="utf-8",
    )


def log(message: str) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{now()} | {message}\n")


def fail(message: str) -> None:
    log(f"FATAL | {message}")
    raise SystemExit(message)


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


def load_secondary_config() -> dict:
    if not CONFIG_PATH.exists():
        fail(f"Missing markets config: {CONFIG_PATH}")

    try:
        payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"Unable to read {CONFIG_PATH}: {exc}")

    try:
        config = payload["markets"]["nhl"]["secondary_model"]
    except Exception as exc:
        fail(f"Missing markets.nhl.secondary_model in {CONFIG_PATH}: {exc}")

    return config


def require_columns(df: pd.DataFrame, required: list[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        fail(f"{label} missing required columns: {missing}")


def load_merged_predictions() -> pd.DataFrame:
    files = sorted(MERGE_DIR.glob("*_NHL_merged.csv"))
    if not files:
        fail(f"No Stage 01 merged files found in {MERGE_DIR}")

    frames: list[pd.DataFrame] = []
    for path in files:
        df = pd.read_csv(path, dtype={"game_id": "string"})
        require_columns(df, MERGED_REQUIRED_COLUMNS, str(path))
        if df.empty:
            continue
        df = df[MERGED_REQUIRED_COLUMNS].copy()
        df["_source_file"] = path.name
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=MERGED_REQUIRED_COLUMNS)

    out = pd.concat(frames, ignore_index=True)
    out["game_id"] = out["game_id"].map(canonical_game_id)
    if out["game_id"].eq("").any():
        fail("Stage 01 merged predictions contain blank game_id")

    dupes = out[out.duplicated("game_id", keep=False)]
    if not dupes.empty:
        ids = sorted(dupes["game_id"].unique().tolist())
        fail(f"Stage 01 merged predictions contain duplicate game_id: {ids[:10]}")

    numeric = [
        "home_prob_moneyline",
        "away_projected_goals",
        "home_projected_goals",
        "total_projected_goals",
        "sdv_home_win_prob",
        "sdv_exp_margin",
        "sdv_exp_total",
    ]
    for col in numeric:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["drat_home_win_prob"] = out["home_prob_moneyline"]
    out["drat_exp_margin"] = (
        out["home_projected_goals"] - out["away_projected_goals"]
    )
    out["drat_exp_total"] = out["total_projected_goals"]
    out["_target_date"] = normalized_date_series(out["game_date"])

    if out["_target_date"].isna().any():
        bad = out.loc[out["_target_date"].isna(), ["game_id", "game_date"]]
        fail(
            "Stage 01 merged predictions contain invalid game_date: "
            f"{bad.to_dict(orient='records')[:5]}"
        )

    return out[
        [
            "game_id",
            "game_date",
            "_target_date",
            "drat_home_win_prob",
            "drat_exp_margin",
            "drat_exp_total",
            "sdv_home_win_prob",
            "sdv_exp_margin",
            "sdv_exp_total",
        ]
    ].copy()


def load_history() -> pd.DataFrame:
    files = sorted(HISTORY_ROOT.glob("season_*/standalone_comparison.csv"))
    frames: list[pd.DataFrame] = []

    for path in files:
        try:
            df = pd.read_csv(path, dtype={"game_id": "string"})
        except Exception as exc:
            log(f"WARN | unable to read historical comparison {path}: {exc}")
            continue

        missing = [c for c in HISTORY_REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            log(f"WARN | skipping {path}; missing columns={missing}")
            continue

        df = df[HISTORY_REQUIRED_COLUMNS].copy()
        df["_source_file"] = str(path)
        frames.append(df)

    if not frames:
        log("No usable P6 standalone comparison history found.")
        return pd.DataFrame(columns=HISTORY_REQUIRED_COLUMNS + ["_date"])

    out = pd.concat(frames, ignore_index=True)
    out["game_id"] = out["game_id"].map(canonical_game_id)
    out["_date"] = normalized_date_series(out["game_date"])

    numeric = [
        c for c in HISTORY_REQUIRED_COLUMNS if c not in {"game_id", "game_date"}
    ]
    for col in numeric:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["_date", *numeric]).copy()
    out = out.sort_values(["_date", "game_id", "_source_file"])
    out = out.drop_duplicates("game_id", keep="last")

    log(
        f"Loaded P6 completed comparison history: rows={len(out)} files={len(files)}"
    )
    return out


def clipped_prob(values: np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(values, dtype=float), EPS, 1.0 - EPS)


def log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = clipped_prob(p)
    y = np.asarray(y, dtype=float)
    return float(
        -np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
    )


def rmse(y: np.ndarray, pred: np.ndarray) -> float:
    return float(
        np.sqrt(np.mean((np.asarray(pred, float) - np.asarray(y, float)) ** 2))
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


def fit_logistic(x: np.ndarray, y: np.ndarray) -> LogisticModel:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    if len(x) != len(y):
        raise ValueError("x and y row counts differ")
    if len(y) == 0:
        raise ValueError("cannot fit logistic model with zero rows")
    if not np.all(np.isfinite(x)):
        raise RuntimeError(
            "logistic calibration training features contain non-finite values"
        )
    if not np.all(np.isfinite(y)):
        raise RuntimeError(
            "logistic calibration training target contains non-finite values"
        )

    design = np.column_stack([np.ones(len(x)), x])
    regularization = 1e-4
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
            if not result.success:
                log(
                    "WARN | logistic calibration accepted by stationarity "
                    f"after {method} termination: status={result.status} "
                    f"grad_inf_norm={grad_inf_norm:.3e} "
                    f"objective={float(result.fun):.12g}"
                )
            return LogisticModel(np.asarray(result.x, dtype=float))

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
        "logistic calibration optimizer did not converge after fallback; "
        + " | ".join(failures)
    )


def apply_logistic(model: LogisticModel, x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    design = np.column_stack([np.ones(len(x)), x])
    return _stable_sigmoid(design @ model.coefficients)

def fit_linear(x: np.ndarray, y: np.ndarray) -> LinearModel:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    design = np.column_stack([np.ones(len(x)), x])
    ridge = 1e-6 * np.eye(design.shape[1])
    ridge[0, 0] = 0.0
    beta = np.linalg.solve(design.T @ design + ridge, design.T @ y)
    return LinearModel(beta)


def apply_linear(model: LinearModel, x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    design = np.column_stack([np.ones(len(x)), x])
    return design @ model.coefficients


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


def select_numeric_weight(
    y: np.ndarray,
    drat: np.ndarray,
    sdv: np.ndarray,
) -> float:
    grid = np.linspace(0.0, 1.0, 101)
    losses = [
        rmse(y, w * drat + (1.0 - w) * sdv)
        for w in grid
    ]
    return float(grid[int(np.argmin(losses))])


def assert_prior_only_training(
    train: pd.DataFrame,
    target_day: date,
) -> None:
    if "_date" not in train.columns:
        raise RuntimeError(
            "secondary calibration leakage assertion cannot run: "
            "training frame is missing _date"
        )
    if train["_date"].isna().any():
        raise RuntimeError(
            "secondary calibration leakage assertion failed: "
            "training frame contains invalid dates"
        )

    same_day = train["_date"] == target_day
    if same_day.any():
        sample_ids = (
            train.loc[same_day, "game_id"].astype(str).head(10).tolist()
            if "game_id" in train.columns
            else []
        )
        raise RuntimeError(
            "secondary calibration leakage detected: same-day outcomes "
            f"entered target_day={target_day.isoformat()} training set; "
            f"sample_game_ids={sample_ids}"
        )

    future = train["_date"] > target_day
    if future.any():
        sample_ids = (
            train.loc[future, "game_id"].astype(str).head(10).tolist()
            if "game_id" in train.columns
            else []
        )
        raise RuntimeError(
            "secondary calibration leakage detected: future outcomes "
            f"entered target_day={target_day.isoformat()} training set; "
            f"sample_game_ids={sample_ids}"
        )

    not_prior = train["_date"] >= target_day
    if not_prior.any():
        raise RuntimeError(
            "secondary calibration leakage detected: training dates must "
            "satisfy historical_date < target_date"
        )


def fit_bundle_for_target(
    history: pd.DataFrame,
    target_day: date,
    *,
    min_train_rows: int,
    disagreement_quantile: float,
) -> ModelBundle | None:
    train = history[history["_date"] < target_day].copy()
    assert_prior_only_training(train, target_day)

    if len(train) < min_train_rows:
        return None
    if train["actual_home_win"].nunique() < 2:
        return None

    y = train["actual_home_win"].to_numpy(float)
    drat_cal = fit_logistic(
        train[["drat_home_win_prob"]].to_numpy(float),
        y,
    )
    sdv_cal = fit_logistic(
        train[["sdv_home_win_prob"]].to_numpy(float),
        y,
    )
    drat_cal_values = apply_logistic(
        drat_cal,
        train[["drat_home_win_prob"]].to_numpy(float),
    )
    sdv_cal_values = apply_logistic(
        sdv_cal,
        train[["sdv_home_win_prob"]].to_numpy(float),
    )

    prob_weight = select_probability_weight(
        y,
        drat_cal_values,
        sdv_cal_values,
    )
    margin_weight = select_numeric_weight(
        train["actual_margin"].to_numpy(float),
        train["drat_exp_margin"].to_numpy(float),
        train["sdv_exp_margin"].to_numpy(float),
    )
    total_weight = select_numeric_weight(
        train["actual_total"].to_numpy(float),
        train["drat_exp_total"].to_numpy(float),
        train["sdv_exp_total"].to_numpy(float),
    )

    prob_disagreement = (
        train["sdv_home_win_prob"] - train["drat_home_win_prob"]
    ).abs()
    margin_disagreement = (
        train["sdv_exp_margin"] - train["drat_exp_margin"]
    ).abs()
    total_disagreement = (
        train["sdv_exp_total"] - train["drat_exp_total"]
    ).abs()

    prob_meta = fit_logistic(
        np.column_stack(
            [
                train["drat_home_win_prob"].to_numpy(float),
                train["sdv_home_win_prob"].to_numpy(float),
                prob_disagreement.to_numpy(float),
            ]
        ),
        y,
    )
    margin_meta = fit_linear(
        np.column_stack(
            [
                train["drat_exp_margin"].to_numpy(float),
                train["sdv_exp_margin"].to_numpy(float),
                margin_disagreement.to_numpy(float),
            ]
        ),
        train["actual_margin"].to_numpy(float),
    )
    total_meta = fit_linear(
        np.column_stack(
            [
                train["drat_exp_total"].to_numpy(float),
                train["sdv_exp_total"].to_numpy(float),
                total_disagreement.to_numpy(float),
            ]
        ),
        train["actual_total"].to_numpy(float),
    )

    history_max = max(train["_date"])
    if history_max >= target_day:
        raise RuntimeError(
            "secondary calibration leakage detected after fitting: "
            f"history_max={history_max.isoformat()} "
            f"target_day={target_day.isoformat()}"
        )

    return ModelBundle(
        train_rows=len(train),
        history_max_game_date=history_max.isoformat(),
        prob_threshold=float(
            prob_disagreement.quantile(disagreement_quantile)
        ),
        margin_threshold=float(
            margin_disagreement.quantile(disagreement_quantile)
        ),
        total_threshold=float(
            total_disagreement.quantile(disagreement_quantile)
        ),
        prob_weight=prob_weight,
        margin_weight=margin_weight,
        total_weight=total_weight,
        drat_cal=drat_cal,
        sdv_cal=sdv_cal,
        prob_meta=prob_meta,
        margin_meta=margin_meta,
        total_meta=total_meta,
    )


def blank_signal_row(base: pd.Series, status: str) -> dict:
    result = {column: np.nan for column in SIGNAL_COLUMNS}
    result.update(
        {
            "drat_home_win_prob": base.get("drat_home_win_prob"),
            "drat_exp_margin": base.get("drat_exp_margin"),
            "drat_exp_total": base.get("drat_exp_total"),
            "sdv_home_win_prob": base.get("sdv_home_win_prob"),
            "sdv_exp_margin": base.get("sdv_exp_margin"),
            "sdv_exp_total": base.get("sdv_exp_total"),
            "secondary_model_status": status,
            "secondary_signal_version": SIGNAL_VERSION,
        }
    )
    return result


def signal_row(
    base: pd.Series,
    bundle: ModelBundle | None,
    history_available: bool,
) -> dict:
    sdv_values = [
        base.get("sdv_home_win_prob"),
        base.get("sdv_exp_margin"),
        base.get("sdv_exp_total"),
    ]
    if any(pd.isna(v) for v in sdv_values):
        return blank_signal_row(base, "sdv_unavailable")

    prob_disagreement = abs(
        float(base["sdv_home_win_prob"])
        - float(base["drat_home_win_prob"])
    )
    margin_disagreement = abs(
        float(base["sdv_exp_margin"])
        - float(base["drat_exp_margin"])
    )
    total_disagreement = abs(
        float(base["sdv_exp_total"])
        - float(base["drat_exp_total"])
    )

    if bundle is None:
        row = blank_signal_row(
            base,
            "insufficient_history"
            if history_available
            else "history_unavailable",
        )
        row.update(
            {
                "prob_disagreement": prob_disagreement,
                "margin_disagreement": margin_disagreement,
                "total_disagreement": total_disagreement,
            }
        )
        return row

    drat_prob = np.array(
        [[float(base["drat_home_win_prob"])]]
    )
    sdv_prob = np.array(
        [[float(base["sdv_home_win_prob"])]]
    )
    drat_cal = float(
        apply_logistic(bundle.drat_cal, drat_prob)[0]
    )
    sdv_cal = float(
        apply_logistic(bundle.sdv_cal, sdv_prob)[0]
    )
    weighted_prob = (
        bundle.prob_weight * drat_cal
        + (1.0 - bundle.prob_weight) * sdv_cal
    )

    weighted_margin = (
        bundle.margin_weight * float(base["drat_exp_margin"])
        + (1.0 - bundle.margin_weight)
        * float(base["sdv_exp_margin"])
    )
    weighted_total = (
        bundle.total_weight * float(base["drat_exp_total"])
        + (1.0 - bundle.total_weight)
        * float(base["sdv_exp_total"])
    )

    meta_prob = float(
        apply_logistic(
            bundle.prob_meta,
            np.array(
                [[
                    float(base["drat_home_win_prob"]),
                    float(base["sdv_home_win_prob"]),
                    prob_disagreement,
                ]]
            ),
        )[0]
    )
    meta_margin = float(
        apply_linear(
            bundle.margin_meta,
            np.array(
                [[
                    float(base["drat_exp_margin"]),
                    float(base["sdv_exp_margin"]),
                    margin_disagreement,
                ]]
            ),
        )[0]
    )
    meta_total = float(
        apply_linear(
            bundle.total_meta,
            np.array(
                [[
                    float(base["drat_exp_total"]),
                    float(base["sdv_exp_total"]),
                    total_disagreement,
                ]]
            ),
        )[0]
    )

    return {
        "drat_home_win_prob": float(
            base["drat_home_win_prob"]
        ),
        "drat_exp_margin": float(
            base["drat_exp_margin"]
        ),
        "drat_exp_total": float(
            base["drat_exp_total"]
        ),
        "sdv_home_win_prob": float(
            base["sdv_home_win_prob"]
        ),
        "sdv_exp_margin": float(
            base["sdv_exp_margin"]
        ),
        "sdv_exp_total": float(
            base["sdv_exp_total"]
        ),
        "prob_disagreement": prob_disagreement,
        "margin_disagreement": margin_disagreement,
        "total_disagreement": total_disagreement,
        "prob_disagreement_threshold_p75_prior":
            bundle.prob_threshold,
        "margin_disagreement_threshold_p75_prior":
            bundle.margin_threshold,
        "total_disagreement_threshold_p75_prior":
            bundle.total_threshold,
        "high_prob_disagreement_flag": int(
            prob_disagreement >= bundle.prob_threshold
        ),
        "high_margin_disagreement_flag": int(
            margin_disagreement >= bundle.margin_threshold
        ),
        "high_total_disagreement_flag": int(
            total_disagreement >= bundle.total_threshold
        ),
        "ensemble_train_rows": bundle.train_rows,
        "weighted_prob_drat_weight": bundle.prob_weight,
        "weighted_margin_drat_weight": bundle.margin_weight,
        "weighted_total_drat_weight": bundle.total_weight,
        "weighted_home_win_prob": float(
            np.clip(weighted_prob, 0.0, 1.0)
        ),
        "weighted_exp_margin": weighted_margin,
        "weighted_exp_total": weighted_total,
        "meta_home_win_prob": float(
            np.clip(meta_prob, 0.0, 1.0)
        ),
        "meta_exp_margin": meta_margin,
        "meta_exp_total": meta_total,
        "secondary_history_max_game_date":
            bundle.history_max_game_date,
        "secondary_model_status": "ready",
        "secondary_signal_version": SIGNAL_VERSION,
    }


def build_signal_frame(
    current: pd.DataFrame,
    history: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    min_train_rows = int(config["min_train_rows"])
    disagreement_quantile = float(
        config["disagreement_quantile"]
    )
    bundles: dict[date, ModelBundle | None] = {}

    for target_day in sorted(
        current["_target_date"].dropna().unique()
    ):
        bundles[target_day] = fit_bundle_for_target(
            history,
            target_day,
            min_train_rows=min_train_rows,
            disagreement_quantile=disagreement_quantile,
        )

    rows: list[dict] = []
    history_available = not history.empty
    for _, base in current.iterrows():
        target_day = base["_target_date"]
        row = {"game_id": base["game_id"]}
        row.update(
            signal_row(
                base,
                bundles.get(target_day),
                history_available,
            )
        )
        rows.append(row)

    return pd.DataFrame(
        rows,
        columns=["game_id", *SIGNAL_COLUMNS],
    )


def wipe_outputs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for path in OUTPUT_DIR.glob("*.csv"):
        path.unlink()


def enrich_ev_files(signals: pd.DataFrame) -> int:
    files = sorted(EV_DIR.glob("*_NHL_*.csv"))
    if not files:
        fail(f"No EV/Kelly market files found in {EV_DIR}")

    written = 0
    signal_ids = set(signals["game_id"].astype(str))

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": "string"})
        if "game_id" not in df.columns:
            fail(f"EV/Kelly file missing game_id: {path}")
        df["game_id"] = df["game_id"].map(
            canonical_game_id
        )

        dupes = df[df.duplicated("game_id", keep=False)]
        if not dupes.empty:
            fail(
                f"EV/Kelly file has duplicate game_id: {path}"
            )

        unknown = sorted(
            set(df["game_id"].astype(str)) - signal_ids
        )
        if unknown:
            fail(
                "EV/Kelly file contains game_id absent from "
                "Stage 01 merged predictions: "
                f"{path} ids={unknown[:10]}"
            )

        existing_signal_columns = [
            col for col in SIGNAL_COLUMNS if col in df.columns
        ]
        if existing_signal_columns:
            df = df.drop(columns=existing_signal_columns)

        enriched = df.merge(
            signals,
            on="game_id",
            how="left",
            validate="one_to_one",
        )
        out_path = OUTPUT_DIR / path.name
        enriched.to_csv(out_path, index=False)
        log(f"WROTE {out_path} rows={len(enriched)}")
        written += 1

    return written


def main() -> None:
    reset_log()
    try:
        config = load_secondary_config()
        current = load_merged_predictions()
        history = load_history()
        signals = build_signal_frame(
            current,
            history,
            config,
        )

        wipe_outputs()
        written = enrich_ev_files(signals)

        statuses = signals[
            "secondary_model_status"
        ].value_counts(dropna=False).to_dict()
        log(
            "STATUS: SUCCESS | "
            f"signal_rows={len(signals)} "
            f"files_written={written} "
            f"status_counts={statuses}"
        )
        print(
            "NHL secondary model signals complete: "
            f"signal_rows={len(signals)} "
            f"files_written={written}"
        )
    except SystemExit:
        raise
    except Exception as exc:
        log(
            f"FATAL | {exc}\n"
            f"{traceback.format_exc()}"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
