#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import platform
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
from scipy.optimize import minimize_scalar
from scipy.stats import nbinom, poisson
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore", category=PerformanceWarning)

NHL_REL = Path("docs/win/hockey/nhl")
HISTORY_REL = NHL_REL / "research/sdv_challenger"
MERGED_REL = NHL_REL / "archive/2025_26/01_merge"
OUTPUT_REL = NHL_REL / "research/total_secondary_probability"

SEED = 20260912
FINAL_TEST_FRACTION = 0.20
N_VALIDATION_FOLDS = 4
INITIAL_TRAIN_DATE_FRACTION = 0.40
MIN_TRAIN_ROWS = 150
MIN_VALIDATION_ROWS = 40
SIMULATIONS_PER_ROW = 5000
EPS = 1e-12

SOURCES = ("sdv", "weighted", "meta")
METHODS = (
    "poisson",
    "negative_binomial",
    "empirical_residual",
    "poisson_simulation",
    "direct_multinomial",
)

UNDER_CLASS = 0
PUSH_CLASS = 1
OVER_CLASS = 2


@dataclass(frozen=True)
class LinearModel:
    coefficients: np.ndarray


@dataclass(frozen=True)
class DerivedBundle:
    total_weight: float
    total_meta: LinearModel


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve()]
    seen: set[Path] = set()
    for start in starts:
        for candidate in (start, *start.parents):
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / NHL_REL).is_dir():
                return candidate
    raise RuntimeError(f"Unable to locate repository root containing {NHL_REL}")


def package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not-installed"


def canonical_game_id(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def parse_date(value):
    ts = pd.to_datetime(
        str(value).strip().replace("_", "-"),
        errors="coerce",
    )
    return None if pd.isna(ts) else ts.normalize()


def to_float(value):
    try:
        out = float(value)
    except Exception:
        return np.nan
    return out if math.isfinite(out) else np.nan


def american_to_decimal(value):
    odds = to_float(value)
    if not np.isfinite(odds) or odds == 0:
        return np.nan
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def resolve_decimal(decimal_value, american_value):
    decimal_odds = to_float(decimal_value)
    if np.isfinite(decimal_odds) and decimal_odds > 1.0:
        return decimal_odds
    return american_to_decimal(american_value)


def is_integer_line(line: float) -> bool:
    return abs(line - round(line)) < 1e-9


def classify_total(actual_total: float, line: float) -> int:
    if actual_total > line:
        return OVER_CLASS
    if actual_total < line:
        return UNDER_CLASS
    return PUSH_CLASS


def fit_linear(x, y) -> LinearModel:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    design = np.column_stack([np.ones(len(x)), x])
    ridge = 1e-6 * np.eye(design.shape[1])
    ridge[0, 0] = 0.0
    beta = np.linalg.solve(
        design.T @ design + ridge,
        design.T @ y,
    )
    return LinearModel(beta)


def apply_linear(model: LinearModel, x):
    x = np.asarray(x, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    design = np.column_stack([np.ones(len(x)), x])
    return design @ model.coefficients


def rmse(y, pred):
    y = np.asarray(y, dtype=float)
    pred = np.asarray(pred, dtype=float)
    return float(np.sqrt(np.mean((pred - y) ** 2)))


def select_numeric_weight(y, drat, sdv):
    y = np.asarray(y, dtype=float)
    drat = np.asarray(drat, dtype=float)
    sdv = np.asarray(sdv, dtype=float)
    grid = np.linspace(0.0, 1.0, 101)
    losses = [
        rmse(y, w * drat + (1.0 - w) * sdv)
        for w in grid
    ]
    return float(grid[int(np.argmin(losses))])


def fit_derived_bundle(train: pd.DataFrame) -> DerivedBundle:
    total_weight = select_numeric_weight(
        train["actual_total"],
        train["drat_exp_total"],
        train["sdv_exp_total"],
    )
    disagreement = (
        train["sdv_exp_total"] - train["drat_exp_total"]
    ).abs()

    total_meta = fit_linear(
        np.column_stack(
            [
                train["drat_exp_total"].to_numpy(float),
                train["sdv_exp_total"].to_numpy(float),
                disagreement.to_numpy(float),
            ]
        ),
        train["actual_total"].to_numpy(float),
    )
    return DerivedBundle(
        total_weight=total_weight,
        total_meta=total_meta,
    )


def source_prediction(
    source: str,
    df: pd.DataFrame,
    bundle: DerivedBundle,
) -> np.ndarray:
    if source == "sdv":
        return df["sdv_exp_total"].to_numpy(float)

    if source == "weighted":
        return (
            bundle.total_weight * df["drat_exp_total"].to_numpy(float)
            + (1.0 - bundle.total_weight)
            * df["sdv_exp_total"].to_numpy(float)
        )

    if source == "meta":
        disagreement = (
            df["sdv_exp_total"] - df["drat_exp_total"]
        ).abs().to_numpy(float)
        return apply_linear(
            bundle.total_meta,
            np.column_stack(
                [
                    df["drat_exp_total"].to_numpy(float),
                    df["sdv_exp_total"].to_numpy(float),
                    disagreement,
                ]
            ),
        )

    raise ValueError(source)


def load_history(root: Path) -> pd.DataFrame:
    files = sorted(
        (root / HISTORY_REL).glob("season_*/standalone_comparison.csv")
    )
    if not files:
        raise RuntimeError(
            f"No standalone comparison history under {root / HISTORY_REL}"
        )

    required = [
        "game_id",
        "game_date",
        "drat_exp_total",
        "sdv_exp_total",
        "actual_total",
    ]
    parts = []

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing history columns: {missing}")
        part = df[required].copy()
        part["history_source_file"] = str(path.relative_to(root))
        parts.append(part)

    history = pd.concat(parts, ignore_index=True)
    history["game_id"] = history["game_id"].map(canonical_game_id)
    history["game_date"] = history["game_date"].map(parse_date)

    for col in ("drat_exp_total", "sdv_exp_total", "actual_total"):
        history[col] = pd.to_numeric(history[col], errors="coerce")

    history = history.dropna(
        subset=[
            "game_id",
            "game_date",
            "drat_exp_total",
            "sdv_exp_total",
            "actual_total",
        ]
    ).copy()
    history = history[history["game_id"].ne("")].copy()
    history = history.sort_values(
        ["game_date", "game_id", "history_source_file"]
    )
    history = history.drop_duplicates("game_id", keep="last")
    return history


def load_archived_total_prices(root: Path) -> pd.DataFrame:
    files = sorted((root / MERGED_REL).glob("*_NHL_merged.csv"))
    if not files:
        raise RuntimeError(f"No archived merged files under {root / MERGED_REL}")

    required = [
        "game_id",
        "total",
        "dk_total_over_american",
        "dk_total_under_american",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
    ]
    parts = []

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing total columns: {missing}")
        part = df[required].copy()
        part["price_source_file"] = path.name
        parts.append(part)

    prices = pd.concat(parts, ignore_index=True)
    prices["game_id"] = prices["game_id"].map(canonical_game_id)

    for col in required[1:]:
        prices[col] = pd.to_numeric(prices[col], errors="coerce")

    duplicate = prices[prices.duplicated("game_id", keep=False)]
    if not duplicate.empty:
        compare = [c for c in required if c != "game_id"]
        conflicts = []
        for game_id, group in duplicate.groupby("game_id"):
            if len(group[compare].drop_duplicates()) > 1:
                conflicts.append(game_id)
        if conflicts:
            raise RuntimeError(
                f"Conflicting total prices for game_id: {conflicts[:20]}"
            )
        prices = prices.drop_duplicates("game_id", keep="first")

    return prices


def build_dataset(
    history: pd.DataFrame,
    prices: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    joined = history.merge(
        prices,
        on="game_id",
        how="inner",
        validate="one_to_one",
    )

    stats = {
        "history_rows": len(history),
        "price_rows": len(prices),
        "joined_rows": len(joined),
        "missing_line_or_price": 0,
        "valid_rows": 0,
        "integer_total_rows": 0,
        "half_total_rows": 0,
        "realized_pushes": 0,
    }

    rows = []

    for record in joined.to_dict("records"):
        line = to_float(record["total"])
        over_decimal = resolve_decimal(
            record["dk_total_over_decimal"],
            record["dk_total_over_american"],
        )
        under_decimal = resolve_decimal(
            record["dk_total_under_decimal"],
            record["dk_total_under_american"],
        )

        if (
            not np.isfinite(line)
            or not np.isfinite(over_decimal)
            or not np.isfinite(under_decimal)
            or over_decimal <= 1.0
            or under_decimal <= 1.0
        ):
            stats["missing_line_or_price"] += 1
            continue

        actual_total = float(record["actual_total"])
        outcome = classify_total(actual_total, line)

        row = dict(record)
        row["total_line"] = line
        row["over_decimal"] = over_decimal
        row["under_decimal"] = under_decimal
        row["outcome_class"] = outcome
        row["actual_over"] = float(outcome == OVER_CLASS)
        row["actual_under"] = float(outcome == UNDER_CLASS)
        row["actual_push"] = float(outcome == PUSH_CLASS)
        rows.append(row)

        if is_integer_line(line):
            stats["integer_total_rows"] += 1
        else:
            stats["half_total_rows"] += 1
        if outcome == PUSH_CLASS:
            stats["realized_pushes"] += 1

    dataset = pd.DataFrame(rows)
    if dataset.empty:
        raise RuntimeError("No valid total rows after price join.")

    dataset = dataset.sort_values(
        ["game_date", "game_id"]
    ).reset_index(drop=True)

    stats["valid_rows"] = len(dataset)
    return dataset, stats


def determine_final_start(df: pd.DataFrame) -> pd.Timestamp:
    games = df[["game_id", "game_date"]].drop_duplicates("game_id")
    games = games.sort_values(["game_date", "game_id"]).reset_index(drop=True)

    if len(games) < 200:
        raise RuntimeError("Insufficient total games for final holdout.")

    index = int(
        math.floor(len(games) * (1.0 - FINAL_TEST_FRACTION))
    )
    index = min(max(index, 1), len(games) - 1)
    return pd.Timestamp(games.iloc[index]["game_date"])


def expanding_folds(dev: pd.DataFrame) -> list[dict[str, Any]]:
    dates = np.array(
        sorted(pd.Timestamp(x) for x in dev["game_date"].unique())
    )

    initial_end = int(
        math.floor(len(dates) * INITIAL_TRAIN_DATE_FRACTION)
    )
    initial_end = min(
        max(initial_end, 5),
        len(dates) - N_VALIDATION_FOLDS,
    )

    chunks = [
        chunk
        for chunk in np.array_split(
            dates[initial_end:],
            N_VALIDATION_FOLDS,
        )
        if len(chunk)
    ]

    folds = []
    for fold_number, chunk in enumerate(chunks, start=1):
        validation_start = pd.Timestamp(chunk[0])
        validation_end = pd.Timestamp(chunk[-1])

        train = dev[dev["game_date"] < validation_start].copy()
        validation = dev[
            (dev["game_date"] >= validation_start)
            & (dev["game_date"] <= validation_end)
        ].copy()

        if len(train) < MIN_TRAIN_ROWS:
            continue
        if len(validation) < MIN_VALIDATION_ROWS:
            continue

        folds.append(
            {
                "fold": fold_number,
                "train": train,
                "validation": validation,
            }
        )

    if len(folds) < 2:
        raise RuntimeError("Fewer than two usable expanding-window folds.")

    return folds


def exact_count_probabilities(
    means,
    lines,
    *,
    distribution: str,
    alpha: float | None = None,
) -> np.ndarray:
    means = np.asarray(means, dtype=float)
    lines = np.asarray(lines, dtype=float)
    out = np.full((len(means), 3), np.nan, dtype=float)

    for index, (mean, line) in enumerate(zip(means, lines)):
        if not np.isfinite(mean) or mean <= 0:
            continue
        if not np.isfinite(line) or line < 0:
            continue

        floor_line = math.floor(line)

        if distribution == "poisson":
            if is_integer_line(line):
                integer_line = int(round(line))
                p_under = poisson.cdf(integer_line - 1, mean)
                p_push = poisson.pmf(integer_line, mean)
                p_over = 1.0 - poisson.cdf(integer_line, mean)
            else:
                p_under = poisson.cdf(floor_line, mean)
                p_push = 0.0
                p_over = 1.0 - p_under

        elif distribution == "negative_binomial":
            if alpha is None or alpha <= 0:
                raise ValueError("negative binomial alpha must be > 0")
            size = 1.0 / alpha
            prob = size / (size + mean)

            if is_integer_line(line):
                integer_line = int(round(line))
                p_under = nbinom.cdf(
                    integer_line - 1,
                    size,
                    prob,
                )
                p_push = nbinom.pmf(
                    integer_line,
                    size,
                    prob,
                )
                p_over = 1.0 - nbinom.cdf(
                    integer_line,
                    size,
                    prob,
                )
            else:
                p_under = nbinom.cdf(floor_line, size, prob)
                p_push = 0.0
                p_over = 1.0 - p_under
        else:
            raise ValueError(distribution)

        probabilities = np.array(
            [p_under, p_push, p_over],
            dtype=float,
        )
        probabilities = np.clip(probabilities, 0.0, 1.0)
        total = probabilities.sum()
        if total > 0:
            probabilities /= total
            out[index] = probabilities

    return out


def fit_nb_alpha(predicted_mean, actual_total) -> float:
    mu = np.asarray(predicted_mean, dtype=float)
    y = np.asarray(actual_total, dtype=float)
    valid = (
        np.isfinite(mu)
        & np.isfinite(y)
        & (mu > 0)
        & (y >= 0)
    )
    mu = mu[valid]
    y = y[valid]

    if len(mu) < MIN_TRAIN_ROWS:
        raise RuntimeError("negative_binomial_insufficient_rows")

    def objective(log_alpha: float) -> float:
        alpha = math.exp(log_alpha)
        size = 1.0 / alpha
        prob = size / (size + mu)
        log_pmf = nbinom.logpmf(y, size, prob)
        if not np.isfinite(log_pmf).all():
            return float("inf")
        return float(-np.sum(log_pmf))

    result = minimize_scalar(
        objective,
        bounds=(-8.0, 3.0),
        method="bounded",
        options={"xatol": 1e-6},
    )
    if not result.success or not np.isfinite(result.fun):
        raise RuntimeError("negative_binomial_alpha_fit_failed")

    return float(math.exp(result.x))


def empirical_residual_probabilities(
    train_actual,
    train_predicted,
    target_predicted,
    target_lines,
) -> np.ndarray:
    train_actual = np.asarray(train_actual, dtype=float)
    train_predicted = np.asarray(train_predicted, dtype=float)
    residuals = train_actual - train_predicted
    residuals = residuals[np.isfinite(residuals)]

    if len(residuals) < MIN_TRAIN_ROWS:
        raise RuntimeError("empirical_residual_insufficient_rows")

    target_predicted = np.asarray(target_predicted, dtype=float)
    target_lines = np.asarray(target_lines, dtype=float)

    out = np.full((len(target_predicted), 3), np.nan)

    for index, (predicted, line) in enumerate(
        zip(target_predicted, target_lines)
    ):
        if not np.isfinite(predicted) or not np.isfinite(line):
            continue

        simulated_total = np.rint(
            np.maximum(0.0, predicted + residuals)
        )

        counts = np.array(
            [
                np.sum(simulated_total < line),
                np.sum(simulated_total == line),
                np.sum(simulated_total > line),
            ],
            dtype=float,
        )
        probabilities = (counts + 0.5) / (
            len(simulated_total) + 1.5
        )

        if not is_integer_line(line):
            probabilities[PUSH_CLASS] = 0.0
            decision_sum = (
                probabilities[UNDER_CLASS]
                + probabilities[OVER_CLASS]
            )
            probabilities[UNDER_CLASS] /= decision_sum
            probabilities[OVER_CLASS] /= decision_sum

        probabilities /= probabilities.sum()
        out[index] = probabilities

    return out


def simulation_probabilities(
    means,
    lines,
    *,
    seed: int,
) -> np.ndarray:
    means = np.asarray(means, dtype=float)
    lines = np.asarray(lines, dtype=float)
    out = np.full((len(means), 3), np.nan)
    rng = np.random.default_rng(seed)

    for index, (mean, line) in enumerate(zip(means, lines)):
        if (
            not np.isfinite(mean)
            or mean <= 0
            or not np.isfinite(line)
        ):
            continue

        draws = rng.poisson(mean, size=SIMULATIONS_PER_ROW)

        counts = np.array(
            [
                np.sum(draws < line),
                np.sum(draws == line),
                np.sum(draws > line),
            ],
            dtype=float,
        )
        probabilities = (counts + 0.5) / (
            SIMULATIONS_PER_ROW + 1.5
        )

        if not is_integer_line(line):
            probabilities[PUSH_CLASS] = 0.0
            decision_sum = (
                probabilities[UNDER_CLASS]
                + probabilities[OVER_CLASS]
            )
            probabilities[UNDER_CLASS] /= decision_sum
            probabilities[OVER_CLASS] /= decision_sum

        probabilities /= probabilities.sum()
        out[index] = probabilities

    return out


def direct_features(predicted_total, lines) -> np.ndarray:
    predicted_total = np.asarray(predicted_total, dtype=float)
    lines = np.asarray(lines, dtype=float)
    expected_delta = predicted_total - lines
    integer_flag = np.array(
        [
            1.0 if is_integer_line(line) else 0.0
            for line in lines
        ],
        dtype=float,
    )
    return np.column_stack(
        [
            expected_delta,
            predicted_total,
            lines,
            integer_flag,
        ]
    )


def fit_direct_multinomial(
    predicted_total,
    lines,
    outcome_class,
):
    x = direct_features(predicted_total, lines)
    y = np.asarray(outcome_class, dtype=int)

    valid = np.isfinite(x).all(axis=1)
    x = x[valid]
    y = y[valid]

    if len(x) < MIN_TRAIN_ROWS:
        raise RuntimeError("direct_multinomial_insufficient_rows")
    if len(np.unique(y)) < 3:
        raise RuntimeError(
            "direct_multinomial_requires_under_push_over_history"
        )

    model = Pipeline(
        [
            ("scale", StandardScaler()),
            (
                "logistic",
                LogisticRegression(
                    C=1.0,
                    solver="lbfgs",
                    max_iter=5000,
                    random_state=SEED,
                ),
            ),
        ]
    )
    model.fit(x, y)
    return model


def predict_direct_multinomial(
    model,
    predicted_total,
    lines,
) -> np.ndarray:
    x = direct_features(predicted_total, lines)
    valid = np.isfinite(x).all(axis=1)
    out = np.full((len(x), 3), np.nan)

    if valid.any():
        raw = model.predict_proba(x[valid])
        classes = model.named_steps["logistic"].classes_

        full = np.zeros((raw.shape[0], 3), dtype=float)
        for raw_index, class_value in enumerate(classes):
            full[:, int(class_value)] = raw[:, raw_index]

        valid_indices = np.flatnonzero(valid)
        for row_index, global_index in enumerate(valid_indices):
            line = float(np.asarray(lines)[global_index])
            probabilities = full[row_index].copy()

            if not is_integer_line(line):
                probabilities[PUSH_CLASS] = 0.0
                decision_sum = (
                    probabilities[UNDER_CLASS]
                    + probabilities[OVER_CLASS]
                )
                if decision_sum <= 0:
                    continue
                probabilities[UNDER_CLASS] /= decision_sum
                probabilities[OVER_CLASS] /= decision_sum

            probabilities = np.clip(probabilities, EPS, 1.0)
            probabilities /= probabilities.sum()
            out[global_index] = probabilities

    return out


def fit_method(
    method: str,
    *,
    train_predicted,
    train_actual,
    train_lines,
    train_outcome,
):
    if method in {"poisson", "poisson_simulation"}:
        return None

    if method == "negative_binomial":
        return {
            "alpha": fit_nb_alpha(
                train_predicted,
                train_actual,
            )
        }

    if method == "empirical_residual":
        return {
            "actual": np.asarray(train_actual, dtype=float),
            "predicted": np.asarray(train_predicted, dtype=float),
        }

    if method == "direct_multinomial":
        return fit_direct_multinomial(
            train_predicted,
            train_lines,
            train_outcome,
        )

    raise ValueError(method)


def predict_method(
    method: str,
    fitted,
    *,
    predicted,
    lines,
    seed: int,
) -> np.ndarray:
    if method == "poisson":
        return exact_count_probabilities(
            predicted,
            lines,
            distribution="poisson",
        )

    if method == "negative_binomial":
        return exact_count_probabilities(
            predicted,
            lines,
            distribution="negative_binomial",
            alpha=fitted["alpha"],
        )

    if method == "empirical_residual":
        return empirical_residual_probabilities(
            fitted["actual"],
            fitted["predicted"],
            predicted,
            lines,
        )

    if method == "poisson_simulation":
        return simulation_probabilities(
            predicted,
            lines,
            seed=seed,
        )

    if method == "direct_multinomial":
        return predict_direct_multinomial(
            fitted,
            predicted,
            lines,
        )

    raise ValueError(method)


def conditional_side_probability(
    win_probability: float,
    push_probability: float,
) -> float:
    no_push = 1.0 - push_probability
    if no_push <= EPS:
        return np.nan
    return win_probability / no_push


def probability_metrics(
    predictions: pd.DataFrame,
) -> dict[str, Any]:
    valid = predictions.dropna(
        subset=["p_under", "p_push", "p_over"]
    ).copy()

    if valid.empty:
        return {
            "game_count": 0,
            "multiclass_brier": np.nan,
            "multiclass_log_loss": np.nan,
            "decision_brier": np.nan,
            "decision_log_loss": np.nan,
            "push_brier": np.nan,
            "over_calibration_gap": np.nan,
            "under_calibration_gap": np.nan,
            "push_calibration_gap": np.nan,
        }

    p = valid[["p_under", "p_push", "p_over"]].to_numpy(float)
    y_class = valid["outcome_class"].to_numpy(int)
    y = np.eye(3)[y_class]

    multiclass_brier = float(
        np.mean(np.sum((p - y) ** 2, axis=1))
    )
    chosen = np.clip(
        p[np.arange(len(p)), y_class],
        EPS,
        1.0,
    )
    multiclass_log_loss = float(-np.mean(np.log(chosen)))

    nonpush = valid[
        valid["outcome_class"] != PUSH_CLASS
    ].copy()

    if nonpush.empty:
        decision_brier = np.nan
        decision_log_loss = np.nan
    else:
        actual_over = (
            nonpush["outcome_class"] == OVER_CLASS
        ).astype(float).to_numpy()
        conditional_over = (
            nonpush["p_over"]
            / (nonpush["p_over"] + nonpush["p_under"])
        ).to_numpy(float)
        conditional_over = np.clip(
            conditional_over,
            EPS,
            1.0 - EPS,
        )
        decision_brier = float(
            np.mean((conditional_over - actual_over) ** 2)
        )
        decision_log_loss = float(
            -np.mean(
                actual_over * np.log(conditional_over)
                + (1.0 - actual_over)
                * np.log(1.0 - conditional_over)
            )
        )

    actual_push = (
        valid["outcome_class"] == PUSH_CLASS
    ).astype(float).to_numpy()
    push_probability = valid["p_push"].to_numpy(float)

    return {
        "game_count": len(valid),
        "multiclass_brier": multiclass_brier,
        "multiclass_log_loss": multiclass_log_loss,
        "decision_brier": decision_brier,
        "decision_log_loss": decision_log_loss,
        "push_brier": float(
            np.mean((push_probability - actual_push) ** 2)
        ),
        "over_calibration_gap": float(
            valid["actual_over"].mean()
            - valid["p_over"].mean()
        ),
        "under_calibration_gap": float(
            valid["actual_under"].mean()
            - valid["p_under"].mean()
        ),
        "push_calibration_gap": float(
            valid["actual_push"].mean()
            - valid["p_push"].mean()
        ),
    }


def side_economics(
    target: pd.DataFrame,
    probabilities: np.ndarray,
    *,
    source: str,
    method: str,
    period: str,
    fold: int | None,
) -> pd.DataFrame:
    rows = []

    for record, probs in zip(
        target.to_dict("records"),
        probabilities,
    ):
        if not np.isfinite(probs).all():
            continue

        p_under = float(probs[UNDER_CLASS])
        p_push = float(probs[PUSH_CLASS])
        p_over = float(probs[OVER_CLASS])

        for (
            side,
            win_probability,
            loss_probability,
            decimal_odds,
            actual_win,
            actual_loss,
        ) in [
            (
                "over",
                p_over,
                p_under,
                float(record["over_decimal"]),
                float(record["actual_over"]),
                float(record["actual_under"]),
            ),
            (
                "under",
                p_under,
                p_over,
                float(record["under_decimal"]),
                float(record["actual_under"]),
                float(record["actual_over"]),
            ),
        ]:
            conditional_probability = conditional_side_probability(
                win_probability,
                p_push,
            )
            break_even = 1.0 / decimal_odds
            predicted_ev = (
                win_probability * (decimal_odds - 1.0)
                - loss_probability
            )

            if actual_win == 1.0:
                realized_profit = decimal_odds - 1.0
            elif actual_loss == 1.0:
                realized_profit = -1.0
            else:
                realized_profit = 0.0

            rows.append(
                {
                    "source": source,
                    "method": method,
                    "period": period,
                    "fold": fold,
                    "game_id": record["game_id"],
                    "game_date": record["game_date"],
                    "side": side,
                    "total_line": float(record["total_line"]),
                    "p_win": win_probability,
                    "p_loss": loss_probability,
                    "p_push": p_push,
                    "conditional_side_probability": conditional_probability,
                    "break_even_probability": break_even,
                    "positive_economic_support": (
                        np.isfinite(conditional_probability)
                        and conditional_probability > break_even
                    ),
                    "predicted_ev": predicted_ev,
                    "decimal_odds": decimal_odds,
                    "actual_win": actual_win,
                    "actual_loss": actual_loss,
                    "actual_push": float(record["actual_push"]),
                    "realized_profit": realized_profit,
                }
            )

    return pd.DataFrame(rows)


def betting_metrics(side_df: pd.DataFrame) -> dict[str, Any]:
    selected = side_df[
        side_df["positive_economic_support"]
    ].copy()

    if selected.empty:
        return {
            "positive_ev_bets": 0,
            "positive_ev_wins": 0,
            "positive_ev_losses": 0,
            "positive_ev_pushes": 0,
            "positive_ev_win_rate_decisions": np.nan,
            "positive_ev_profit_units": 0.0,
            "positive_ev_roi": np.nan,
            "mean_predicted_ev": np.nan,
            "ev_bias": np.nan,
            "ev_mae": np.nan,
        }

    wins = int((selected["actual_win"] == 1.0).sum())
    losses = int((selected["actual_loss"] == 1.0).sum())
    pushes = int((selected["actual_push"] == 1.0).sum())
    decisions = wins + losses

    predicted_ev = selected["predicted_ev"].to_numpy(float)
    realized = selected["realized_profit"].to_numpy(float)

    return {
        "positive_ev_bets": len(selected),
        "positive_ev_wins": wins,
        "positive_ev_losses": losses,
        "positive_ev_pushes": pushes,
        "positive_ev_win_rate_decisions": (
            wins / decisions if decisions else np.nan
        ),
        "positive_ev_profit_units": float(realized.sum()),
        "positive_ev_roi": float(realized.mean()),
        "mean_predicted_ev": float(predicted_ev.mean()),
        "ev_bias": float(np.mean(predicted_ev - realized)),
        "ev_mae": float(np.mean(np.abs(predicted_ev - realized))),
    }


def current_point_rule_rows(
    target: pd.DataFrame,
    predicted_total,
    *,
    source: str,
    period: str,
    fold: int | None,
) -> pd.DataFrame:
    predicted_total = np.asarray(predicted_total, dtype=float)
    rows = []

    for record, prediction in zip(
        target.to_dict("records"),
        predicted_total,
    ):
        line = float(record["total_line"])
        if not np.isfinite(prediction):
            continue
        if abs(prediction - line) < EPS:
            continue

        if prediction > line:
            side = "over"
            decimal_odds = float(record["over_decimal"])
            actual_win = float(record["actual_over"])
            actual_loss = float(record["actual_under"])
        else:
            side = "under"
            decimal_odds = float(record["under_decimal"])
            actual_win = float(record["actual_under"])
            actual_loss = float(record["actual_over"])

        if actual_win == 1.0:
            realized_profit = decimal_odds - 1.0
        elif actual_loss == 1.0:
            realized_profit = -1.0
        else:
            realized_profit = 0.0

        rows.append(
            {
                "source": source,
                "period": period,
                "fold": fold,
                "game_id": record["game_id"],
                "game_date": record["game_date"],
                "side": side,
                "predicted_total": prediction,
                "total_line": line,
                "actual_win": actual_win,
                "actual_loss": actual_loss,
                "actual_push": float(record["actual_push"]),
                "decimal_odds": decimal_odds,
                "realized_profit": realized_profit,
            }
        )

    return pd.DataFrame(rows)


def current_rule_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "current_rule_bets": 0,
            "current_rule_wins": 0,
            "current_rule_losses": 0,
            "current_rule_pushes": 0,
            "current_rule_win_rate_decisions": np.nan,
            "current_rule_profit_units": 0.0,
            "current_rule_roi": np.nan,
        }

    wins = int((rows["actual_win"] == 1.0).sum())
    losses = int((rows["actual_loss"] == 1.0).sum())
    pushes = int((rows["actual_push"] == 1.0).sum())
    decisions = wins + losses

    return {
        "current_rule_bets": len(rows),
        "current_rule_wins": wins,
        "current_rule_losses": losses,
        "current_rule_pushes": pushes,
        "current_rule_win_rate_decisions": (
            wins / decisions if decisions else np.nan
        ),
        "current_rule_profit_units": float(
            rows["realized_profit"].sum()
        ),
        "current_rule_roi": float(
            rows["realized_profit"].mean()
        ),
    }


def reliability_rows(
    predictions: pd.DataFrame,
    *,
    probability_column: str,
    actual_column: str,
    label: str,
) -> pd.DataFrame:
    frame = predictions[
        [
            "source",
            "method",
            "period",
            "fold",
            probability_column,
            actual_column,
        ]
    ].dropna().copy()

    if frame.empty:
        return pd.DataFrame()

    frame["bucket"] = pd.cut(
        frame[probability_column],
        bins=np.linspace(0.0, 1.0, 11),
        include_lowest=True,
        right=True,
    ).astype(str)

    rows = []
    for keys, group in frame.groupby(
        [
            "source",
            "method",
            "period",
            "fold",
            "bucket",
        ],
        dropna=False,
    ):
        rows.append(
            {
                "source": keys[0],
                "method": keys[1],
                "period": keys[2],
                "fold": keys[3],
                "probability_type": label,
                "bucket": keys[4],
                "sample_size": len(group),
                "mean_probability": float(
                    group[probability_column].mean()
                ),
                "realized_rate": float(
                    group[actual_column].mean()
                ),
                "calibration_gap": float(
                    group[actual_column].mean()
                    - group[probability_column].mean()
                ),
            }
        )

    return pd.DataFrame(rows)


def evaluate_period(
    train: pd.DataFrame,
    target: pd.DataFrame,
    *,
    period: str,
    fold: int | None,
    seed_base: int,
):
    bundle = fit_derived_bundle(train)

    metric_rows = []
    prediction_parts = []
    side_parts = []
    current_parts = []

    for source_index, source in enumerate(SOURCES):
        train_predicted = source_prediction(
            source,
            train,
            bundle,
        )
        target_predicted = source_prediction(
            source,
            target,
            bundle,
        )

        current = current_point_rule_rows(
            target,
            target_predicted,
            source=source,
            period=period,
            fold=fold,
        )
        current_parts.append(current)

        for method_index, method in enumerate(METHODS):
            status = "ok"
            failure_reason = ""
            fitted_details = ""

            try:
                fitted = fit_method(
                    method,
                    train_predicted=train_predicted,
                    train_actual=train[
                        "actual_total"
                    ].to_numpy(float),
                    train_lines=train[
                        "total_line"
                    ].to_numpy(float),
                    train_outcome=train[
                        "outcome_class"
                    ].to_numpy(int),
                )

                if method == "negative_binomial":
                    fitted_details = (
                        f"alpha={fitted['alpha']:.12g}"
                    )

                probabilities = predict_method(
                    method,
                    fitted,
                    predicted=target_predicted,
                    lines=target[
                        "total_line"
                    ].to_numpy(float),
                    seed=(
                        seed_base
                        + source_index * 100
                        + method_index
                    ),
                )
            except Exception as exc:
                probabilities = np.full(
                    (len(target), 3),
                    np.nan,
                )
                status = "ineligible"
                failure_reason = str(exc)

            prediction = target[
                [
                    "game_id",
                    "game_date",
                    "total_line",
                    "actual_total",
                    "outcome_class",
                    "actual_under",
                    "actual_push",
                    "actual_over",
                    "over_decimal",
                    "under_decimal",
                ]
            ].copy()
            prediction["source"] = source
            prediction["method"] = method
            prediction["period"] = period
            prediction["fold"] = fold
            prediction["predicted_total"] = target_predicted
            prediction["p_under"] = probabilities[:, UNDER_CLASS]
            prediction["p_push"] = probabilities[:, PUSH_CLASS]
            prediction["p_over"] = probabilities[:, OVER_CLASS]
            prediction_parts.append(prediction)

            sides = side_economics(
                target,
                probabilities,
                source=source,
                method=method,
                period=period,
                fold=fold,
            )
            side_parts.append(sides)

            pm = probability_metrics(prediction)
            bm = betting_metrics(sides)
            cm = current_rule_metrics(current)

            metric_rows.append(
                {
                    "period": period,
                    "fold": fold,
                    "source": source,
                    "method": method,
                    "status": status,
                    "failure_reason": failure_reason,
                    "fitted_details": fitted_details,
                    "train_rows": len(train),
                    "target_rows": len(target),
                    "train_start": train["game_date"].min(),
                    "train_end": train["game_date"].max(),
                    "target_start": target["game_date"].min(),
                    "target_end": target["game_date"].max(),
                    "weighted_total_drat_weight": bundle.total_weight,
                    **pm,
                    **bm,
                    **cm,
                }
            )

    return (
        pd.DataFrame(metric_rows),
        pd.concat(prediction_parts, ignore_index=True),
        pd.concat(side_parts, ignore_index=True),
        pd.concat(current_parts, ignore_index=True),
    )


def aggregate_metrics(
    predictions: pd.DataFrame,
    side_rows: pd.DataFrame,
    current_rows: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    for source in SOURCES:
        current = current_rows[
            current_rows["source"] == source
        ].copy()
        cm = current_rule_metrics(current)

        for method in METHODS:
            pred = predictions[
                (predictions["source"] == source)
                & (predictions["method"] == method)
            ].copy()
            sides = side_rows[
                (side_rows["source"] == source)
                & (side_rows["method"] == method)
            ].copy()

            rows.append(
                {
                    "source": source,
                    "method": method,
                    **probability_metrics(pred),
                    **betting_metrics(sides),
                    **cm,
                }
            )

    return pd.DataFrame(rows)


def serialize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in out.columns:
        name = str(column).lower()
        if (
            "date" in name
            or name.endswith("_start")
            or name.endswith("_end")
        ):
            out[column] = out[column].map(
                lambda value: (
                    value.strftime("%Y-%m-%d")
                    if isinstance(
                        value,
                        (pd.Timestamp, datetime),
                    )
                    else value
                )
            )
    return out


def main() -> None:
    root = find_repo_root()
    output_dir = root / OUTPUT_REL
    output_dir.mkdir(parents=True, exist_ok=True)

    history = load_history(root)
    prices = load_archived_total_prices(root)
    dataset, coverage_stats = build_dataset(
        history,
        prices,
    )

    final_start = determine_final_start(dataset)
    development = dataset[
        dataset["game_date"] < final_start
    ].copy()
    final_test = dataset[
        dataset["game_date"] >= final_start
    ].copy()

    folds = expanding_folds(development)

    fold_metric_parts = []
    validation_prediction_parts = []
    validation_side_parts = []
    validation_current_parts = []

    for fold in folds:
        (
            metrics,
            predictions,
            sides,
            current,
        ) = evaluate_period(
            fold["train"],
            fold["validation"],
            period="development_validation",
            fold=fold["fold"],
            seed_base=SEED + fold["fold"] * 1000,
        )
        fold_metric_parts.append(metrics)
        validation_prediction_parts.append(predictions)
        validation_side_parts.append(sides)
        validation_current_parts.append(current)

    fold_metrics = pd.concat(
        fold_metric_parts,
        ignore_index=True,
    )
    validation_predictions = pd.concat(
        validation_prediction_parts,
        ignore_index=True,
    )
    validation_sides = pd.concat(
        validation_side_parts,
        ignore_index=True,
    )
    validation_current = pd.concat(
        validation_current_parts,
        ignore_index=True,
    )

    development_metrics = aggregate_metrics(
        validation_predictions,
        validation_sides,
        validation_current,
    )
    development_ranking = (
        development_metrics
        .sort_values(
            [
                "multiclass_log_loss",
                "decision_log_loss",
                "multiclass_brier",
                "source",
                "method",
            ]
        )
        .reset_index(drop=True)
    )
    development_ranking["development_rank"] = (
        np.arange(len(development_ranking)) + 1
    )

    (
        final_metrics,
        final_predictions,
        final_sides,
        final_current,
    ) = evaluate_period(
        development,
        final_test,
        period="final_test",
        fold=None,
        seed_base=SEED + 900000,
    )

    final_ranking = (
        final_metrics
        .sort_values(
            [
                "multiclass_log_loss",
                "decision_log_loss",
                "multiclass_brier",
                "source",
                "method",
            ]
        )
        .reset_index(drop=True)
    )

    combined_predictions = pd.concat(
        [validation_predictions, final_predictions],
        ignore_index=True,
    )

    reliability_parts = [
        reliability_rows(
            combined_predictions,
            probability_column="p_over",
            actual_column="actual_over",
            label="unconditional_over",
        ),
        reliability_rows(
            combined_predictions,
            probability_column="p_under",
            actual_column="actual_under",
            label="unconditional_under",
        ),
        reliability_rows(
            combined_predictions,
            probability_column="p_push",
            actual_column="actual_push",
            label="push",
        ),
    ]

    nonpush = combined_predictions[
        combined_predictions["outcome_class"] != PUSH_CLASS
    ].copy()
    nonpush["conditional_over_probability"] = (
        nonpush["p_over"]
        / (nonpush["p_over"] + nonpush["p_under"])
    )
    nonpush["actual_over_decision"] = nonpush["actual_over"]
    reliability_parts.append(
        reliability_rows(
            nonpush,
            probability_column="conditional_over_probability",
            actual_column="actual_over_decision",
            label="conditional_over_no_push",
        )
    )

    reliability = pd.concat(
        [
            part
            for part in reliability_parts
            if not part.empty
        ],
        ignore_index=True,
    )

    support_change_rows = []
    for source in SOURCES:
        current_keys = set(
            zip(
                final_current.loc[
                    final_current["source"] == source,
                    "game_id",
                ].astype(str),
                final_current.loc[
                    final_current["source"] == source,
                    "side",
                ].astype(str),
            )
        )

        for method in METHODS:
            selected = final_sides[
                (final_sides["source"] == source)
                & (final_sides["method"] == method)
                & (final_sides["positive_economic_support"])
            ].copy()

            new_keys = set(
                zip(
                    selected["game_id"].astype(str),
                    selected["side"].astype(str),
                )
            )

            support_change_rows.append(
                {
                    "source": source,
                    "method": method,
                    "current_point_rule_supports": len(current_keys),
                    "probability_price_supports": len(new_keys),
                    "support_overlap": len(current_keys & new_keys),
                    "current_only": len(current_keys - new_keys),
                    "probability_only": len(new_keys - current_keys),
                }
            )

    support_changes = pd.DataFrame(support_change_rows)

    split_manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "research_only": True,
        "production_files_modified": False,
        "production_promotion_performed": False,
        "seed": SEED,
        "simulations_per_row": SIMULATIONS_PER_ROW,
        "requested_final_test_fraction": FINAL_TEST_FRACTION,
        "development_start": (
            development["game_date"].min().strftime("%Y-%m-%d")
        ),
        "development_end": (
            development["game_date"].max().strftime("%Y-%m-%d")
        ),
        "final_test_start": (
            final_test["game_date"].min().strftime("%Y-%m-%d")
        ),
        "final_test_end": (
            final_test["game_date"].max().strftime("%Y-%m-%d")
        ),
        "development_games": len(development),
        "final_test_games": len(final_test),
        "validation_folds": len(folds),
        "coverage": coverage_stats,
    }

    package_versions = {
        "python": platform.python_version(),
        "numpy": package_version("numpy"),
        "pandas": package_version("pandas"),
        "scipy": package_version("scipy"),
        "scikit-learn": package_version("scikit-learn"),
    }

    run_manifest = {
        **split_manifest,
        "package_versions": package_versions,
        "sources": list(SOURCES),
        "methods": list(METHODS),
        "current_production_rule": (
            "expected_total > line -> over; "
            "expected_total < line -> under"
        ),
        "price_support_rule": (
            "conditional no-push side probability "
            "> 1 / offered_decimal_odds"
        ),
        "integer_total_ev_rule": (
            "EV = P(win)*(decimal-1) - P(loss); "
            "push contributes 0"
        ),
        "poisson": "exact discrete count probabilities",
        "negative_binomial": (
            "exact discrete count probabilities with chronologically "
            "fitted global dispersion per source/fold"
        ),
        "empirical_residual": (
            "resample prior actual_total - predicted_total residuals; "
            "round simulated totals to nonnegative integers"
        ),
        "poisson_simulation": (
            f"{SIMULATIONS_PER_ROW} Monte Carlo draws per row"
        ),
        "direct_multinomial_features": [
            "predicted_total - sportsbook_line",
            "predicted_total",
            "sportsbook_line",
            "integer_line_flag",
        ],
        "direct_multinomial": {
            "family": "LogisticRegression",
            "C": 1.0,
            "solver": "lbfgs",
            "max_iter": 5000,
            "standardization": True,
            "random_state": SEED,
        },
        "selection_or_promotion_performed": False,
    }

    (output_dir / "split_manifest.json").write_text(
        json.dumps(split_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    serialize_dates(dataset).to_csv(
        output_dir / "research_dataset.csv",
        index=False,
    )
    serialize_dates(fold_metrics).to_csv(
        output_dir / "fold_metrics.csv",
        index=False,
    )
    development_metrics.to_csv(
        output_dir / "development_metrics.csv",
        index=False,
    )
    development_ranking.to_csv(
        output_dir / "development_ranking.csv",
        index=False,
    )
    serialize_dates(validation_predictions).to_csv(
        output_dir / "validation_total_probabilities.csv",
        index=False,
    )
    serialize_dates(validation_sides).to_csv(
        output_dir / "validation_side_economics.csv",
        index=False,
    )
    serialize_dates(validation_current).to_csv(
        output_dir / "validation_current_point_support.csv",
        index=False,
    )
    serialize_dates(final_metrics).to_csv(
        output_dir / "final_test_metrics.csv",
        index=False,
    )
    serialize_dates(final_predictions).to_csv(
        output_dir / "final_test_total_probabilities.csv",
        index=False,
    )
    serialize_dates(final_sides).to_csv(
        output_dir / "final_test_side_economics.csv",
        index=False,
    )
    serialize_dates(final_current).to_csv(
        output_dir / "final_test_current_point_support.csv",
        index=False,
    )
    serialize_dates(reliability).to_csv(
        output_dir / "reliability_buckets.csv",
        index=False,
    )
    support_changes.to_csv(
        output_dir / "support_decision_changes.csv",
        index=False,
    )

    best_dev = development_ranking.iloc[0]
    best_final = final_ranking.iloc[0]

    summary = [
        "NHL TOTAL SECONDARY PROBABILITY RESEARCH",
        "========================================",
        "research_only=true",
        "production_files_modified=false",
        "production_promotion_performed=false",
        (
            "current_rule=expected_total > line -> over; "
            "expected_total < line -> under"
        ),
        (
            "research_rule=explicit P(Over)/P(Under)/P(Push); "
            "conditional no-push side probability "
            "> 1 / offered_decimal_odds"
        ),
        f"valid_total_games={len(dataset)}",
        (
            f"integer_total_games="
            f"{coverage_stats['integer_total_rows']}"
        ),
        (
            f"realized_pushes="
            f"{coverage_stats['realized_pushes']}"
        ),
        f"development_games={len(development)}",
        f"final_test_games={len(final_test)}",
        f"final_test_start={split_manifest['final_test_start']}",
        f"validation_folds={len(folds)}",
        "sources=" + ",".join(SOURCES),
        "methods=" + ",".join(METHODS),
        "",
        "BEST DEVELOPMENT COMBINATION (selection evidence only)",
        (
            f"source={best_dev['source']} | "
            f"method={best_dev['method']} | "
            f"multiclass_Brier={best_dev['multiclass_brier']:.6f} | "
            f"multiclass_log_loss={best_dev['multiclass_log_loss']:.6f} | "
            f"decision_Brier={best_dev['decision_brier']:.6f} | "
            f"decision_log_loss={best_dev['decision_log_loss']:.6f} | "
            f"positive_EV_ROI={best_dev['positive_ev_roi']:.6f}"
        ),
        "",
        "BEST FINAL-TEST COMBINATION (evaluation only; not used for selection)",
        (
            f"source={best_final['source']} | "
            f"method={best_final['method']} | "
            f"multiclass_Brier={best_final['multiclass_brier']:.6f} | "
            f"multiclass_log_loss={best_final['multiclass_log_loss']:.6f} | "
            f"decision_Brier={best_final['decision_brier']:.6f} | "
            f"decision_log_loss={best_final['decision_log_loss']:.6f} | "
            f"positive_EV_bets={int(best_final['positive_ev_bets'])} | "
            f"positive_EV_ROI={best_final['positive_ev_roi']:.6f}"
        ),
        "",
        "FINAL-TEST COMPARISON",
    ]

    for row in final_ranking.to_dict("records"):
        summary.append(
            f"{row['source']} / {row['method']}: "
            f"multi_Brier={row['multiclass_brier']:.6f} | "
            f"multi_log={row['multiclass_log_loss']:.6f} | "
            f"decision_Brier={row['decision_brier']:.6f} | "
            f"decision_log={row['decision_log_loss']:.6f} | "
            f"push_Brier={row['push_brier']:.6f} | "
            f"positive_EV_bets={int(row['positive_ev_bets'])} | "
            f"positive_EV_ROI={row['positive_ev_roi']:.6f} | "
            f"current_rule_ROI={row['current_rule_roi']:.6f}"
        )

    summary += [
        "",
        "Artifacts:",
        str(output_dir / "split_manifest.json"),
        str(output_dir / "run_manifest.json"),
        str(output_dir / "research_dataset.csv"),
        str(output_dir / "fold_metrics.csv"),
        str(output_dir / "development_metrics.csv"),
        str(output_dir / "development_ranking.csv"),
        str(output_dir / "validation_total_probabilities.csv"),
        str(output_dir / "validation_side_economics.csv"),
        str(output_dir / "final_test_metrics.csv"),
        str(output_dir / "final_test_total_probabilities.csv"),
        str(output_dir / "final_test_side_economics.csv"),
        str(output_dir / "reliability_buckets.csv"),
        str(output_dir / "support_decision_changes.csv"),
    ]

    (output_dir / "summary.txt").write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8",
    )

    print("TOTAL SECONDARY PROBABILITY RESEARCH COMPLETE")
    print(f"Output: {output_dir}")
    print(f"Valid total games: {len(dataset)}")
    print(
        "Integer totals: "
        f"{coverage_stats['integer_total_rows']} | "
        "realized pushes: "
        f"{coverage_stats['realized_pushes']}"
    )
    print(
        "Final test starts: "
        f"{split_manifest['final_test_start']}"
    )
    print()
    print(
        final_ranking[
            [
                "source",
                "method",
                "game_count",
                "multiclass_brier",
                "multiclass_log_loss",
                "decision_brier",
                "decision_log_loss",
                "push_brier",
                "positive_ev_bets",
                "positive_ev_win_rate_decisions",
                "positive_ev_roi",
                "current_rule_roi",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
