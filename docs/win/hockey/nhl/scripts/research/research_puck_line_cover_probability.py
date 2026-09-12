#!/usr/bin/env python3
"""
Research-only NHL puck-line secondary cover-probability comparison.

Current production Stage 04 puck support is based on expected margin:
    home: expected_margin + home_line > 0
    away: -expected_margin + away_line > 0

That is a directional point-estimate rule, not a betting-value test.

This research script produces explicit P(cover) for secondary sources:
    - SDV challenger
    - weighted D-Ratings/SDV derived model
    - meta D-Ratings/SDV derived model

Candidate P(cover) methods:
    1. Skellam from expected margin + expected total
    2. Empirical residual margin distribution (ECDF)
    3. Direct binary cover logistic classifier
    4. Independent-Poisson Monte Carlo simulation

Each probability is compared with:
    break_even_probability = 1 / offered_decimal_odds

Research outputs only:
    docs/win/hockey/nhl/research/puck_line_secondary_cover_probability/

Production Stage 03/04 files are not modified.
"""

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
from scipy.stats import skellam
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


warnings.filterwarnings("ignore", category=PerformanceWarning)

NHL_REL = Path("docs/win/hockey/nhl")
HISTORY_REL = NHL_REL / "research/sdv_challenger"
MERGED_REL = NHL_REL / "archive/2025_26/01_merge"
OUTPUT_REL = NHL_REL / "research/puck_line_secondary_cover_probability"

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
    "skellam",
    "empirical_margin_ecdf",
    "direct_cover_logistic",
    "poisson_simulation",
)


@dataclass(frozen=True)
class LinearModel:
    coefficients: np.ndarray


@dataclass(frozen=True)
class DerivedBundle:
    margin_weight: float
    total_weight: float
    margin_meta: LinearModel
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


def log_loss(y, p):
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    return float(
        -np.mean(
            y * np.log(p)
            + (1.0 - y) * np.log(1.0 - p)
        )
    )


def brier_score(y, p):
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    return float(np.mean((p - y) ** 2))


def rmse(y, p):
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    return float(np.sqrt(np.mean((p - y) ** 2)))


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
    margin_weight = select_numeric_weight(
        train["actual_margin"],
        train["drat_exp_margin"],
        train["sdv_exp_margin"],
    )
    total_weight = select_numeric_weight(
        train["actual_total"],
        train["drat_exp_total"],
        train["sdv_exp_total"],
    )

    margin_disagreement = (
        train["sdv_exp_margin"] - train["drat_exp_margin"]
    ).abs()
    total_disagreement = (
        train["sdv_exp_total"] - train["drat_exp_total"]
    ).abs()

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

    return DerivedBundle(
        margin_weight=margin_weight,
        total_weight=total_weight,
        margin_meta=margin_meta,
        total_meta=total_meta,
    )


def source_predictions(
    source: str,
    df: pd.DataFrame,
    bundle: DerivedBundle,
) -> tuple[np.ndarray, np.ndarray]:
    if source == "sdv":
        return (
            df["sdv_exp_margin"].to_numpy(float),
            df["sdv_exp_total"].to_numpy(float),
        )

    if source == "weighted":
        margin = (
            bundle.margin_weight
            * df["drat_exp_margin"].to_numpy(float)
            + (1.0 - bundle.margin_weight)
            * df["sdv_exp_margin"].to_numpy(float)
        )
        total = (
            bundle.total_weight
            * df["drat_exp_total"].to_numpy(float)
            + (1.0 - bundle.total_weight)
            * df["sdv_exp_total"].to_numpy(float)
        )
        return margin, total

    if source == "meta":
        margin_disagreement = (
            df["sdv_exp_margin"] - df["drat_exp_margin"]
        ).abs().to_numpy(float)
        total_disagreement = (
            df["sdv_exp_total"] - df["drat_exp_total"]
        ).abs().to_numpy(float)

        margin = apply_linear(
            bundle.margin_meta,
            np.column_stack(
                [
                    df["drat_exp_margin"].to_numpy(float),
                    df["sdv_exp_margin"].to_numpy(float),
                    margin_disagreement,
                ]
            ),
        )
        total = apply_linear(
            bundle.total_meta,
            np.column_stack(
                [
                    df["drat_exp_total"].to_numpy(float),
                    df["sdv_exp_total"].to_numpy(float),
                    total_disagreement,
                ]
            ),
        )
        return margin, total

    raise ValueError(source)


def load_history(root: Path) -> pd.DataFrame:
    files = sorted(
        (root / HISTORY_REL).glob(
            "season_*/standalone_comparison.csv"
        )
    )
    if not files:
        raise RuntimeError(
            f"No standalone secondary-model history under {root / HISTORY_REL}"
        )

    required = [
        "game_id",
        "game_date",
        "drat_exp_margin",
        "drat_exp_total",
        "sdv_exp_margin",
        "sdv_exp_total",
        "actual_margin",
        "actual_total",
    ]
    parts = []

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"{path} missing history columns: {missing}"
            )
        part = df[required].copy()
        part["history_source_file"] = str(
            path.relative_to(root)
        )
        parts.append(part)

    history = pd.concat(parts, ignore_index=True)
    history["game_id"] = history["game_id"].map(
        canonical_game_id
    )
    history["game_date"] = history["game_date"].map(
        parse_date
    )

    numeric = [
        "drat_exp_margin",
        "drat_exp_total",
        "sdv_exp_margin",
        "sdv_exp_total",
        "actual_margin",
        "actual_total",
    ]
    for col in numeric:
        history[col] = pd.to_numeric(
            history[col],
            errors="coerce",
        )

    history = history.dropna(
        subset=["game_date", "game_id", *numeric]
    ).copy()
    history = history[
        history["game_id"].ne("")
    ].copy()
    history = history.sort_values(
        ["game_date", "game_id", "history_source_file"]
    )
    history = history.drop_duplicates(
        "game_id",
        keep="last",
    )
    return history


def load_archived_puck_prices(root: Path) -> pd.DataFrame:
    files = sorted(
        (root / MERGED_REL).glob("*_NHL_merged.csv")
    )
    if not files:
        raise RuntimeError(
            f"No archived merged files under {root / MERGED_REL}"
        )

    required = [
        "game_id",
        "home_puck_line",
        "away_puck_line",
        "home_dk_puck_line_american",
        "away_dk_puck_line_american",
        "home_dk_puck_line_decimal",
        "away_dk_puck_line_decimal",
    ]
    parts = []

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"{path} missing puck columns: {missing}"
            )
        part = df[required].copy()
        part["price_source_file"] = path.name
        parts.append(part)

    prices = pd.concat(parts, ignore_index=True)
    prices["game_id"] = prices["game_id"].map(
        canonical_game_id
    )

    numeric = [
        "home_puck_line",
        "away_puck_line",
        "home_dk_puck_line_american",
        "away_dk_puck_line_american",
        "home_dk_puck_line_decimal",
        "away_dk_puck_line_decimal",
    ]
    for col in numeric:
        prices[col] = pd.to_numeric(
            prices[col],
            errors="coerce",
        )

    duplicate = prices[
        prices.duplicated("game_id", keep=False)
    ]
    if not duplicate.empty:
        compare = [c for c in required if c != "game_id"]
        conflicting = []
        for game_id, group in duplicate.groupby("game_id"):
            if len(group[compare].drop_duplicates()) > 1:
                conflicting.append(game_id)
        if conflicting:
            raise RuntimeError(
                "Conflicting archived puck prices for game_id: "
                f"{conflicting[:20]}"
            )
        prices = prices.drop_duplicates(
            "game_id",
            keep="first",
        )

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
        "non_opposing_lines": 0,
        "integer_line_rows": 0,
        "valid_rows": 0,
    }

    rows = []

    for record in joined.to_dict("records"):
        home_line = to_float(record["home_puck_line"])
        away_line = to_float(record["away_puck_line"])
        home_american = to_float(
            record["home_dk_puck_line_american"]
        )
        away_american = to_float(
            record["away_dk_puck_line_american"]
        )
        home_decimal = resolve_decimal(
            record["home_dk_puck_line_decimal"],
            home_american,
        )
        away_decimal = resolve_decimal(
            record["away_dk_puck_line_decimal"],
            away_american,
        )

        if not all(
            np.isfinite(v)
            for v in [
                home_line,
                away_line,
                home_decimal,
                away_decimal,
            ]
        ) or home_decimal <= 1 or away_decimal <= 1:
            stats["missing_line_or_price"] += 1
            continue

        if abs(home_line + away_line) > 1e-9:
            stats["non_opposing_lines"] += 1
            continue

        # Integer puck lines can push. This item researches explicit binary
        # cover probability against 1/decimal, so keep only no-push half-lines.
        if (
            float(home_line).is_integer()
            or float(away_line).is_integer()
        ):
            stats["integer_line_rows"] += 1
            continue

        actual_margin = float(record["actual_margin"])
        home_cover_margin = actual_margin + home_line

        if abs(home_cover_margin) < 1e-12:
            # Defensive: half-point lines should make this impossible.
            stats["integer_line_rows"] += 1
            continue

        row = dict(record)
        row["home_decimal"] = home_decimal
        row["away_decimal"] = away_decimal
        row["home_american"] = home_american
        row["away_american"] = away_american
        row["home_cover"] = float(
            home_cover_margin > 0
        )
        row["away_cover"] = (
            1.0 - row["home_cover"]
        )
        rows.append(row)

    dataset = pd.DataFrame(rows)
    if dataset.empty:
        raise RuntimeError(
            "No valid no-push opposing puck-line rows."
        )

    dataset = dataset.sort_values(
        ["game_date", "game_id"]
    ).reset_index(drop=True)

    stats["valid_rows"] = len(dataset)
    return dataset, stats


def determine_final_start(
    df: pd.DataFrame,
) -> pd.Timestamp:
    games = df[
        ["game_id", "game_date"]
    ].drop_duplicates("game_id")
    games = games.sort_values(
        ["game_date", "game_id"]
    ).reset_index(drop=True)

    if len(games) < 200:
        raise RuntimeError(
            "Insufficient puck-line games for final holdout."
        )

    index = int(
        math.floor(
            len(games)
            * (1.0 - FINAL_TEST_FRACTION)
        )
    )
    index = min(max(index, 1), len(games) - 1)
    return pd.Timestamp(
        games.iloc[index]["game_date"]
    )


def expanding_folds(
    dev: pd.DataFrame,
) -> list[dict[str, Any]]:
    dates = np.array(
        sorted(
            pd.Timestamp(x)
            for x in dev["game_date"].unique()
        )
    )

    initial_end = int(
        math.floor(
            len(dates)
            * INITIAL_TRAIN_DATE_FRACTION
        )
    )
    initial_end = min(
        max(initial_end, 5),
        len(dates) - N_VALIDATION_FOLDS,
    )

    chunks = [
        x
        for x in np.array_split(
            dates[initial_end:],
            N_VALIDATION_FOLDS,
        )
        if len(x)
    ]

    folds = []
    for fold_number, chunk in enumerate(
        chunks,
        start=1,
    ):
        val_start = pd.Timestamp(chunk[0])
        val_end = pd.Timestamp(chunk[-1])

        train = dev[
            dev["game_date"] < val_start
        ].copy()
        validation = dev[
            (dev["game_date"] >= val_start)
            & (dev["game_date"] <= val_end)
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
                "train_start": train["game_date"].min(),
                "train_end": train["game_date"].max(),
                "validation_start": validation["game_date"].min(),
                "validation_end": validation["game_date"].max(),
            }
        )

    if len(folds) < 2:
        raise RuntimeError(
            "Fewer than two usable expanding-window folds."
        )

    return folds


def implied_goal_means(
    margin,
    total,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    margin = np.asarray(margin, dtype=float)
    total = np.asarray(total, dtype=float)

    home_lambda = (
        total + margin
    ) / 2.0
    away_lambda = (
        total - margin
    ) / 2.0

    valid = (
        np.isfinite(home_lambda)
        & np.isfinite(away_lambda)
        & (home_lambda > 0)
        & (away_lambda > 0)
    )
    return home_lambda, away_lambda, valid


def skellam_cover_probability(
    margin,
    total,
    home_line,
) -> np.ndarray:
    margin = np.asarray(margin, dtype=float)
    total = np.asarray(total, dtype=float)
    home_line = np.asarray(home_line, dtype=float)

    home_lambda, away_lambda, valid = (
        implied_goal_means(margin, total)
    )
    out = np.full(len(margin), np.nan)

    threshold = np.floor(-home_line)
    out[valid] = (
        1.0
        - skellam.cdf(
            threshold[valid],
            home_lambda[valid],
            away_lambda[valid],
        )
    )
    return np.clip(out, EPS, 1.0 - EPS)


def empirical_margin_probability(
    train_actual_margin,
    train_pred_margin,
    target_pred_margin,
    target_home_line,
) -> np.ndarray:
    residual = (
        np.asarray(train_actual_margin, dtype=float)
        - np.asarray(train_pred_margin, dtype=float)
    )
    residual = residual[
        np.isfinite(residual)
    ]

    if len(residual) < MIN_TRAIN_ROWS:
        return np.full(
            len(target_pred_margin),
            np.nan,
        )

    target_pred_margin = np.asarray(
        target_pred_margin,
        dtype=float,
    )
    target_home_line = np.asarray(
        target_home_line,
        dtype=float,
    )

    thresholds = (
        -target_home_line
        - target_pred_margin
    )

    # Jeffreys-style smoothing prevents exact 0/1 probabilities.
    result = np.array(
        [
            (
                np.sum(residual > threshold)
                + 0.5
            )
            / (len(residual) + 1.0)
            for threshold in thresholds
        ],
        dtype=float,
    )
    return np.clip(
        result,
        EPS,
        1.0 - EPS,
    )


def direct_cover_features(
    margin,
    total,
    home_line,
) -> np.ndarray:
    margin = np.asarray(margin, dtype=float)
    total = np.asarray(total, dtype=float)
    home_line = np.asarray(home_line, dtype=float)

    expected_cover_margin = (
        margin + home_line
    )
    return np.column_stack(
        [
            expected_cover_margin,
            total,
            home_line,
        ]
    )


def fit_direct_classifier(
    margin,
    total,
    home_line,
    y,
):
    x = direct_cover_features(
        margin,
        total,
        home_line,
    )
    y = np.asarray(y, dtype=float)

    valid = (
        np.isfinite(x).all(axis=1)
        & np.isfinite(y)
    )
    x = x[valid]
    y = y[valid]

    if len(x) < MIN_TRAIN_ROWS:
        raise RuntimeError(
            "direct_classifier_insufficient_rows"
        )
    if len(np.unique(y)) < 2:
        raise RuntimeError(
            "direct_classifier_single_class"
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


def predict_direct_classifier(
    model,
    margin,
    total,
    home_line,
):
    x = direct_cover_features(
        margin,
        total,
        home_line,
    )
    valid = np.isfinite(x).all(axis=1)
    out = np.full(len(x), np.nan)
    if valid.any():
        out[valid] = model.predict_proba(
            x[valid]
        )[:, 1]
    return np.clip(
        out,
        EPS,
        1.0 - EPS,
    )


def poisson_simulation_probability(
    margin,
    total,
    home_line,
    *,
    seed: int,
) -> np.ndarray:
    margin = np.asarray(margin, dtype=float)
    total = np.asarray(total, dtype=float)
    home_line = np.asarray(home_line, dtype=float)

    home_lambda, away_lambda, valid = (
        implied_goal_means(margin, total)
    )
    out = np.full(len(margin), np.nan)
    rng = np.random.default_rng(seed)

    for index in np.flatnonzero(valid):
        home_goals = rng.poisson(
            home_lambda[index],
            size=SIMULATIONS_PER_ROW,
        )
        away_goals = rng.poisson(
            away_lambda[index],
            size=SIMULATIONS_PER_ROW,
        )
        cover = (
            home_goals
            - away_goals
            + home_line[index]
            > 0
        )
        # Smooth finite Monte Carlo estimate.
        out[index] = (
            cover.sum() + 0.5
        ) / (
            SIMULATIONS_PER_ROW + 1.0
        )

    return np.clip(
        out,
        EPS,
        1.0 - EPS,
    )


def fit_method(
    method: str,
    *,
    train_margin,
    train_total,
    train_line,
    train_actual_margin,
    train_y,
):
    if method in {
        "skellam",
        "poisson_simulation",
    }:
        return None

    if method == "empirical_margin_ecdf":
        return {
            "actual_margin": np.asarray(
                train_actual_margin,
                dtype=float,
            ),
            "pred_margin": np.asarray(
                train_margin,
                dtype=float,
            ),
        }

    if method == "direct_cover_logistic":
        return fit_direct_classifier(
            train_margin,
            train_total,
            train_line,
            train_y,
        )

    raise ValueError(method)


def predict_method(
    method: str,
    fitted,
    *,
    margin,
    total,
    home_line,
    seed: int,
):
    if method == "skellam":
        return skellam_cover_probability(
            margin,
            total,
            home_line,
        )

    if method == "empirical_margin_ecdf":
        return empirical_margin_probability(
            fitted["actual_margin"],
            fitted["pred_margin"],
            margin,
            home_line,
        )

    if method == "direct_cover_logistic":
        return predict_direct_classifier(
            fitted,
            margin,
            total,
            home_line,
        )

    if method == "poisson_simulation":
        return poisson_simulation_probability(
            margin,
            total,
            home_line,
            seed=seed,
        )

    raise ValueError(method)


def current_margin_support_rows(
    df: pd.DataFrame,
    margin,
    *,
    source: str,
    period: str,
    fold: int | None,
) -> pd.DataFrame:
    margin = np.asarray(margin, dtype=float)
    rows = []

    for record, pred_margin in zip(
        df.to_dict("records"),
        margin,
    ):
        sides = [
            (
                "home",
                pred_margin + float(
                    record["home_puck_line"]
                ),
                float(record["home_cover"]),
                float(record["home_decimal"]),
            ),
            (
                "away",
                -pred_margin + float(
                    record["away_puck_line"]
                ),
                float(record["away_cover"]),
                float(record["away_decimal"]),
            ),
        ]

        for (
            side,
            expected_cover_margin,
            actual_cover,
            decimal_odds,
        ) in sides:
            supports = (
                expected_cover_margin > 0
            )
            if not supports:
                continue

            realized_profit = (
                decimal_odds - 1.0
                if actual_cover == 1.0
                else -1.0
            )
            rows.append(
                {
                    "source": source,
                    "period": period,
                    "fold": fold,
                    "game_id": record["game_id"],
                    "game_date": record["game_date"],
                    "side": side,
                    "expected_cover_margin": (
                        expected_cover_margin
                    ),
                    "actual_cover": actual_cover,
                    "decimal_odds": decimal_odds,
                    "realized_profit": (
                        realized_profit
                    ),
                }
            )

    return pd.DataFrame(rows)


def probability_side_rows(
    df: pd.DataFrame,
    home_probability,
    *,
    source: str,
    method: str,
    period: str,
    fold: int | None,
) -> pd.DataFrame:
    home_probability = np.asarray(
        home_probability,
        dtype=float,
    )
    rows = []

    for record, home_prob in zip(
        df.to_dict("records"),
        home_probability,
    ):
        if not np.isfinite(home_prob):
            continue

        for (
            side,
            side_prob,
            actual_cover,
            decimal_odds,
            line,
        ) in [
            (
                "home",
                home_prob,
                float(record["home_cover"]),
                float(record["home_decimal"]),
                float(record["home_puck_line"]),
            ),
            (
                "away",
                1.0 - home_prob,
                float(record["away_cover"]),
                float(record["away_decimal"]),
                float(record["away_puck_line"]),
            ),
        ]:
            break_even = 1.0 / decimal_odds
            predicted_ev = (
                side_prob * decimal_odds - 1.0
            )
            realized_profit = (
                decimal_odds - 1.0
                if actual_cover == 1.0
                else -1.0
            )

            rows.append(
                {
                    "source": source,
                    "method": method,
                    "period": period,
                    "fold": fold,
                    "game_id": record["game_id"],
                    "game_date": record["game_date"],
                    "side": side,
                    "line": line,
                    "cover_probability": side_prob,
                    "break_even_probability": (
                        break_even
                    ),
                    "positive_economic_support": (
                        side_prob > break_even
                    ),
                    "predicted_ev": predicted_ev,
                    "actual_cover": actual_cover,
                    "decimal_odds": decimal_odds,
                    "realized_profit": realized_profit,
                }
            )

    return pd.DataFrame(rows)


def probability_metrics(
    home_df: pd.DataFrame,
) -> dict[str, Any]:
    valid = home_df[
        home_df["predicted_home_cover_probability"].notna()
    ].copy()

    if valid.empty:
        return {
            "game_count": 0,
            "brier_score": np.nan,
            "log_loss": np.nan,
            "mean_predicted_home_cover": np.nan,
            "realized_home_cover_rate": np.nan,
            "calibration_gap": np.nan,
        }

    y = valid["home_cover"].to_numpy(float)
    p = valid[
        "predicted_home_cover_probability"
    ].to_numpy(float)

    return {
        "game_count": len(valid),
        "brier_score": brier_score(y, p),
        "log_loss": log_loss(y, p),
        "mean_predicted_home_cover": float(
            np.mean(p)
        ),
        "realized_home_cover_rate": float(
            np.mean(y)
        ),
        "calibration_gap": float(
            np.mean(y) - np.mean(p)
        ),
    }


def betting_metrics(
    side_df: pd.DataFrame,
) -> dict[str, Any]:
    selected = side_df[
        side_df["positive_economic_support"]
    ].copy()

    if selected.empty:
        return {
            "positive_ev_bets": 0,
            "positive_ev_wins": 0,
            "positive_ev_losses": 0,
            "positive_ev_hit_rate": np.nan,
            "positive_ev_profit_units": 0.0,
            "positive_ev_roi": np.nan,
            "mean_predicted_ev": np.nan,
            "ev_bias": np.nan,
            "ev_mae": np.nan,
        }

    profit = selected[
        "realized_profit"
    ].to_numpy(float)
    predicted_ev = selected[
        "predicted_ev"
    ].to_numpy(float)

    return {
        "positive_ev_bets": len(selected),
        "positive_ev_wins": int(
            (selected["actual_cover"] == 1.0).sum()
        ),
        "positive_ev_losses": int(
            (selected["actual_cover"] == 0.0).sum()
        ),
        "positive_ev_hit_rate": float(
            selected["actual_cover"].mean()
        ),
        "positive_ev_profit_units": float(
            profit.sum()
        ),
        "positive_ev_roi": float(
            profit.mean()
        ),
        "mean_predicted_ev": float(
            predicted_ev.mean()
        ),
        "ev_bias": float(
            np.mean(
                predicted_ev - profit
            )
        ),
        "ev_mae": float(
            np.mean(
                np.abs(
                    predicted_ev - profit
                )
            )
        ),
    }


def margin_rule_metrics(
    support_df: pd.DataFrame,
) -> dict[str, Any]:
    if support_df.empty:
        return {
            "margin_rule_support_bets": 0,
            "margin_rule_wins": 0,
            "margin_rule_losses": 0,
            "margin_rule_hit_rate": np.nan,
            "margin_rule_profit_units": 0.0,
            "margin_rule_roi": np.nan,
        }

    return {
        "margin_rule_support_bets": len(
            support_df
        ),
        "margin_rule_wins": int(
            (
                support_df["actual_cover"]
                == 1.0
            ).sum()
        ),
        "margin_rule_losses": int(
            (
                support_df["actual_cover"]
                == 0.0
            ).sum()
        ),
        "margin_rule_hit_rate": float(
            support_df["actual_cover"].mean()
        ),
        "margin_rule_profit_units": float(
            support_df[
                "realized_profit"
            ].sum()
        ),
        "margin_rule_roi": float(
            support_df[
                "realized_profit"
            ].mean()
        ),
    }


def evaluate_period(
    train: pd.DataFrame,
    target: pd.DataFrame,
    *,
    period: str,
    fold: int | None,
    seed_base: int,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    bundle = fit_derived_bundle(train)

    metric_rows = []
    prediction_parts = []
    side_parts = []
    current_parts = []

    for source_index, source in enumerate(SOURCES):
        (
            train_margin,
            train_total,
        ) = source_predictions(
            source,
            train,
            bundle,
        )
        (
            target_margin,
            target_total,
        ) = source_predictions(
            source,
            target,
            bundle,
        )

        current = current_margin_support_rows(
            target,
            target_margin,
            source=source,
            period=period,
            fold=fold,
        )
        if not current.empty:
            current_parts.append(current)

        for method_index, method in enumerate(
            METHODS
        ):
            try:
                fitted = fit_method(
                    method,
                    train_margin=train_margin,
                    train_total=train_total,
                    train_line=train[
                        "home_puck_line"
                    ].to_numpy(float),
                    train_actual_margin=train[
                        "actual_margin"
                    ].to_numpy(float),
                    train_y=train[
                        "home_cover"
                    ].to_numpy(float),
                )
                predicted = predict_method(
                    method,
                    fitted,
                    margin=target_margin,
                    total=target_total,
                    home_line=target[
                        "home_puck_line"
                    ].to_numpy(float),
                    seed=(
                        seed_base
                        + source_index * 100
                        + method_index
                    ),
                )
                status = "ok"
                failure_reason = ""
            except Exception as exc:
                predicted = np.full(
                    len(target),
                    np.nan,
                )
                status = "ineligible"
                failure_reason = str(exc)

            pred_df = target[
                [
                    "game_id",
                    "game_date",
                    "home_puck_line",
                    "away_puck_line",
                    "home_decimal",
                    "away_decimal",
                    "home_cover",
                    "away_cover",
                ]
            ].copy()
            pred_df[
                "source"
            ] = source
            pred_df[
                "method"
            ] = method
            pred_df[
                "period"
            ] = period
            pred_df[
                "fold"
            ] = fold
            pred_df[
                "predicted_margin"
            ] = target_margin
            pred_df[
                "predicted_total"
            ] = target_total
            pred_df[
                "predicted_home_cover_probability"
            ] = predicted
            prediction_parts.append(
                pred_df
            )

            side_df = probability_side_rows(
                target,
                predicted,
                source=source,
                method=method,
                period=period,
                fold=fold,
            )
            if not side_df.empty:
                side_parts.append(side_df)

            pm = probability_metrics(pred_df)
            bm = betting_metrics(side_df)

            metric_rows.append(
                {
                    "period": period,
                    "fold": fold,
                    "source": source,
                    "method": method,
                    "status": status,
                    "failure_reason": failure_reason,
                    "train_rows": len(train),
                    "target_rows": len(target),
                    "train_start": (
                        train["game_date"].min()
                    ),
                    "train_end": (
                        train["game_date"].max()
                    ),
                    "target_start": (
                        target["game_date"].min()
                    ),
                    "target_end": (
                        target["game_date"].max()
                    ),
                    "weighted_margin_drat_weight": (
                        bundle.margin_weight
                    ),
                    "weighted_total_drat_weight": (
                        bundle.total_weight
                    ),
                    **pm,
                    **bm,
                }
            )

    return (
        pd.DataFrame(metric_rows),
        pd.concat(
            prediction_parts,
            ignore_index=True,
        ),
        (
            pd.concat(
                side_parts,
                ignore_index=True,
            )
            if side_parts
            else pd.DataFrame()
        ),
        (
            pd.concat(
                current_parts,
                ignore_index=True,
            )
            if current_parts
            else pd.DataFrame()
        ),
    )


def aggregate_development(
    predictions: pd.DataFrame,
    side_predictions: pd.DataFrame,
    current_support: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []

    for source in SOURCES:
        source_current = current_support[
            current_support["source"] == source
        ].copy()
        current_metrics = margin_rule_metrics(
            source_current
        )

        for method in METHODS:
            pred = predictions[
                (
                    predictions["source"]
                    == source
                )
                & (
                    predictions["method"]
                    == method
                )
            ].copy()

            side = side_predictions[
                (
                    side_predictions["source"]
                    == source
                )
                & (
                    side_predictions["method"]
                    == method
                )
            ].copy()

            pm = probability_metrics(pred)
            bm = betting_metrics(side)

            rows.append(
                {
                    "source": source,
                    "method": method,
                    **pm,
                    **bm,
                    **current_metrics,
                }
            )

    metrics = pd.DataFrame(rows)
    metrics["development_rank"] = (
        metrics[
            ["log_loss", "brier_score"]
        ]
        .apply(tuple, axis=1)
        .rank(
            method="min",
            ascending=True,
        )
    )

    rank = metrics.sort_values(
        [
            "log_loss",
            "brier_score",
            "source",
            "method",
        ]
    ).reset_index(drop=True)
    rank["development_rank"] = (
        np.arange(len(rank)) + 1
    )

    return metrics, rank


def serialize_dates(df: pd.DataFrame):
    out = df.copy()
    for col in out.columns:
        if "date" in str(col).lower() or (
            str(col).endswith("_start")
            or str(col).endswith("_end")
        ):
            out[col] = out[col].map(
                lambda x: (
                    x.strftime("%Y-%m-%d")
                    if isinstance(
                        x,
                        (pd.Timestamp, datetime),
                    )
                    else x
                )
            )
    return out


def main() -> None:
    root = find_repo_root()
    output_dir = root / OUTPUT_REL
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    history = load_history(root)
    prices = load_archived_puck_prices(root)
    dataset, coverage_stats = build_dataset(
        history,
        prices,
    )

    final_start = determine_final_start(
        dataset
    )
    dev = dataset[
        dataset["game_date"] < final_start
    ].copy()
    final = dataset[
        dataset["game_date"] >= final_start
    ].copy()

    split_manifest = {
        "created_at_utc": (
            datetime.now(UTC).isoformat()
        ),
        "research_only": True,
        "production_files_modified": False,
        "seed": SEED,
        "simulations_per_row": (
            SIMULATIONS_PER_ROW
        ),
        "requested_final_test_fraction": (
            FINAL_TEST_FRACTION
        ),
        "development_start": (
            dev["game_date"]
            .min()
            .strftime("%Y-%m-%d")
        ),
        "development_end": (
            dev["game_date"]
            .max()
            .strftime("%Y-%m-%d")
        ),
        "final_test_start": (
            final["game_date"]
            .min()
            .strftime("%Y-%m-%d")
        ),
        "final_test_end": (
            final["game_date"]
            .max()
            .strftime("%Y-%m-%d")
        ),
        "development_games": len(dev),
        "final_test_games": len(final),
        "coverage": coverage_stats,
        "no_push_rule": (
            "research dataset restricted to "
            "opposing non-integer puck lines"
        ),
    }

    (
        output_dir / "split_manifest.json"
    ).write_text(
        json.dumps(
            split_manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    folds = expanding_folds(dev)

    fold_metric_parts = []
    validation_prediction_parts = []
    validation_side_parts = []
    validation_current_parts = []

    for fold in folds:
        (
            metrics,
            predictions,
            side_predictions,
            current_support,
        ) = evaluate_period(
            fold["train"],
            fold["validation"],
            period="development_validation",
            fold=fold["fold"],
            seed_base=(
                SEED
                + fold["fold"] * 1000
            ),
        )

        fold_metric_parts.append(metrics)
        validation_prediction_parts.append(
            predictions
        )
        if not side_predictions.empty:
            validation_side_parts.append(
                side_predictions
            )
        if not current_support.empty:
            validation_current_parts.append(
                current_support
            )

    fold_metrics = pd.concat(
        fold_metric_parts,
        ignore_index=True,
    )
    validation_predictions = pd.concat(
        validation_prediction_parts,
        ignore_index=True,
    )
    validation_side = pd.concat(
        validation_side_parts,
        ignore_index=True,
    )
    validation_current = pd.concat(
        validation_current_parts,
        ignore_index=True,
    )

    (
        development_metrics,
        development_ranking,
    ) = aggregate_development(
        validation_predictions,
        validation_side,
        validation_current,
    )

    # Final test is evaluated for all methods only after development
    # comparison is complete. No production promotion is performed.
    (
        final_metrics,
        final_predictions,
        final_side,
        final_current,
    ) = evaluate_period(
        dev,
        final,
        period="final_test",
        fold=None,
        seed_base=SEED + 900000,
    )

    current_final_rows = []
    for source in SOURCES:
        current_final_rows.append(
            {
                "source": source,
                **margin_rule_metrics(
                    final_current[
                        final_current[
                            "source"
                        ]
                        == source
                    ]
                ),
            }
        )
    current_final_metrics = pd.DataFrame(
        current_final_rows
    )

    # Attach current point-estimate comparator to each final method.
    final_metrics = final_metrics.merge(
        current_final_metrics,
        on="source",
        how="left",
        validate="many_to_one",
    )

    # Difference in support decisions on the final test.
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
            candidate = final_side[
                (
                    final_side["source"]
                    == source
                )
                & (
                    final_side["method"]
                    == method
                )
                & (
                    final_side[
                        "positive_economic_support"
                    ]
                )
            ].copy()

            new_keys = set(
                zip(
                    candidate["game_id"].astype(str),
                    candidate["side"].astype(str),
                )
            )

            support_change_rows.append(
                {
                    "source": source,
                    "method": method,
                    "current_margin_rule_supports": len(
                        current_keys
                    ),
                    "price_aware_probability_supports": len(
                        new_keys
                    ),
                    "support_overlap": len(
                        current_keys & new_keys
                    ),
                    "current_only": len(
                        current_keys - new_keys
                    ),
                    "price_aware_only": len(
                        new_keys - current_keys
                    ),
                }
            )

    support_changes = pd.DataFrame(
        support_change_rows
    )

    package_versions = {
        "python": platform.python_version(),
        "numpy": package_version("numpy"),
        "pandas": package_version("pandas"),
        "scipy": package_version("scipy"),
        "scikit-learn": package_version(
            "scikit-learn"
        ),
    }

    run_manifest = {
        **split_manifest,
        "package_versions": package_versions,
        "sources": list(SOURCES),
        "methods": list(METHODS),
        "validation_folds": len(folds),
        "selection_or_promotion_performed": False,
        "current_production_rule": (
            "expected_margin + side_line > 0"
        ),
        "economic_support_rule": (
            "P(cover) > 1 / offered_decimal_odds"
        ),
        "weighted_model": (
            "D-Ratings weight selected on prior RMSE "
            "using grid 0.00..1.00 by 0.01"
        ),
        "meta_model": (
            "linear model of D-Ratings prediction, "
            "SDV prediction, absolute disagreement"
        ),
        "skellam_method": (
            "lambda_home=(expected_total+expected_margin)/2; "
            "lambda_away=(expected_total-expected_margin)/2"
        ),
        "empirical_margin_method": (
            "prior residual ECDF with 0.5/(n+1) smoothing"
        ),
        "direct_classifier_features": [
            "expected_margin + home_puck_line",
            "expected_total",
            "home_puck_line",
        ],
        "direct_classifier": {
            "family": "LogisticRegression",
            "C": 1.0,
            "solver": "lbfgs",
            "max_iter": 5000,
            "standardization": True,
            "random_state": SEED,
        },
        "simulation_method": (
            "independent Poisson scoring using implied goal means"
        ),
    }

    (
        output_dir / "run_manifest.json"
    ).write_text(
        json.dumps(
            run_manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n",
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
    serialize_dates(
        validation_predictions
    ).to_csv(
        output_dir
        / "validation_cover_predictions.csv",
        index=False,
    )
    serialize_dates(
        validation_side
    ).to_csv(
        output_dir
        / "validation_side_economics.csv",
        index=False,
    )
    serialize_dates(
        validation_current
    ).to_csv(
        output_dir
        / "validation_current_margin_support.csv",
        index=False,
    )
    serialize_dates(
        final_metrics
    ).to_csv(
        output_dir / "final_test_metrics.csv",
        index=False,
    )
    serialize_dates(
        final_predictions
    ).to_csv(
        output_dir / "final_test_cover_predictions.csv",
        index=False,
    )
    serialize_dates(
        final_side
    ).to_csv(
        output_dir / "final_test_side_economics.csv",
        index=False,
    )
    serialize_dates(
        final_current
    ).to_csv(
        output_dir / "final_test_current_margin_support.csv",
        index=False,
    )
    support_changes.to_csv(
        output_dir / "support_decision_changes.csv",
        index=False,
    )

    final_rank = final_metrics.sort_values(
        [
            "log_loss",
            "brier_score",
            "source",
            "method",
        ]
    ).reset_index(drop=True)

    best_development = (
        development_ranking.iloc[0]
    )
    best_final = final_rank.iloc[0]

    summary = [
        "NHL PUCK-LINE SECONDARY COVER-PROBABILITY RESEARCH",
        "==================================================",
        "research_only=true",
        "production_files_modified=false",
        "production_promotion_performed=false",
        (
            "current_rule="
            "expected_margin + side_line > 0"
        ),
        (
            "research_rule="
            "P(cover) > 1 / offered_decimal_odds"
        ),
        (
            f"valid_puck_games={len(dataset)}"
        ),
        (
            f"development_games={len(dev)}"
        ),
        (
            f"final_test_games={len(final)}"
        ),
        (
            "final_test_start="
            f"{split_manifest['final_test_start']}"
        ),
        (
            f"validation_folds={len(folds)}"
        ),
        (
            "sources="
            + ",".join(SOURCES)
        ),
        (
            "methods="
            + ",".join(METHODS)
        ),
        "",
        "BEST DEVELOPMENT COMBINATION (informational only)",
        (
            f"source={best_development['source']} | "
            f"method={best_development['method']} | "
            f"Brier={best_development['brier_score']:.6f} | "
            f"log_loss={best_development['log_loss']:.6f} | "
            f"positive_EV_ROI={best_development['positive_ev_roi']:.6f}"
        ),
        "",
        "BEST FINAL-TEST COMBINATION (evaluation only; not used for selection)",
        (
            f"source={best_final['source']} | "
            f"method={best_final['method']} | "
            f"Brier={best_final['brier_score']:.6f} | "
            f"log_loss={best_final['log_loss']:.6f} | "
            f"positive_EV_bets={int(best_final['positive_ev_bets'])} | "
            f"positive_EV_ROI={best_final['positive_ev_roi']:.6f}"
        ),
        "",
        "FINAL-TEST COMPARISON",
    ]

    for row in final_rank.to_dict("records"):
        summary.append(
            f"{row['source']} / {row['method']}: "
            f"Brier={row['brier_score']:.6f} | "
            f"log_loss={row['log_loss']:.6f} | "
            f"cal_gap={row['calibration_gap']:.6f} | "
            f"positive_EV_bets={int(row['positive_ev_bets'])} | "
            f"positive_EV_hit={row['positive_ev_hit_rate']:.6f} | "
            f"positive_EV_ROI={row['positive_ev_roi']:.6f} | "
            f"current_margin_rule_ROI={row['margin_rule_roi']:.6f}"
        )

    summary += [
        "",
        "Artifacts:",
        str(
            output_dir
            / "split_manifest.json"
        ),
        str(
            output_dir
            / "run_manifest.json"
        ),
        str(
            output_dir
            / "research_dataset.csv"
        ),
        str(
            output_dir
            / "fold_metrics.csv"
        ),
        str(
            output_dir
            / "development_metrics.csv"
        ),
        str(
            output_dir
            / "development_ranking.csv"
        ),
        str(
            output_dir
            / "validation_cover_predictions.csv"
        ),
        str(
            output_dir
            / "validation_side_economics.csv"
        ),
        str(
            output_dir
            / "final_test_metrics.csv"
        ),
        str(
            output_dir
            / "final_test_cover_predictions.csv"
        ),
        str(
            output_dir
            / "final_test_side_economics.csv"
        ),
        str(
            output_dir
            / "support_decision_changes.csv"
        ),
    ]

    (
        output_dir / "summary.txt"
    ).write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8",
    )

    print(
        "PUCK-LINE SECONDARY COVER RESEARCH COMPLETE"
    )
    print(f"Output: {output_dir}")
    print(
        "Valid no-push opposing puck games: "
        f"{len(dataset)}"
    )
    print(
        "Final test starts: "
        f"{split_manifest['final_test_start']}"
    )
    print()
    print(
        final_rank[
            [
                "source",
                "method",
                "game_count",
                "brier_score",
                "log_loss",
                "positive_ev_bets",
                "positive_ev_hit_rate",
                "positive_ev_roi",
                "margin_rule_roi",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
