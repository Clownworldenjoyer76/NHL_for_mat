#!/usr/bin/env python3
"""
Research-only NHL probability calibration search.

Purpose
-------
Compare raw probabilities, the existing Stage 02 adjustment, and proper
calibration methods without modifying production Stage 02 artifacts.

Data
----
Historical merged inputs:
  docs/win/hockey/nhl/archive/2025_26/01_merge/*_NHL_merged.csv

Historical final scores:
  docs/win/hockey/nhl/archive/2025_26/05_final_scores/final_scores/*_NHL_final_scores.csv

Current production adjustment configs are read-only inputs:
  docs/win/hockey/nhl/config/juice/nhl_moneyline_juice.csv
  docs/win/hockey/nhl/config/juice/nhl_puck_line_juice.csv
  docs/win/hockey/nhl/config/juice/nhl_total_juice.csv

Outputs
-------
Only:
  docs/win/hockey/nhl/research/probability_calibration_search/

Methodology
-----------
1. Reserve the final chronological 20% of games BEFORE model selection.
2. Use four chronological expanding-window folds within the earlier development period.
3. Search moneyline, puck line, and total independently.
4. Candidate families:
   - raw/unadjusted baseline
   - existing production Stage 02 adjustment baseline
   - Platt/logistic using raw probability
   - logistic using logit(raw probability)
   - constrained beta calibration
   - isotonic regression when sample requirements are met
5. Select each market winner using development validation log loss, then Brier score.
6. Refit the locked winner on all development data and evaluate once on the untouched
   final chronological test.
7. Final test is never used for model family, features, hyperparameters, calibration
   method, disagreement thresholds, or selection thresholds.

No markets.yaml file is read. Betting evaluation uses one fixed research rule:
predicted expected value > 0.
"""

from __future__ import annotations

import json
import math
import platform
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime, UTC
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
from scipy.optimize import minimize
from scipy.special import expit
from scipy.stats import poisson, skellam
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression


warnings.filterwarnings("ignore", category=PerformanceWarning)

NHL_REL = Path("docs/win/hockey/nhl")
ARCHIVE_INPUT_REL = NHL_REL / "archive/2025_26/01_merge"
ARCHIVE_SCORE_REL = NHL_REL / "archive/2025_26/05_final_scores/final_scores"
CONFIG_REL = NHL_REL / "config/juice"
OUTPUT_REL = NHL_REL / "research/probability_calibration_search"

SEED = 20260912
FINAL_TEST_FRACTION = 0.20
N_VALIDATION_FOLDS = 4
INITIAL_TRAIN_DATE_FRACTION = 0.40
PROB_EPS = 1e-12
RELIABILITY_BINS = 10

LOGISTIC_C_GRID = [0.01, 0.1, 1.0, 10.0, 100.0]
BETA_L2_GRID = [0.0, 1e-4, 1e-3, 1e-2, 1e-1]

ISOTONIC_MIN_TRAIN_DECISIONS = 300
ISOTONIC_MIN_CLASS_COUNT = 30
ISOTONIC_MIN_UNIQUE_PROB = 20

MIN_TRAIN_DECISIONS = 200
MIN_VALIDATION_DECISIONS = 50

CANONICAL_SIDE = {
    "moneyline": "home",
    "puck_line": "home",
    "total": "over",
}

OPPOSITE_SIDE = {
    "moneyline": "away",
    "puck_line": "away",
    "total": "under",
}


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    family: str
    feature_spec: str
    hyperparameters: dict[str, Any]


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve()]
    seen: set[Path] = set()
    for start in starts:
        for candidate in [start, *start.parents]:
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / NHL_REL).is_dir():
                return candidate
    raise RuntimeError(f"Could not find repository root containing {NHL_REL}")


def package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not-installed"


def parse_date(value) -> pd.Timestamp | None:
    ts = pd.to_datetime(str(value).strip().replace("_", "-"), errors="coerce")
    if pd.isna(ts):
        return None
    return ts.normalize()


def to_float(value) -> float:
    try:
        x = float(value)
    except Exception:
        return np.nan
    return x if math.isfinite(x) else np.nan


def valid_probability(p: float) -> bool:
    return bool(np.isfinite(p) and 0.0 < p < 1.0)


def clip_probability(p):
    return np.clip(np.asarray(p, dtype=float), PROB_EPS, 1.0 - PROB_EPS)


def logit(p):
    p = clip_probability(p)
    return np.log(p / (1.0 - p))


def american_to_decimal(value) -> float:
    a = to_float(value)
    if not np.isfinite(a) or a == 0:
        return np.nan
    if a > 0:
        return 1.0 + a / 100.0
    return 1.0 + 100.0 / abs(a)


def resolve_decimal(decimal_value, american_value) -> float:
    d = to_float(decimal_value)
    if np.isfinite(d) and d > 1.0:
        return d
    return american_to_decimal(american_value)


def read_current_configs(root: Path) -> dict[str, pd.DataFrame]:
    paths = {
        "moneyline": root / CONFIG_REL / "nhl_moneyline_juice.csv",
        "puck_line": root / CONFIG_REL / "nhl_puck_line_juice.csv",
        "total": root / CONFIG_REL / "nhl_total_juice.csv",
    }
    out = {}
    for market, path in paths.items():
        if not path.exists():
            raise RuntimeError(f"Missing production config: {path}")
        df = pd.read_csv(path)
        for col in ["band_min", "band_max", "model_calibration_adjustment"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        out[market] = df
    return out


def lookup_adjustment(
    configs: dict[str, pd.DataFrame],
    market: str,
    *,
    american: float | None = None,
    line: float | None = None,
    side: str,
) -> float:
    df = configs[market]

    if market == "moneyline":
        if american is None or not np.isfinite(american):
            return np.nan
        fav_ud = "favorite" if american < 0 else "underdog"
        hit = df[
            (df["band_min"] <= american)
            & (american <= df["band_max"])
            & (df["fav_ud"].astype(str).str.strip() == fav_ud)
            & (df["venue"].astype(str).str.strip() == side)
        ]

    elif market == "puck_line":
        if line is None or not np.isfinite(line):
            return np.nan
        fav_ud = "favorite" if line < 0 else "underdog"
        hit = df[
            (df["band_min"] <= line)
            & (line <= df["band_max"])
            & (df["fav_ud"].astype(str).str.strip() == fav_ud)
            & (df["venue"].astype(str).str.strip() == side)
        ]

    elif market == "total":
        if line is None or not np.isfinite(line):
            return np.nan
        hit = df[
            (df["band_min"] <= line)
            & (line <= df["band_max"])
            & (df["side"].astype(str).str.strip() == side)
        ]
    else:
        raise ValueError(market)

    if len(hit) != 1:
        return np.nan

    return to_float(hit.iloc[0]["model_calibration_adjustment"])


def production_adjust_pair(p1: float, p2: float, a1: float, a2: float) -> tuple[float, float] | None:
    if not all(valid_probability(v) for v in [p1, p2]):
        return None
    if not all(np.isfinite(v) for v in [a1, a2]):
        return None

    fair1 = 1.0 / p1
    fair2 = 1.0 / p2
    adjusted_decimal1 = fair1 * (1.0 - a1)
    adjusted_decimal2 = fair2 * (1.0 - a2)

    if (
        not np.isfinite(adjusted_decimal1)
        or not np.isfinite(adjusted_decimal2)
        or adjusted_decimal1 <= 1.0
        or adjusted_decimal2 <= 1.0
    ):
        return None

    q1 = 1.0 / adjusted_decimal1
    q2 = 1.0 / adjusted_decimal2
    total = q1 + q2
    if not np.isfinite(total) or total <= 0:
        return None

    return float(q1 / total), float(q2 / total)


def normalize_pair(p1: float, p2: float) -> tuple[float, float] | None:
    if not all(valid_probability(v) for v in [p1, p2]):
        return None
    total = p1 + p2
    if not np.isfinite(total) or total <= 0:
        return None
    return float(p1 / total), float(p2 / total)


def puck_probability(side_line: float, side_goals: float, opp_goals: float) -> float:
    if not all(np.isfinite(v) for v in [side_line, side_goals, opp_goals]):
        return np.nan
    if side_goals <= 0 or opp_goals <= 0:
        return np.nan
    threshold = math.floor(-side_line)
    p = 1.0 - skellam.cdf(threshold, side_goals, opp_goals)
    return float(p) if np.isfinite(p) else np.nan


def total_probabilities(total_line: float, total_goals: float) -> tuple[float, float, float] | None:
    if not np.isfinite(total_line) or not np.isfinite(total_goals) or total_goals <= 0:
        return None

    if float(total_line).is_integer():
        push_total = int(total_line)
        under_win = float(poisson.cdf(push_total - 1, total_goals))
        push = float(poisson.pmf(push_total, total_goals))
        over_win = float(1.0 - poisson.cdf(push_total, total_goals))
        no_push = under_win + over_win
        if no_push <= 0:
            return None
        under = under_win / no_push
        over = over_win / no_push
    else:
        cutoff = math.floor(total_line)
        under_win = float(poisson.cdf(cutoff, total_goals))
        over_win = 1.0 - under_win
        push = 0.0
        under = under_win
        over = over_win

    pair = normalize_pair(over, under)
    if pair is None:
        return None
    return pair[0], pair[1], push


def load_historical_inputs(root: Path) -> pd.DataFrame:
    input_dir = root / ARCHIVE_INPUT_REL
    files = sorted(input_dir.glob("*_NHL_merged.csv"))
    if not files:
        raise RuntimeError(f"No historical merged files found: {input_dir}")

    parts = []
    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        if df.empty:
            continue
        required = [
            "game_id", "game_date",
            "home_prob_moneyline", "away_prob_moneyline",
            "home_projected_goals", "away_projected_goals", "total_projected_goals",
            "home_puck_line", "away_puck_line", "total",
            "home_dk_moneyline_american", "away_dk_moneyline_american",
            "home_dk_moneyline_decimal", "away_dk_moneyline_decimal",
            "home_dk_puck_line_american", "away_dk_puck_line_american",
            "home_dk_puck_line_decimal", "away_dk_puck_line_decimal",
            "dk_total_over_american", "dk_total_under_american",
            "dk_total_over_decimal", "dk_total_under_decimal",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing required columns: {missing}")
        df = df[required].copy()
        df["source_input_file"] = path.name
        parts.append(df)

    merged = pd.concat(parts, ignore_index=True)
    merged["game_id"] = merged["game_id"].astype(str).str.strip()
    merged["game_date"] = merged["game_date"].map(parse_date)
    merged = merged[merged["game_date"].notna() & merged["game_id"].ne("")].copy()

    duplicate = merged[merged.duplicated("game_id", keep=False)].copy()
    if not duplicate.empty:
        compare_cols = [c for c in merged.columns if c not in {"source_input_file"}]
        conflicts = []
        for game_id, group in duplicate.groupby("game_id"):
            if len(group[compare_cols].drop_duplicates()) > 1:
                conflicts.append(game_id)
        if conflicts:
            raise RuntimeError(f"Conflicting historical merged game_ids: {conflicts[:20]}")
        merged = merged.drop_duplicates("game_id", keep="first").copy()

    return merged


def load_final_scores(root: Path) -> pd.DataFrame:
    score_dir = root / ARCHIVE_SCORE_REL
    files = sorted(score_dir.glob("*_NHL_final_scores.csv"))
    if not files:
        raise RuntimeError(f"No historical final-score files found: {score_dir}")

    parts = []
    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        if df.empty:
            continue
        required = ["game_id", "away_score", "home_score"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing score columns: {missing}")
        part = df[required].copy()
        part["source_score_file"] = path.name
        parts.append(part)

    scores = pd.concat(parts, ignore_index=True)
    scores["game_id"] = scores["game_id"].astype(str).str.strip()
    scores["away_score"] = pd.to_numeric(scores["away_score"], errors="coerce")
    scores["home_score"] = pd.to_numeric(scores["home_score"], errors="coerce")
    scores = scores[
        scores["game_id"].ne("")
        & scores["away_score"].notna()
        & scores["home_score"].notna()
    ].copy()

    duplicate = scores[scores.duplicated("game_id", keep=False)].copy()
    if not duplicate.empty:
        conflicts = []
        for game_id, group in duplicate.groupby("game_id"):
            if len(group[["away_score", "home_score"]].drop_duplicates()) > 1:
                conflicts.append(game_id)
        if conflicts:
            raise RuntimeError(f"Conflicting final-score game_ids: {conflicts[:20]}")
        scores = scores.drop_duplicates("game_id", keep="first").copy()

    return scores


def build_market_rows(
    historical: pd.DataFrame,
    scores: pd.DataFrame,
    configs: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    base = historical.merge(scores, on="game_id", how="inner", validate="one_to_one")
    outputs: dict[str, list[dict]] = {"moneyline": [], "puck_line": [], "total": []}

    for row in base.to_dict("records"):
        game_id = str(row["game_id"])
        game_date = row["game_date"]
        home_score = to_float(row["home_score"])
        away_score = to_float(row["away_score"])
        if not all(np.isfinite(v) for v in [home_score, away_score]):
            continue

        # MONEYLINE
        home_raw = to_float(row["home_prob_moneyline"])
        away_raw = to_float(row["away_prob_moneyline"])
        raw_pair = normalize_pair(home_raw, away_raw)

        home_ml_a = to_float(row["home_dk_moneyline_american"])
        away_ml_a = to_float(row["away_dk_moneyline_american"])
        home_ml_d = resolve_decimal(row["home_dk_moneyline_decimal"], home_ml_a)
        away_ml_d = resolve_decimal(row["away_dk_moneyline_decimal"], away_ml_a)

        if raw_pair and home_ml_d > 1 and away_ml_d > 1:
            ah = lookup_adjustment(
                configs, "moneyline", american=home_ml_a, side="home"
            )
            aa = lookup_adjustment(
                configs, "moneyline", american=away_ml_a, side="away"
            )
            prod = production_adjust_pair(raw_pair[0], raw_pair[1], ah, aa)
            if prod is not None:
                outputs["moneyline"].append(
                    {
                        "market": "moneyline",
                        "game_id": game_id,
                        "game_date": game_date,
                        "canonical_side": "home",
                        "opposite_side": "away",
                        "raw_prob": raw_pair[0],
                        "production_prob": prod[0],
                        "canonical_decimal": home_ml_d,
                        "opposite_decimal": away_ml_d,
                        "canonical_american": home_ml_a,
                        "opposite_american": away_ml_a,
                        "line": np.nan,
                        "opposite_line": np.nan,
                        "push_prob": 0.0,
                        "is_push": False,
                        "y": float(home_score > away_score),
                        "home_score": home_score,
                        "away_score": away_score,
                        "source_input_file": row["source_input_file"],
                        "source_score_file": row["source_score_file"],
                    }
                )

        # PUCK LINE
        home_line = to_float(row["home_puck_line"])
        away_line = to_float(row["away_puck_line"])
        home_goals = to_float(row["home_projected_goals"])
        away_goals = to_float(row["away_projected_goals"])
        home_pl_a = to_float(row["home_dk_puck_line_american"])
        away_pl_a = to_float(row["away_dk_puck_line_american"])
        home_pl_d = resolve_decimal(row["home_dk_puck_line_decimal"], home_pl_a)
        away_pl_d = resolve_decimal(row["away_dk_puck_line_decimal"], away_pl_a)

        if (
            np.isfinite(home_line)
            and np.isfinite(away_line)
            and abs(home_line + away_line) <= 1e-9
            and home_pl_d > 1
            and away_pl_d > 1
        ):
            ph = puck_probability(home_line, home_goals, away_goals)
            pa = puck_probability(away_line, away_goals, home_goals)
            raw_pair = normalize_pair(ph, pa)
            if raw_pair:
                ah = lookup_adjustment(
                    configs, "puck_line", line=home_line, side="home"
                )
                aa = lookup_adjustment(
                    configs, "puck_line", line=away_line, side="away"
                )
                prod = production_adjust_pair(raw_pair[0], raw_pair[1], ah, aa)
                if prod is not None:
                    diff = home_score - away_score + home_line
                    is_push = abs(diff) <= 1e-9
                    y = np.nan if is_push else float(diff > 0)
                    outputs["puck_line"].append(
                        {
                            "market": "puck_line",
                            "game_id": game_id,
                            "game_date": game_date,
                            "canonical_side": "home",
                            "opposite_side": "away",
                            "raw_prob": raw_pair[0],
                            "production_prob": prod[0],
                            "canonical_decimal": home_pl_d,
                            "opposite_decimal": away_pl_d,
                            "canonical_american": home_pl_a,
                            "opposite_american": away_pl_a,
                            "line": home_line,
                            "opposite_line": away_line,
                            "push_prob": 0.0,
                            "is_push": is_push,
                            "y": y,
                            "home_score": home_score,
                            "away_score": away_score,
                            "source_input_file": row["source_input_file"],
                            "source_score_file": row["source_score_file"],
                        }
                    )

        # TOTAL
        total_line = to_float(row["total"])
        projected_total = to_float(row["total_projected_goals"])
        over_a = to_float(row["dk_total_over_american"])
        under_a = to_float(row["dk_total_under_american"])
        over_d = resolve_decimal(row["dk_total_over_decimal"], over_a)
        under_d = resolve_decimal(row["dk_total_under_decimal"], under_a)

        probs = total_probabilities(total_line, projected_total)
        if probs and over_d > 1 and under_d > 1:
            over_raw, under_raw, push_prob = probs
            ao = lookup_adjustment(
                configs, "total", line=total_line, side="over"
            )
            au = lookup_adjustment(
                configs, "total", line=total_line, side="under"
            )
            prod = production_adjust_pair(over_raw, under_raw, ao, au)
            if prod is not None:
                realized_total = home_score + away_score
                diff = realized_total - total_line
                is_push = abs(diff) <= 1e-9
                y = np.nan if is_push else float(diff > 0)
                outputs["total"].append(
                    {
                        "market": "total",
                        "game_id": game_id,
                        "game_date": game_date,
                        "canonical_side": "over",
                        "opposite_side": "under",
                        "raw_prob": over_raw,
                        "production_prob": prod[0],
                        "canonical_decimal": over_d,
                        "opposite_decimal": under_d,
                        "canonical_american": over_a,
                        "opposite_american": under_a,
                        "line": total_line,
                        "opposite_line": total_line,
                        "push_prob": push_prob,
                        "is_push": is_push,
                        "y": y,
                        "home_score": home_score,
                        "away_score": away_score,
                        "source_input_file": row["source_input_file"],
                        "source_score_file": row["source_score_file"],
                    }
                )

    result = {}
    for market, rows in outputs.items():
        if not rows:
            raise RuntimeError(f"No valid research rows built for {market}")
        df = pd.DataFrame(rows).sort_values(["game_date", "game_id"]).reset_index(drop=True)
        result[market] = df
    return result


def determine_final_test_start(all_market_rows: dict[str, pd.DataFrame]) -> pd.Timestamp:
    games = pd.concat(
        [df[["game_id", "game_date"]] for df in all_market_rows.values()],
        ignore_index=True,
    ).drop_duplicates("game_id")
    games = games.sort_values(["game_date", "game_id"]).reset_index(drop=True)
    if len(games) < 100:
        raise RuntimeError("Insufficient games for chronological final test reservation.")

    target_index = int(math.floor(len(games) * (1.0 - FINAL_TEST_FRACTION)))
    target_index = min(max(target_index, 1), len(games) - 1)
    cutoff_date = games.iloc[target_index]["game_date"]
    return pd.Timestamp(cutoff_date)


def candidate_grid() -> list[Candidate]:
    candidates = [
        Candidate(
            "raw_baseline",
            "raw_baseline",
            "raw_probability",
            {},
        ),
        Candidate(
            "production_baseline",
            "production_baseline",
            "current_stage02_adjusted_probability",
            {},
        ),
    ]

    for c in LOGISTIC_C_GRID:
        candidates.append(
            Candidate(
                f"platt_raw_C={c:g}",
                "platt_logistic_raw_probability",
                "[raw_probability]",
                {
                    "C": c,
                    "solver": "lbfgs",
                    "max_iter": 5000,
                    "random_state": SEED,
                    "class_weight": None,
                },
            )
        )
        candidates.append(
            Candidate(
                f"logit_logistic_C={c:g}",
                "logistic_logit_probability",
                "[logit(raw_probability)]",
                {
                    "C": c,
                    "solver": "lbfgs",
                    "max_iter": 5000,
                    "random_state": SEED,
                    "class_weight": None,
                },
            )
        )

    for l2 in BETA_L2_GRID:
        candidates.append(
            Candidate(
                f"beta_l2={l2:g}",
                "beta_calibration",
                "[log(raw_probability), -log(1-raw_probability)]",
                {
                    "l2": l2,
                    "constraints": "a>=0,b>=0",
                    "optimizer": "L-BFGS-B",
                    "maxiter": 5000,
                },
            )
        )

    candidates.append(
        Candidate(
            "isotonic",
            "isotonic_regression",
            "raw_probability",
            {
                "out_of_bounds": "clip",
                "min_train_decisions": ISOTONIC_MIN_TRAIN_DECISIONS,
                "min_class_count": ISOTONIC_MIN_CLASS_COUNT,
                "min_unique_probability": ISOTONIC_MIN_UNIQUE_PROB,
            },
        )
    )
    return candidates


def expanding_folds(dev: pd.DataFrame) -> list[dict]:
    dates = np.array(sorted(pd.Timestamp(x) for x in dev["game_date"].dropna().unique()))
    if len(dates) < 20:
        raise RuntimeError("Insufficient development dates for expanding-window folds.")

    initial_end = int(math.floor(len(dates) * INITIAL_TRAIN_DATE_FRACTION))
    initial_end = min(max(initial_end, 5), len(dates) - N_VALIDATION_FOLDS)
    validation_dates = dates[initial_end:]
    chunks = [chunk for chunk in np.array_split(validation_dates, N_VALIDATION_FOLDS) if len(chunk)]

    folds = []
    for fold_index, chunk in enumerate(chunks, start=1):
        val_start = pd.Timestamp(chunk[0])
        val_end = pd.Timestamp(chunk[-1])
        train = dev[dev["game_date"] < val_start].copy()
        val = dev[(dev["game_date"] >= val_start) & (dev["game_date"] <= val_end)].copy()

        train_decisions = train[~train["is_push"] & train["y"].notna()]
        val_decisions = val[~val["is_push"] & val["y"].notna()]

        if len(train_decisions) < MIN_TRAIN_DECISIONS:
            continue
        if len(val_decisions) < MIN_VALIDATION_DECISIONS:
            continue

        folds.append(
            {
                "fold": fold_index,
                "train": train,
                "validation": val,
                "train_start": train["game_date"].min(),
                "train_end": train["game_date"].max(),
                "validation_start": val["game_date"].min(),
                "validation_end": val["game_date"].max(),
            }
        )

    if len(folds) < 2:
        raise RuntimeError("Fewer than two usable chronological expanding-window folds.")
    return folds


class BetaCalibrator:
    def __init__(self, l2: float):
        self.l2 = float(l2)
        self.params_: np.ndarray | None = None

    def fit(self, p, y):
        p = clip_probability(p)
        y = np.asarray(y, dtype=float)
        x1 = np.log(p)
        x2 = -np.log(1.0 - p)

        def objective(theta):
            a, b, c = theta
            z = a * x1 + b * x2 + c
            q = np.clip(expit(z), PROB_EPS, 1.0 - PROB_EPS)
            nll = -np.mean(y * np.log(q) + (1.0 - y) * np.log(1.0 - q))
            penalty = self.l2 * (a * a + b * b)
            return float(nll + penalty)

        result = minimize(
            objective,
            x0=np.array([1.0, 1.0, 0.0], dtype=float),
            method="L-BFGS-B",
            bounds=[(0.0, None), (0.0, None), (None, None)],
            options={"maxiter": 5000},
        )
        if not result.success:
            raise RuntimeError(f"Beta calibration optimizer failed: {result.message}")
        self.params_ = np.asarray(result.x, dtype=float)
        return self

    def predict(self, p):
        if self.params_ is None:
            raise RuntimeError("Beta calibrator not fitted.")
        p = clip_probability(p)
        a, b, c = self.params_
        z = a * np.log(p) + b * (-np.log(1.0 - p)) + c
        return np.clip(expit(z), PROB_EPS, 1.0 - PROB_EPS)

    def fitted_parameters(self):
        if self.params_ is None:
            return {}
        return {
            "a": float(self.params_[0]),
            "b": float(self.params_[1]),
            "c": float(self.params_[2]),
        }


def fit_candidate(candidate: Candidate, train: pd.DataFrame):
    decisions = train[~train["is_push"] & train["y"].notna()].copy()
    p = decisions["raw_prob"].to_numpy(dtype=float)
    y = decisions["y"].to_numpy(dtype=float)

    if candidate.family in {"raw_baseline", "production_baseline"}:
        return None, {}

    if len(np.unique(y)) < 2:
        raise RuntimeError("Training fold contains only one outcome class.")

    if candidate.family == "platt_logistic_raw_probability":
        c = candidate.hyperparameters["C"]
        model = LogisticRegression(
            C=c,
            solver="lbfgs",
            max_iter=5000,
            random_state=SEED,
        )
        model.fit(p.reshape(-1, 1), y)
        params = {
            "intercept": float(model.intercept_[0]),
            "coefficient_raw_probability": float(model.coef_[0][0]),
        }
        return model, params

    if candidate.family == "logistic_logit_probability":
        c = candidate.hyperparameters["C"]
        model = LogisticRegression(
            C=c,
            solver="lbfgs",
            max_iter=5000,
            random_state=SEED,
        )
        x = logit(p).reshape(-1, 1)
        model.fit(x, y)
        params = {
            "intercept": float(model.intercept_[0]),
            "coefficient_logit_probability": float(model.coef_[0][0]),
        }
        return model, params

    if candidate.family == "beta_calibration":
        model = BetaCalibrator(candidate.hyperparameters["l2"]).fit(p, y)
        return model, model.fitted_parameters()

    if candidate.family == "isotonic_regression":
        class_counts = pd.Series(y).value_counts()
        if len(decisions) < ISOTONIC_MIN_TRAIN_DECISIONS:
            raise RuntimeError("isotonic_insufficient_train_rows")
        if class_counts.min() < ISOTONIC_MIN_CLASS_COUNT:
            raise RuntimeError("isotonic_insufficient_class_count")
        if len(np.unique(np.round(p, 12))) < ISOTONIC_MIN_UNIQUE_PROB:
            raise RuntimeError("isotonic_insufficient_unique_probabilities")
        model = IsotonicRegression(
            y_min=PROB_EPS,
            y_max=1.0 - PROB_EPS,
            out_of_bounds="clip",
        )
        model.fit(p, y)
        params = {
            "x_threshold_count": int(len(model.X_thresholds_)),
            "y_threshold_count": int(len(model.y_thresholds_)),
        }
        return model, params

    raise ValueError(candidate.family)


def predict_candidate(candidate: Candidate, model, df: pd.DataFrame) -> np.ndarray:
    p = df["raw_prob"].to_numpy(dtype=float)

    if candidate.family == "raw_baseline":
        return clip_probability(p)

    if candidate.family == "production_baseline":
        return clip_probability(df["production_prob"].to_numpy(dtype=float))

    if candidate.family == "platt_logistic_raw_probability":
        return clip_probability(model.predict_proba(p.reshape(-1, 1))[:, 1])

    if candidate.family == "logistic_logit_probability":
        return clip_probability(model.predict_proba(logit(p).reshape(-1, 1))[:, 1])

    if candidate.family == "beta_calibration":
        return clip_probability(model.predict(p))

    if candidate.family == "isotonic_regression":
        return clip_probability(model.predict(p))

    raise ValueError(candidate.family)


def calibration_intercept_slope(y, p) -> tuple[float, float]:
    y = np.asarray(y, dtype=float)
    p = clip_probability(p)
    x = logit(p)

    if len(np.unique(y)) < 2:
        return np.nan, np.nan

    def objective(theta):
        intercept, slope = theta
        q = np.clip(expit(intercept + slope * x), PROB_EPS, 1.0 - PROB_EPS)
        return float(-np.mean(y * np.log(q) + (1.0 - y) * np.log(1.0 - q)))

    result = minimize(
        objective,
        x0=np.array([0.0, 1.0], dtype=float),
        method="BFGS",
        options={"maxiter": 5000},
    )
    if not result.success or not np.all(np.isfinite(result.x)):
        return np.nan, np.nan
    return float(result.x[0]), float(result.x[1])


def reliability_rows(
    market: str,
    candidate_id: str,
    evaluation_period: str,
    y,
    p,
) -> list[dict]:
    y = np.asarray(y, dtype=float)
    p = clip_probability(p)
    edges = np.linspace(0.0, 1.0, RELIABILITY_BINS + 1)
    rows = []
    for i in range(RELIABILITY_BINS):
        low = edges[i]
        high = edges[i + 1]
        if i == RELIABILITY_BINS - 1:
            mask = (p >= low) & (p <= high)
        else:
            mask = (p >= low) & (p < high)
        n = int(mask.sum())
        rows.append(
            {
                "market": market,
                "candidate_id": candidate_id,
                "evaluation_period": evaluation_period,
                "bin_index": i + 1,
                "bin_low": low,
                "bin_high": high,
                "count": n,
                "mean_predicted_probability": float(p[mask].mean()) if n else np.nan,
                "observed_frequency": float(y[mask].mean()) if n else np.nan,
            }
        )
    return rows


def calibration_metrics(y, p) -> dict:
    y = np.asarray(y, dtype=float)
    p = clip_probability(p)
    if not len(y):
        return {
            "decision_count": 0,
            "brier_score": np.nan,
            "log_loss": np.nan,
            "ece": np.nan,
            "calibration_intercept": np.nan,
            "calibration_slope": np.nan,
        }

    brier = float(np.mean((p - y) ** 2))
    log_loss = float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))

    edges = np.linspace(0.0, 1.0, RELIABILITY_BINS + 1)
    ece = 0.0
    for i in range(RELIABILITY_BINS):
        low = edges[i]
        high = edges[i + 1]
        if i == RELIABILITY_BINS - 1:
            mask = (p >= low) & (p <= high)
        else:
            mask = (p >= low) & (p < high)
        if mask.any():
            ece += (mask.sum() / len(y)) * abs(float(p[mask].mean() - y[mask].mean()))

    intercept, slope = calibration_intercept_slope(y, p)

    return {
        "decision_count": int(len(y)),
        "brier_score": brier,
        "log_loss": log_loss,
        "ece": float(ece),
        "calibration_intercept": intercept,
        "calibration_slope": slope,
    }


def side_level_predictions(df: pd.DataFrame, canonical_pred: np.ndarray) -> pd.DataFrame:
    rows = []
    canonical_pred = clip_probability(canonical_pred)

    for record, p_canon in zip(df.to_dict("records"), canonical_pred):
        for is_canonical in [True, False]:
            side = record["canonical_side"] if is_canonical else record["opposite_side"]
            p = float(p_canon if is_canonical else 1.0 - p_canon)
            decimal_odds = float(
                record["canonical_decimal"] if is_canonical else record["opposite_decimal"]
            )
            american_odds = float(
                record["canonical_american"] if is_canonical else record["opposite_american"]
            )
            line = record["line"] if is_canonical else record["opposite_line"]

            if record["is_push"]:
                y = np.nan
                realized_profit = 0.0
                result = "Push"
            else:
                y_canon = float(record["y"])
                y = y_canon if is_canonical else 1.0 - y_canon
                if y == 1.0:
                    realized_profit = decimal_odds - 1.0
                    result = "Win"
                else:
                    realized_profit = -1.0
                    result = "Loss"

            break_even = 1.0 / decimal_odds
            edge = p - break_even

            if record["market"] == "total":
                no_push_prob = 1.0 - float(record["push_prob"])
                predicted_ev = no_push_prob * (p * decimal_odds - 1.0)
            else:
                predicted_ev = p * decimal_odds - 1.0

            b = decimal_odds - 1.0
            kelly = max(((b * p) - (1.0 - p)) / b, 0.0)
            fair_decimal = 1.0 / p

            actual_edge_proxy = np.nan if np.isnan(y) else y - break_even

            rows.append(
                {
                    "market": record["market"],
                    "game_id": record["game_id"],
                    "game_date": record["game_date"],
                    "side": side,
                    "line": line,
                    "probability": p,
                    "fair_decimal": fair_decimal,
                    "sportsbook_american": american_odds,
                    "sportsbook_decimal": decimal_odds,
                    "break_even_probability": break_even,
                    "predicted_edge": edge,
                    "predicted_ev": predicted_ev,
                    "kelly": kelly,
                    "push_probability": float(record["push_prob"]),
                    "result": result,
                    "y": y,
                    "actual_edge_proxy": actual_edge_proxy,
                    "realized_profit": realized_profit,
                }
            )

    return pd.DataFrame(rows)


def downstream_metrics(side_df: pd.DataFrame) -> dict:
    decisions = side_df[side_df["result"].isin(["Win", "Loss"])].copy()
    settled = side_df[side_df["result"].isin(["Win", "Loss", "Push"])].copy()

    if len(decisions):
        edge_error = decisions["predicted_edge"] - decisions["actual_edge_proxy"]
        edge_mae = float(edge_error.abs().mean())
        edge_bias = float(edge_error.mean())
    else:
        edge_mae = np.nan
        edge_bias = np.nan

    positive_edge = decisions[decisions["predicted_edge"] > 0].copy()
    positive_edge_hit_rate = (
        float((positive_edge["result"] == "Win").mean())
        if len(positive_edge)
        else np.nan
    )

    if len(settled):
        ev_error = settled["predicted_ev"] - settled["realized_profit"]
        ev_mae = float(ev_error.abs().mean())
        ev_bias = float(ev_error.mean())
    else:
        ev_mae = np.nan
        ev_bias = np.nan

    bets = settled[settled["predicted_ev"] > 0].copy()
    bet_count = len(bets)
    betting_profit = float(bets["realized_profit"].sum()) if bet_count else 0.0
    betting_roi = betting_profit / bet_count if bet_count else np.nan

    return {
        "side_decision_count": int(len(decisions)),
        "edge_mae": edge_mae,
        "edge_bias": edge_bias,
        "positive_edge_decisions": int(len(positive_edge)),
        "positive_edge_hit_rate": positive_edge_hit_rate,
        "ev_mae": ev_mae,
        "ev_bias": ev_bias,
        "positive_ev_bets": int(bet_count),
        "positive_ev_wins": int((bets["result"] == "Win").sum()) if bet_count else 0,
        "positive_ev_losses": int((bets["result"] == "Loss").sum()) if bet_count else 0,
        "positive_ev_pushes": int((bets["result"] == "Push").sum()) if bet_count else 0,
        "positive_ev_profit_units": betting_profit,
        "positive_ev_roi": betting_roi,
    }


def run_market_search(
    market: str,
    df: pd.DataFrame,
    final_start: pd.Timestamp,
    candidates: list[Candidate],
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    list[dict],
    Candidate,
]:
    dev = df[df["game_date"] < final_start].copy()
    final = df[df["game_date"] >= final_start].copy()

    if final.empty:
        raise RuntimeError(f"{market}: final chronological test is empty.")

    folds = expanding_folds(dev)
    validation_prediction_rows = []
    fold_metric_rows = []
    reliability = []

    for candidate in candidates:
        candidate_failed = False

        for fold in folds:
            train = fold["train"]
            val = fold["validation"]

            try:
                model, fitted_params = fit_candidate(candidate, train)
                predicted = predict_candidate(candidate, model, val)
            except Exception as exc:
                fold_metric_rows.append(
                    {
                        "market": market,
                        "candidate_id": candidate.candidate_id,
                        "family": candidate.family,
                        "feature_spec": candidate.feature_spec,
                        "hyperparameters": json.dumps(candidate.hyperparameters, sort_keys=True),
                        "fold": fold["fold"],
                        "status": "ineligible",
                        "failure_reason": str(exc),
                        "train_start": fold["train_start"],
                        "train_end": fold["train_end"],
                        "validation_start": fold["validation_start"],
                        "validation_end": fold["validation_end"],
                        "train_rows": len(train),
                        "validation_rows": len(val),
                        "train_decisions": int((~train["is_push"] & train["y"].notna()).sum()),
                        "validation_decisions": int((~val["is_push"] & val["y"].notna()).sum()),
                    }
                )
                candidate_failed = True
                break

            temp = val.copy()
            temp["predicted_probability"] = predicted
            temp["candidate_id"] = candidate.candidate_id
            temp["family"] = candidate.family
            temp["fold"] = fold["fold"]
            temp["evaluation_period"] = "development_validation"
            temp["train_start"] = fold["train_start"]
            temp["train_end"] = fold["train_end"]
            temp["validation_start"] = fold["validation_start"]
            temp["validation_end"] = fold["validation_end"]
            temp["fitted_parameters"] = json.dumps(fitted_params, sort_keys=True)
            validation_prediction_rows.append(temp)

            decision_mask = ~temp["is_push"] & temp["y"].notna()
            y = temp.loc[decision_mask, "y"].to_numpy(dtype=float)
            p = temp.loc[decision_mask, "predicted_probability"].to_numpy(dtype=float)
            cm = calibration_metrics(y, p)
            dm = downstream_metrics(side_level_predictions(temp, predicted))

            fold_metric_rows.append(
                {
                    "market": market,
                    "candidate_id": candidate.candidate_id,
                    "family": candidate.family,
                    "feature_spec": candidate.feature_spec,
                    "hyperparameters": json.dumps(candidate.hyperparameters, sort_keys=True),
                    "fold": fold["fold"],
                    "status": "ok",
                    "failure_reason": "",
                    "train_start": fold["train_start"],
                    "train_end": fold["train_end"],
                    "validation_start": fold["validation_start"],
                    "validation_end": fold["validation_end"],
                    "train_rows": len(train),
                    "validation_rows": len(val),
                    "train_decisions": int((~train["is_push"] & train["y"].notna()).sum()),
                    "validation_decisions": cm["decision_count"],
                    "fitted_parameters": json.dumps(fitted_params, sort_keys=True),
                    **cm,
                    **dm,
                }
            )

            reliability.extend(
                reliability_rows(
                    market,
                    candidate.candidate_id,
                    f"development_validation_fold_{fold['fold']}",
                    y,
                    p,
                )
            )

        if candidate_failed:
            continue

    if not validation_prediction_rows:
        raise RuntimeError(f"{market}: no validation predictions produced.")

    val_predictions = pd.concat(validation_prediction_rows, ignore_index=True)
    fold_metrics = pd.DataFrame(fold_metric_rows)

    expected_fold_count = len(folds)
    aggregate_rows = []

    for candidate in candidates:
        cdf = val_predictions[val_predictions["candidate_id"] == candidate.candidate_id].copy()
        completed_folds = cdf["fold"].nunique() if not cdf.empty else 0

        if completed_folds != expected_fold_count:
            aggregate_rows.append(
                {
                    "market": market,
                    "candidate_id": candidate.candidate_id,
                    "family": candidate.family,
                    "feature_spec": candidate.feature_spec,
                    "hyperparameters": json.dumps(candidate.hyperparameters, sort_keys=True),
                    "eligible_for_selection": False,
                    "folds_completed": int(completed_folds),
                    "required_folds": expected_fold_count,
                }
            )
            continue

        decision_mask = ~cdf["is_push"] & cdf["y"].notna()
        y = cdf.loc[decision_mask, "y"].to_numpy(dtype=float)
        p = cdf.loc[decision_mask, "predicted_probability"].to_numpy(dtype=float)
        cm = calibration_metrics(y, p)
        dm = downstream_metrics(
            side_level_predictions(cdf, cdf["predicted_probability"].to_numpy(dtype=float))
        )

        aggregate_rows.append(
            {
                "market": market,
                "candidate_id": candidate.candidate_id,
                "family": candidate.family,
                "feature_spec": candidate.feature_spec,
                "hyperparameters": json.dumps(candidate.hyperparameters, sort_keys=True),
                "eligible_for_selection": True,
                "folds_completed": int(completed_folds),
                "required_folds": expected_fold_count,
                **cm,
                **dm,
            }
        )

        reliability.extend(
            reliability_rows(
                market,
                candidate.candidate_id,
                "development_validation_aggregate",
                y,
                p,
            )
        )

    aggregate = pd.DataFrame(aggregate_rows)
    eligible = aggregate[aggregate["eligible_for_selection"] == True].copy()
    if eligible.empty:
        raise RuntimeError(f"{market}: no candidate completed all development folds.")

    eligible = eligible.sort_values(
        ["log_loss", "brier_score", "candidate_id"],
        ascending=[True, True, True],
    )
    winner_id = str(eligible.iloc[0]["candidate_id"])
    winner = next(c for c in candidates if c.candidate_id == winner_id)

    return aggregate, fold_metrics, val_predictions, reliability, winner


def final_test_evaluation(
    market: str,
    df: pd.DataFrame,
    final_start: pd.Timestamp,
    winner: Candidate,
    candidates: list[Candidate],
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict], dict]:
    dev = df[df["game_date"] < final_start].copy()
    final = df[df["game_date"] >= final_start].copy()

    comparator_ids = ["raw_baseline", "production_baseline", winner.candidate_id]
    comparator_ids = list(dict.fromkeys(comparator_ids))
    comparators = [next(c for c in candidates if c.candidate_id == cid) for cid in comparator_ids]

    metric_rows = []
    prediction_parts = []
    reliability = []
    winner_fit_params = {}

    for candidate in comparators:
        model, fitted_params = fit_candidate(candidate, dev)
        predicted = predict_candidate(candidate, model, final)

        if candidate.candidate_id == winner.candidate_id:
            winner_fit_params = fitted_params

        temp = final.copy()
        temp["candidate_id"] = candidate.candidate_id
        temp["family"] = candidate.family
        temp["predicted_probability"] = predicted
        temp["evaluation_period"] = "final_test"
        temp["fitted_parameters"] = json.dumps(fitted_params, sort_keys=True)
        prediction_parts.append(temp)

        decision_mask = ~temp["is_push"] & temp["y"].notna()
        y = temp.loc[decision_mask, "y"].to_numpy(dtype=float)
        p = temp.loc[decision_mask, "predicted_probability"].to_numpy(dtype=float)
        cm = calibration_metrics(y, p)

        side_df = side_level_predictions(temp, predicted)
        dm = downstream_metrics(side_df)

        metric_rows.append(
            {
                "market": market,
                "candidate_id": candidate.candidate_id,
                "family": candidate.family,
                "feature_spec": candidate.feature_spec,
                "hyperparameters": json.dumps(candidate.hyperparameters, sort_keys=True),
                "is_locked_winner": candidate.candidate_id == winner.candidate_id,
                "development_start": dev["game_date"].min(),
                "development_end": dev["game_date"].max(),
                "final_test_start": final["game_date"].min(),
                "final_test_end": final["game_date"].max(),
                "development_rows": len(dev),
                "final_test_rows": len(final),
                "fitted_parameters": json.dumps(fitted_params, sort_keys=True),
                **cm,
                **dm,
            }
        )

        reliability.extend(
            reliability_rows(
                market,
                candidate.candidate_id,
                "final_test",
                y,
                p,
            )
        )

    return (
        pd.DataFrame(metric_rows),
        pd.concat(prediction_parts, ignore_index=True),
        reliability,
        winner_fit_params,
    )


def serialize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if "date" in str(col).lower():
            if pd.api.types.is_datetime64_any_dtype(out[col]):
                out[col] = out[col].dt.strftime("%Y-%m-%d")
            else:
                out[col] = out[col].map(
                    lambda x: x.strftime("%Y-%m-%d") if isinstance(x, pd.Timestamp) else x
                )
    return out


def main() -> None:
    root = find_repo_root()
    output_dir = root / OUTPUT_REL
    output_dir.mkdir(parents=True, exist_ok=True)

    configs = read_current_configs(root)
    historical = load_historical_inputs(root)
    scores = load_final_scores(root)
    market_rows = build_market_rows(historical, scores, configs)

    # IMPORTANT: final chronological test is locked before any model search.
    final_start = determine_final_test_start(market_rows)

    all_games = pd.concat(
        [df[["game_id", "game_date"]] for df in market_rows.values()],
        ignore_index=True,
    ).drop_duplicates("game_id")
    all_games = all_games.sort_values(["game_date", "game_id"]).reset_index(drop=True)

    final_games = all_games[all_games["game_date"] >= final_start]
    dev_games = all_games[all_games["game_date"] < final_start]

    split_manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "research_only": True,
        "production_stage02_modified": False,
        "seed": SEED,
        "split_rule": "reserve final chronological 20% of unique games; keep complete cutoff date in final test",
        "requested_final_fraction": FINAL_TEST_FRACTION,
        "final_test_start": final_start.strftime("%Y-%m-%d"),
        "development_game_count": int(len(dev_games)),
        "final_test_game_count": int(len(final_games)),
        "development_start": dev_games["game_date"].min().strftime("%Y-%m-%d"),
        "development_end": dev_games["game_date"].max().strftime("%Y-%m-%d"),
        "final_test_end": final_games["game_date"].max().strftime("%Y-%m-%d"),
        "final_test_forbidden_for": [
            "model family selection",
            "feature selection",
            "hyperparameter selection",
            "calibration method selection",
            "disagreement threshold selection",
            "selection threshold selection",
        ],
    }

    # Write split lock before model selection begins.
    (output_dir / "split_manifest.json").write_text(
        json.dumps(split_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    candidates = candidate_grid()
    search_results_parts = []
    fold_metrics_parts = []
    validation_predictions_parts = []
    reliability_rows_all = []
    winners = {}

    for market in ["moneyline", "puck_line", "total"]:
        aggregate, fold_metrics, val_predictions, reliability, winner = run_market_search(
            market,
            market_rows[market],
            final_start,
            candidates,
        )
        search_results_parts.append(aggregate)
        fold_metrics_parts.append(fold_metrics)
        validation_predictions_parts.append(val_predictions)
        reliability_rows_all.extend(reliability)
        winners[market] = winner

    search_results = pd.concat(search_results_parts, ignore_index=True)
    fold_metrics = pd.concat(fold_metrics_parts, ignore_index=True)
    validation_predictions = pd.concat(validation_predictions_parts, ignore_index=True)

    final_metric_parts = []
    final_prediction_parts = []
    selected_rows = []

    for market in ["moneyline", "puck_line", "total"]:
        winner = winners[market]
        final_metrics, final_predictions, reliability, winner_fit_params = final_test_evaluation(
            market,
            market_rows[market],
            final_start,
            winner,
            candidates,
        )
        final_metric_parts.append(final_metrics)
        final_prediction_parts.append(final_predictions)
        reliability_rows_all.extend(reliability)

        market_df = market_rows[market]
        dev = market_df[market_df["game_date"] < final_start]
        final = market_df[market_df["game_date"] >= final_start]

        selected_rows.append(
            {
                "market": market,
                "locked_candidate_id": winner.candidate_id,
                "model_family": winner.family,
                "feature_specification": winner.feature_spec,
                "hyperparameters": json.dumps(winner.hyperparameters, sort_keys=True),
                "selection_metric_primary": "development expanding-window aggregate log_loss",
                "selection_metric_tiebreaker": "development expanding-window aggregate brier_score",
                "development_start": dev["game_date"].min(),
                "development_end": dev["game_date"].max(),
                "final_test_start": final["game_date"].min(),
                "final_test_end": final["game_date"].max(),
                "final_test_used_for_selection": False,
                "refit_training_rows": len(dev),
                "refit_training_decisions": int((~dev["is_push"] & dev["y"].notna()).sum()),
                "final_test_rows": len(final),
                "final_test_decisions": int((~final["is_push"] & final["y"].notna()).sum()),
                "locked_winner_fitted_parameters": json.dumps(
                    winner_fit_params, sort_keys=True
                ),
                "random_seed": SEED,
            }
        )

    final_metrics = pd.concat(final_metric_parts, ignore_index=True)
    final_predictions = pd.concat(final_prediction_parts, ignore_index=True)
    selected_models = pd.DataFrame(selected_rows)
    reliability_df = pd.DataFrame(reliability_rows_all)

    versions = {
        "python": platform.python_version(),
        "numpy": package_version("numpy"),
        "pandas": package_version("pandas"),
        "scipy": package_version("scipy"),
        "scikit-learn": package_version("scikit-learn"),
    }

    run_manifest = {
        **split_manifest,
        "package_versions": versions,
        "candidate_families": sorted(set(c.family for c in candidates)),
        "candidate_count": len(candidates),
        "validation_folds": N_VALIDATION_FOLDS,
        "initial_train_date_fraction": INITIAL_TRAIN_DATE_FRACTION,
        "logistic_C_grid": LOGISTIC_C_GRID,
        "beta_l2_grid": BETA_L2_GRID,
        "isotonic_requirements": {
            "min_train_decisions": ISOTONIC_MIN_TRAIN_DECISIONS,
            "min_class_count": ISOTONIC_MIN_CLASS_COUNT,
            "min_unique_probability": ISOTONIC_MIN_UNIQUE_PROB,
        },
        "reliability_bins": RELIABILITY_BINS,
        "betting_evaluation_rule": "predicted_ev > 0; fixed before final test; no threshold search",
        "calibration_fit_unit": "one canonical side per game: home for moneyline/puck_line, over for total",
        "opposite_probability_rule": "1 - calibrated canonical probability",
        "total_probability_definition": "conditional on no-push; push probability retained separately for actual EV",
        "production_adjustment_formula": [
            "fair_decimal = 1 / raw_probability",
            "adjusted_decimal = fair_decimal * (1 - model_calibration_adjustment)",
            "adjusted_probability_raw = 1 / adjusted_decimal",
            "opposing adjusted probabilities normalized to sum to 1",
        ],
        "selection_rule": "lowest development expanding-window aggregate log loss; Brier score tiebreaker",
        "final_test_comparators": [
            "raw_baseline",
            "production_baseline",
            "locked_market_winner",
        ],
    }

    # Persist artifacts.
    serialize_dates(search_results).to_csv(output_dir / "search_results.csv", index=False)
    serialize_dates(fold_metrics).to_csv(output_dir / "fold_metrics.csv", index=False)

    val_cols = [
        "market", "game_id", "game_date", "candidate_id", "family", "fold",
        "evaluation_period", "raw_prob", "production_prob", "predicted_probability",
        "y", "is_push", "push_prob", "canonical_decimal", "opposite_decimal",
        "line", "train_start", "train_end", "validation_start", "validation_end",
        "fitted_parameters", "source_input_file", "source_score_file",
    ]
    serialize_dates(validation_predictions[val_cols]).to_csv(
        output_dir / "validation_predictions.csv", index=False
    )

    final_cols = [
        "market", "game_id", "game_date", "candidate_id", "family",
        "evaluation_period", "raw_prob", "production_prob", "predicted_probability",
        "y", "is_push", "push_prob", "canonical_decimal", "opposite_decimal",
        "line", "fitted_parameters", "source_input_file", "source_score_file",
    ]
    serialize_dates(final_predictions[final_cols]).to_csv(
        output_dir / "final_test_predictions.csv", index=False
    )

    serialize_dates(final_metrics).to_csv(output_dir / "final_test_metrics.csv", index=False)
    serialize_dates(selected_models).to_csv(output_dir / "selected_models.csv", index=False)
    reliability_df.to_csv(output_dir / "reliability_curves.csv", index=False)

    # Save the research-ready source rows so market coverage is inspectable.
    source_rows = pd.concat(
        [df.assign(market_name=market) for market, df in market_rows.items()],
        ignore_index=True,
    )
    serialize_dates(source_rows).to_csv(output_dir / "research_dataset.csv", index=False)

    (output_dir / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    locked_summary = final_metrics[final_metrics["is_locked_winner"] == True].copy()
    summary_lines = [
        "NHL RESEARCH-ONLY PROBABILITY CALIBRATION SEARCH",
        "================================================",
        "research_only=true",
        "production_stage02_modified=false",
        f"seed={SEED}",
        f"development_start={split_manifest['development_start']}",
        f"development_end={split_manifest['development_end']}",
        f"final_test_start={split_manifest['final_test_start']}",
        f"final_test_end={split_manifest['final_test_end']}",
        f"development_games={split_manifest['development_game_count']}",
        f"final_test_games={split_manifest['final_test_game_count']}",
        f"candidate_count={len(candidates)}",
        f"validation_folds={N_VALIDATION_FOLDS}",
        "selection_rule=lowest development expanding-window log loss; Brier tiebreaker",
        "final_test_used_for_selection=false",
        "betting_rule=predicted_ev > 0 (fixed; not tuned)",
        "",
        "LOCKED MARKET WINNERS",
    ]
    for row in selected_models.to_dict("records"):
        summary_lines.append(
            f"{row['market']}: {row['locked_candidate_id']} | "
            f"family={row['model_family']} | hyperparameters={row['hyperparameters']}"
        )

    summary_lines += ["", "LOCKED WINNER FINAL-TEST METRICS"]
    for row in locked_summary.to_dict("records"):
        summary_lines.append(
            f"{row['market']}: "
            f"Brier={row['brier_score']:.6f} | "
            f"log_loss={row['log_loss']:.6f} | "
            f"ECE={row['ece']:.6f} | "
            f"cal_intercept={row['calibration_intercept']:.6f} | "
            f"cal_slope={row['calibration_slope']:.6f} | "
            f"edge_MAE={row['edge_mae']:.6f} | "
            f"EV_MAE={row['ev_mae']:.6f} | "
            f"positive_EV_bets={int(row['positive_ev_bets'])} | "
            f"ROI={row['positive_ev_roi']:.6f}"
        )

    summary_lines += [
        "",
        "Artifacts:",
        str(output_dir / "split_manifest.json"),
        str(output_dir / "run_manifest.json"),
        str(output_dir / "research_dataset.csv"),
        str(output_dir / "fold_metrics.csv"),
        str(output_dir / "search_results.csv"),
        str(output_dir / "validation_predictions.csv"),
        str(output_dir / "selected_models.csv"),
        str(output_dir / "final_test_predictions.csv"),
        str(output_dir / "final_test_metrics.csv"),
        str(output_dir / "reliability_curves.csv"),
    ]

    (output_dir / "summary.txt").write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8",
    )

    print("PROBABILITY CALIBRATION SEARCH COMPLETE")
    print(f"Output: {output_dir}")
    print(f"Final test starts: {split_manifest['final_test_start']}")
    print()
    print(selected_models[[
        "market", "locked_candidate_id", "model_family",
        "development_start", "development_end",
        "final_test_start", "final_test_end"
    ]].to_string(index=False))
    print()
    print("Locked-winner final-test metrics:")
    print(locked_summary[[
        "market", "candidate_id", "brier_score", "log_loss", "ece",
        "calibration_intercept", "calibration_slope",
        "edge_mae", "ev_mae", "positive_ev_bets", "positive_ev_roi"
    ]].to_string(index=False))


if __name__ == "__main__":
    main()
