#!/usr/bin/env python3
"""
Research-only audit/optimization of the NHL high-disagreement gate.

Scope:
- Reproduce point-in-time probability, margin, and total disagreement.
- Reproduce the current prior-history 75th-percentile thresholds.
- Test prior-history quantiles 60/70/75/80/85/90.
- Compare:
    primary_only
    reject_high_disagreement
    require_sdv_support
    require_weighted_support
    require_meta_support
    price_aware_positive_ev_secondary
    continuous_disagreement
- Evaluate Brier, log loss, calibration/ECE, ROI, bet count,
  maximum drawdown, EV buckets, and CLV only if an explicit CLV/closing field exists.
- Lock all threshold/strategy choices on development validation only.
- Evaluate the untouched final chronological holdout afterward.

The candidate pool intentionally isolates the secondary gate:
    primary model side probability > 1 / offered decimal odds

This script does not read markets.yaml and does not modify production files.
"""

from __future__ import annotations

import importlib.util
import json
import math
import platform
import sys
import warnings
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore", category=PerformanceWarning)

NHL_REL = Path("docs/win/hockey/nhl")
STAGE03_REL = NHL_REL / "scripts/03_edges/build_secondary_model_signals.py"
STAGE04_REL = NHL_REL / "scripts/04_select/hockey_select_bets.py"
HISTORY_REL = NHL_REL / "research/sdv_challenger"
EDGE_ARCHIVE_REL = NHL_REL / "archive/2025_26/03_edges"
OUTPUT_REL = NHL_REL / "research/high_disagreement_gate_audit"

SEED = 20260912
MIN_PRIOR_ROWS = 100
FINAL_TEST_FRACTION = 0.20
N_FOLDS = 4
INITIAL_TRAIN_DATE_FRACTION = 0.40
MIN_FOLD_TRAIN_CANDIDATES = 100
MIN_FOLD_VALIDATION_CANDIDATES = 1
EPS = 1e-12

QUANTILES = (0.60, 0.70, 0.75, 0.80, 0.85, 0.90)

MARKETS = ("moneyline", "puck_line", "total")
DERIVED_BY_MARKET = {
    "moneyline": "weighted",
    "puck_line": "meta",
    "total": "meta",
}

BINARY_STRATEGIES = (
    "reject_high_disagreement",
    "require_sdv_support",
    "require_weighted_support",
    "require_meta_support",
    "price_aware_positive_ev_secondary",
)

ALL_STRATEGIES = (
    "primary_only",
    *BINARY_STRATEGIES,
    "continuous_disagreement",
)

EV_BUCKETS = (
    (-float("inf"), 0.02, "<=2%"),
    (0.02, 0.05, "2-5%"),
    (0.05, 0.10, "5-10%"),
    (0.10, 0.20, "10-20%"),
    (0.20, float("inf"), ">20%"),
)


@dataclass(frozen=True)
class ContinuousGateModel:
    market_type: str
    model: Pipeline


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


def load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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


def logit(value: float) -> float:
    p = float(np.clip(value, 1e-6, 1.0 - 1e-6))
    return math.log(p / (1.0 - p))


def load_history(root: Path) -> pd.DataFrame:
    files = sorted(
        (root / HISTORY_REL).glob("season_*/standalone_comparison.csv")
    )
    if not files:
        raise RuntimeError(f"No historical comparison files under {root / HISTORY_REL}")

    required = [
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

    parts = []
    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing required history columns: {missing}")
        part = df[required].copy()
        part["_source_file"] = str(path.relative_to(root))
        parts.append(part)

    out = pd.concat(parts, ignore_index=True)
    out["game_id"] = out["game_id"].map(canonical_game_id)
    out["game_date"] = out["game_date"].map(parse_date)
    out["_date"] = out["game_date"].dt.date

    numeric = [c for c in required if c not in {"game_id", "game_date"}]
    for col in numeric:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(
        subset=["game_id", "game_date", "_date", *numeric]
    ).copy()
    out = out[out["game_id"].ne("")].copy()
    out = out.sort_values(["game_date", "game_id", "_source_file"])
    out = out.drop_duplicates("game_id", keep="last")
    return out.reset_index(drop=True)


def market_spec(market_type: str) -> dict[str, Any]:
    if market_type == "moneyline":
        return {
            "glob": "*_NHL_moneyline.csv",
            "line_col": None,
            "side_specs": {
                "home": (
                    "home_model_prob_moneyline",
                    "home_dk_moneyline_decimal",
                    None,
                ),
                "away": (
                    "away_model_prob_moneyline",
                    "away_dk_moneyline_decimal",
                    None,
                ),
            },
        }

    if market_type == "puck_line":
        return {
            "glob": "*_NHL_puck_line.csv",
            "line_col": None,
            "side_specs": {
                "home": (
                    "home_model_prob_puck_line",
                    "home_dk_puck_line_decimal",
                    "home_puck_line",
                ),
                "away": (
                    "away_model_prob_puck_line",
                    "away_dk_puck_line_decimal",
                    "away_puck_line",
                ),
            },
        }

    if market_type == "total":
        return {
            "glob": "*_NHL_total.csv",
            "line_col": "total",
            "side_specs": {
                "over": (
                    "over_model_prob_total",
                    "dk_total_over_decimal",
                    "total",
                ),
                "under": (
                    "under_model_prob_total",
                    "dk_total_under_decimal",
                    "total",
                ),
            },
        }

    raise ValueError(market_type)


def load_market_archive(
    root: Path,
    market_type: str,
) -> tuple[pd.DataFrame, list[str]]:
    spec = market_spec(market_type)
    files = sorted((root / EDGE_ARCHIVE_REL).glob(spec["glob"]))
    if not files:
        raise RuntimeError(
            f"No archived {market_type} edge files under {root / EDGE_ARCHIVE_REL}"
        )

    needed = {"game_id", "game_date"}
    for probability_col, odds_col, line_col in spec["side_specs"].values():
        needed.add(probability_col)
        needed.add(odds_col)
        if line_col:
            needed.add(line_col)

    parts = []
    detected_clv: set[str] = set()

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        if df.empty:
            continue

        missing = [c for c in needed if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing archived columns: {missing}")

        clv_columns = [
            c
            for c in df.columns
            if (
                "clv" in c.lower()
                or "closing" in c.lower()
                or c.lower().startswith("close_")
                or c.lower().endswith("_close")
            )
        ]
        detected_clv.update(clv_columns)

        keep = list(needed) + clv_columns
        part = df[keep].copy()
        part["_edge_source_file"] = path.name
        parts.append(part)

    if not parts:
        raise RuntimeError(f"No non-empty archived {market_type} files")

    out = pd.concat(parts, ignore_index=True)
    out["game_id"] = out["game_id"].map(canonical_game_id)
    out["game_date"] = out["game_date"].map(parse_date)
    out = out.dropna(subset=["game_id", "game_date"]).copy()
    out = out[out["game_id"].ne("")].copy()

    value_columns = sorted(needed - {"game_id", "game_date"})
    for col in value_columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    duplicates = out[out.duplicated("game_id", keep=False)]
    if not duplicates.empty:
        compare_cols = value_columns
        conflicting = []
        for game_id, group in duplicates.groupby("game_id"):
            if len(group[compare_cols].drop_duplicates()) > 1:
                conflicting.append(game_id)
        if conflicting:
            raise RuntimeError(
                f"Conflicting archived {market_type} rows for game_id: "
                f"{conflicting[:20]}"
            )
        out = out.drop_duplicates("game_id", keep="first")

    return out.reset_index(drop=True), sorted(detected_clv)


def actual_result(
    market_type: str,
    side: str,
    *,
    actual_home_win: float,
    actual_margin: float,
    actual_total: float,
    line: float | None,
) -> tuple[float | None, str]:
    if market_type == "moneyline":
        if side == "home":
            return float(actual_home_win), "decision"
        return float(1.0 - actual_home_win), "decision"

    if line is None or not np.isfinite(line):
        return None, "missing"

    if market_type == "puck_line":
        cover_margin = (
            actual_margin + line
            if side == "home"
            else -actual_margin + line
        )
        if abs(cover_margin) < 1e-12:
            return None, "push"
        return float(cover_margin > 0), "decision"

    if market_type == "total":
        difference = actual_total - line
        if abs(difference) < 1e-12:
            return None, "push"
        if side == "over":
            return float(difference > 0), "decision"
        return float(difference < 0), "decision"

    raise ValueError(market_type)


def build_primary_candidates(
    history: pd.DataFrame,
    market_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    history_actual = history[
        [
            "game_id",
            "game_date",
            "actual_home_win",
            "actual_margin",
            "actual_total",
        ]
    ].copy()

    rows = []

    for market_type in MARKETS:
        market = market_frames[market_type].merge(
            history_actual,
            on="game_id",
            how="inner",
            suffixes=("_edge", "_history"),
            validate="one_to_one",
        )

        if "game_date_history" in market.columns:
            market["game_date"] = market["game_date_history"]
        elif "game_date" not in market.columns:
            market["game_date"] = market["game_date_edge"]

        spec = market_spec(market_type)

        for record in market.to_dict("records"):
            for side, (
                probability_col,
                decimal_col,
                line_col,
            ) in spec["side_specs"].items():
                probability = to_float(record.get(probability_col))
                decimal_odds = to_float(record.get(decimal_col))
                line = (
                    to_float(record.get(line_col))
                    if line_col
                    else np.nan
                )

                if (
                    not np.isfinite(probability)
                    or not 0.0 < probability < 1.0
                    or not np.isfinite(decimal_odds)
                    or decimal_odds <= 1.0
                ):
                    continue

                break_even = 1.0 / decimal_odds
                primary_ev = probability * decimal_odds - 1.0
                if primary_ev <= 0.0:
                    continue

                outcome, result_type = actual_result(
                    market_type,
                    side,
                    actual_home_win=float(record["actual_home_win"]),
                    actual_margin=float(record["actual_margin"]),
                    actual_total=float(record["actual_total"]),
                    line=(line if np.isfinite(line) else None),
                )

                if result_type == "decision":
                    profit = (
                        decimal_odds - 1.0
                        if outcome == 1.0
                        else -1.0
                    )
                elif result_type == "push":
                    profit = 0.0
                else:
                    continue

                rows.append(
                    {
                        "game_id": record["game_id"],
                        "game_date": pd.Timestamp(record["game_date"]),
                        "market_type": market_type,
                        "bet_side": side,
                        "line": line,
                        "primary_probability": probability,
                        "decimal_odds": decimal_odds,
                        "break_even_probability": break_even,
                        "primary_ev": primary_ev,
                        "actual_result": outcome,
                        "result_type": result_type,
                        "realized_profit": profit,
                    }
                )

    candidates = pd.DataFrame(rows)
    if candidates.empty:
        raise RuntimeError("No positive-EV primary candidates were reconstructed.")

    return candidates.sort_values(
        ["game_date", "game_id", "market_type", "bet_side"]
    ).reset_index(drop=True)


def source_train_predictions(
    stage03,
    source: str,
    train: pd.DataFrame,
    bundle,
    target: str,
) -> np.ndarray:
    if target == "margin":
        if source == "sdv":
            return train["sdv_exp_margin"].to_numpy(float)
        if source == "weighted":
            return (
                bundle.margin_weight
                * train["drat_exp_margin"].to_numpy(float)
                + (1.0 - bundle.margin_weight)
                * train["sdv_exp_margin"].to_numpy(float)
            )
        disagreement = (
            train["sdv_exp_margin"] - train["drat_exp_margin"]
        ).abs().to_numpy(float)
        return stage03.apply_linear(
            bundle.margin_meta,
            np.column_stack(
                [
                    train["drat_exp_margin"].to_numpy(float),
                    train["sdv_exp_margin"].to_numpy(float),
                    disagreement,
                ]
            ),
        )

    if target == "total":
        if source == "sdv":
            return train["sdv_exp_total"].to_numpy(float)
        if source == "weighted":
            return (
                bundle.total_weight
                * train["drat_exp_total"].to_numpy(float)
                + (1.0 - bundle.total_weight)
                * train["sdv_exp_total"].to_numpy(float)
            )
        disagreement = (
            train["sdv_exp_total"] - train["drat_exp_total"]
        ).abs().to_numpy(float)
        return stage03.apply_linear(
            bundle.total_meta,
            np.column_stack(
                [
                    train["drat_exp_total"].to_numpy(float),
                    train["sdv_exp_total"].to_numpy(float),
                    disagreement,
                ]
            ),
        )

    raise ValueError(target)


def empirical_puck_home_cover(
    predicted_margin: float,
    home_line: float,
    residuals: np.ndarray,
) -> float:
    if not np.isfinite(predicted_margin) or not np.isfinite(home_line):
        return np.nan
    if abs(home_line - round(home_line)) < 1e-9:
        return np.nan

    residuals = np.asarray(residuals, dtype=float)
    residuals = residuals[np.isfinite(residuals)]
    if residuals.size == 0:
        return np.nan

    threshold = -home_line - predicted_margin
    return float(
        (np.sum(residuals > threshold) + 0.5)
        / (residuals.size + 1.0)
    )


def empirical_total_probabilities(
    predicted_total: float,
    line: float,
    residuals: np.ndarray,
) -> tuple[float, float, float]:
    if not np.isfinite(predicted_total) or not np.isfinite(line):
        return np.nan, np.nan, np.nan

    residuals = np.asarray(residuals, dtype=float)
    residuals = residuals[np.isfinite(residuals)]
    if residuals.size == 0:
        return np.nan, np.nan, np.nan

    simulated_total = np.rint(
        np.maximum(0.0, predicted_total + residuals)
    )

    under_count = np.sum(simulated_total < line)
    push_count = np.sum(simulated_total == line)
    over_count = np.sum(simulated_total > line)

    probabilities = (
        np.array(
            [under_count, push_count, over_count],
            dtype=float,
        )
        + 0.5
    ) / (len(simulated_total) + 1.5)

    if abs(line - round(line)) >= 1e-9:
        probabilities[1] = 0.0
        decision_sum = probabilities[0] + probabilities[2]
        if decision_sum <= 0:
            return np.nan, np.nan, np.nan
        probabilities[0] /= decision_sum
        probabilities[2] /= decision_sum

    probabilities /= probabilities.sum()
    return (
        float(probabilities[0]),
        float(probabilities[1]),
        float(probabilities[2]),
    )


def conditional_side_probability(
    market_type: str,
    side: str,
    *,
    home_probability: float | None = None,
    under_probability: float | None = None,
    push_probability: float | None = None,
    over_probability: float | None = None,
) -> float:
    if market_type == "moneyline":
        if home_probability is None or not np.isfinite(home_probability):
            return np.nan
        return float(
            home_probability if side == "home" else 1.0 - home_probability
        )

    if market_type == "puck_line":
        if home_probability is None or not np.isfinite(home_probability):
            return np.nan
        return float(
            home_probability if side == "home" else 1.0 - home_probability
        )

    if market_type == "total":
        values = [under_probability, push_probability, over_probability]
        if any(v is None or not np.isfinite(v) for v in values):
            return np.nan
        no_push = 1.0 - float(push_probability)
        if no_push <= EPS:
            return np.nan
        win = (
            float(over_probability)
            if side == "over"
            else float(under_probability)
        )
        return win / no_push

    raise ValueError(market_type)


def prepare_point_in_time_features(
    stage03,
    stage04,
    history: pd.DataFrame,
    candidates: pd.DataFrame,
    market_frames: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    history_by_id = history.set_index("game_id", drop=False)

    puck_lines = market_frames["puck_line"][
        ["game_id", "home_puck_line", "away_puck_line"]
    ].copy()
    puck_lines["home_puck_line"] = pd.to_numeric(
        puck_lines["home_puck_line"], errors="coerce"
    )
    puck_lines["away_puck_line"] = pd.to_numeric(
        puck_lines["away_puck_line"], errors="coerce"
    )
    puck_line_lookup = puck_lines.set_index("game_id").to_dict("index")

    unique_games = (
        candidates[["game_id", "game_date"]]
        .drop_duplicates("game_id")
        .sort_values(["game_date", "game_id"])
    )

    feature_rows = []
    threshold_rows = []

    for game in unique_games.to_dict("records"):
        game_id = game["game_id"]
        target_timestamp = pd.Timestamp(game["game_date"])
        target_day = target_timestamp.date()

        if game_id not in history_by_id.index:
            continue

        hist_target = history_by_id.loc[game_id]
        if isinstance(hist_target, pd.DataFrame):
            hist_target = hist_target.iloc[-1]

        prior = history[history["_date"] < target_day].copy()

        base_common = {
            "drat_home_win_prob": float(hist_target["drat_home_win_prob"]),
            "drat_exp_margin": float(hist_target["drat_exp_margin"]),
            "drat_exp_total": float(hist_target["drat_exp_total"]),
            "sdv_home_win_prob": float(hist_target["sdv_home_win_prob"]),
            "sdv_exp_margin": float(hist_target["sdv_exp_margin"]),
            "sdv_exp_total": float(hist_target["sdv_exp_total"]),
        }

        puck_record = puck_line_lookup.get(game_id, {})
        base_common["home_puck_line"] = to_float(
            puck_record.get("home_puck_line")
        )
        base_common["away_puck_line"] = to_float(
            puck_record.get("away_puck_line")
        )

        prob_disagreement = abs(
            base_common["sdv_home_win_prob"]
            - base_common["drat_home_win_prob"]
        )
        margin_disagreement = abs(
            base_common["sdv_exp_margin"]
            - base_common["drat_exp_margin"]
        )
        total_disagreement = abs(
            base_common["sdv_exp_total"]
            - base_common["drat_exp_total"]
        )

        row = {
            "game_id": game_id,
            "game_date": target_timestamp,
            "prob_disagreement": prob_disagreement,
            "margin_disagreement": margin_disagreement,
            "total_disagreement": total_disagreement,
            "secondary_status": "insufficient_history",
        }

        for q in QUANTILES:
            q_tag = f"q{int(round(q * 100))}"
            if len(prior) >= MIN_PRIOR_ROWS:
                row[f"prob_threshold_{q_tag}"] = float(
                    (
                        prior["sdv_home_win_prob"]
                        - prior["drat_home_win_prob"]
                    ).abs().quantile(q)
                )
                row[f"margin_threshold_{q_tag}"] = float(
                    (
                        prior["sdv_exp_margin"]
                        - prior["drat_exp_margin"]
                    ).abs().quantile(q)
                )
                row[f"total_threshold_{q_tag}"] = float(
                    (
                        prior["sdv_exp_total"]
                        - prior["drat_exp_total"]
                    ).abs().quantile(q)
                )
            else:
                row[f"prob_threshold_{q_tag}"] = np.nan
                row[f"margin_threshold_{q_tag}"] = np.nan
                row[f"total_threshold_{q_tag}"] = np.nan

        bundle = stage03.fit_bundle_for_target(
            history,
            target_day,
            min_train_rows=MIN_PRIOR_ROWS,
            disagreement_quantile=0.75,
        )

        if bundle is None:
            feature_rows.append(row)
            continue

        signal = stage03.signal_row(
            pd.Series(base_common),
            bundle,
            not history.empty,
        )
        row["secondary_status"] = signal.get(
            "secondary_model_status",
            "ready",
        )

        for key in (
            "sdv_home_win_prob",
            "weighted_home_win_prob",
            "meta_home_win_prob",
            "sdv_exp_margin",
            "weighted_exp_margin",
            "meta_exp_margin",
            "sdv_exp_total",
            "weighted_exp_total",
            "meta_exp_total",
        ):
            row[key] = to_float(signal.get(key))

        threshold_rows.append(
            {
                "game_id": game_id,
                "game_date": target_timestamp,
                "prior_rows": len(prior),
                "calculated_prob_p75": row["prob_threshold_q75"],
                "bundle_prob_p75": float(bundle.prob_threshold),
                "prob_abs_diff": abs(
                    row["prob_threshold_q75"] - float(bundle.prob_threshold)
                ),
                "calculated_margin_p75": row["margin_threshold_q75"],
                "bundle_margin_p75": float(bundle.margin_threshold),
                "margin_abs_diff": abs(
                    row["margin_threshold_q75"] - float(bundle.margin_threshold)
                ),
                "calculated_total_p75": row["total_threshold_q75"],
                "bundle_total_p75": float(bundle.total_threshold),
                "total_abs_diff": abs(
                    row["total_threshold_q75"] - float(bundle.total_threshold)
                ),
            }
        )

        train_margin_predictions: dict[str, np.ndarray] = {}
        train_total_predictions: dict[str, np.ndarray] = {}

        for source in ("sdv", "weighted", "meta"):
            train_margin_predictions[source] = source_train_predictions(
                stage03,
                source,
                prior,
                bundle,
                "margin",
            )
            train_total_predictions[source] = source_train_predictions(
                stage03,
                source,
                prior,
                bundle,
                "total",
            )

            margin_residuals = (
                prior["actual_margin"].to_numpy(float)
                - train_margin_predictions[source]
            )
            total_residuals = (
                prior["actual_total"].to_numpy(float)
                - train_total_predictions[source]
            )

            target_margin = row[f"{source}_exp_margin"]
            target_total = row[f"{source}_exp_total"]

            home_puck_line = base_common["home_puck_line"]
            row[f"{source}_home_cover_prob_puck"] = empirical_puck_home_cover(
                target_margin,
                home_puck_line,
                margin_residuals,
            )

            total_line_candidates = candidates[
                (candidates["game_id"] == game_id)
                & (candidates["market_type"] == "total")
            ]
            if total_line_candidates.empty:
                row[f"{source}_under_prob_total"] = np.nan
                row[f"{source}_push_prob_total"] = np.nan
                row[f"{source}_over_prob_total"] = np.nan
            else:
                total_line = float(total_line_candidates.iloc[0]["line"])
                (
                    under_probability,
                    push_probability,
                    over_probability,
                ) = empirical_total_probabilities(
                    target_total,
                    total_line,
                    total_residuals,
                )
                row[f"{source}_under_prob_total"] = under_probability
                row[f"{source}_push_prob_total"] = push_probability
                row[f"{source}_over_prob_total"] = over_probability

        feature_rows.append(row)

    features = pd.DataFrame(feature_rows)

    enriched = candidates.merge(
        features,
        on=["game_id", "game_date"],
        how="left",
        validate="many_to_one",
    )

    for index, candidate in enriched.iterrows():
        market_type = candidate["market_type"]
        side = candidate["bet_side"]
        line = candidate["line"]
        decimal_odds = candidate["decimal_odds"]
        break_even = candidate["break_even_probability"]

        if market_type == "moneyline":
            disagreement = candidate["prob_disagreement"]
        elif market_type == "puck_line":
            disagreement = candidate["margin_disagreement"]
        else:
            disagreement = candidate["total_disagreement"]

        enriched.at[index, "relevant_disagreement"] = disagreement

        for source in ("sdv", "weighted", "meta"):
            if market_type == "moneyline":
                prediction_for_production = candidate[
                    f"{source}_home_win_prob"
                ]
                explicit_side_probability = conditional_side_probability(
                    market_type,
                    side,
                    home_probability=prediction_for_production,
                )
            elif market_type == "puck_line":
                prediction_for_production = candidate[
                    f"{source}_home_cover_prob_puck"
                ]
                explicit_side_probability = conditional_side_probability(
                    market_type,
                    side,
                    home_probability=prediction_for_production,
                )
            else:
                prediction_for_production = candidate[
                    f"{source}_exp_total"
                ]
                explicit_side_probability = conditional_side_probability(
                    market_type,
                    side,
                    under_probability=candidate[
                        f"{source}_under_prob_total"
                    ],
                    push_probability=candidate[
                        f"{source}_push_prob_total"
                    ],
                    over_probability=candidate[
                        f"{source}_over_prob_total"
                    ],
                )

            try:
                support = stage04.support_label(
                    market_type=market_type,
                    bet_side=side,
                    prediction=prediction_for_production,
                    line=line,
                    decimal_odds=decimal_odds,
                )
            except Exception:
                support = "unavailable"

            enriched.at[index, f"{source}_production_support"] = support
            enriched.at[
                index,
                f"{source}_explicit_side_probability",
            ] = explicit_side_probability
            enriched.at[
                index,
                f"{source}_price_margin",
            ] = (
                explicit_side_probability - break_even
                if np.isfinite(explicit_side_probability)
                else np.nan
            )

        derived = DERIVED_BY_MARKET[market_type]
        sdv_margin = enriched.at[index, "sdv_price_margin"]
        derived_margin = enriched.at[index, f"{derived}_price_margin"]
        values = [
            value
            for value in (sdv_margin, derived_margin)
            if np.isfinite(value)
        ]
        enriched.at[
            index,
            "configured_secondary_price_margin",
        ] = max(values) if values else np.nan
        enriched.at[
            index,
            "configured_price_aware_support",
        ] = bool(values and max(values) > 0.0)

    return enriched, pd.DataFrame(threshold_rows)


def relevant_threshold(row: pd.Series, quantile: float) -> float:
    q_tag = f"q{int(round(quantile * 100))}"
    market_type = row["market_type"]
    if market_type == "moneyline":
        return to_float(row[f"prob_threshold_{q_tag}"])
    if market_type == "puck_line":
        return to_float(row[f"margin_threshold_{q_tag}"])
    return to_float(row[f"total_threshold_{q_tag}"])


def production_support_for_strategy(
    row: pd.Series,
    strategy: str,
) -> bool:
    if strategy == "require_sdv_support":
        return row.get("sdv_production_support") == "supports"
    if strategy == "require_weighted_support":
        return row.get("weighted_production_support") == "supports"
    if strategy == "require_meta_support":
        return row.get("meta_production_support") == "supports"
    if strategy == "price_aware_positive_ev_secondary":
        return bool(row.get("configured_price_aware_support", False))
    raise ValueError(strategy)


def apply_binary_strategy(
    candidates: pd.DataFrame,
    strategy: str,
    quantile: float,
) -> pd.DataFrame:
    kept_rows = []

    for _, row in candidates.iterrows():
        threshold = relevant_threshold(row, quantile)
        disagreement = to_float(row["relevant_disagreement"])

        if not np.isfinite(threshold) or not np.isfinite(disagreement):
            kept_rows.append(row)
            continue

        high = disagreement >= threshold

        if not high:
            kept_rows.append(row)
            continue

        if strategy == "reject_high_disagreement":
            continue

        if production_support_for_strategy(row, strategy):
            kept_rows.append(row)

    if not kept_rows:
        return candidates.iloc[0:0].copy()

    return pd.DataFrame(kept_rows).reset_index(drop=True)


def continuous_features(frame: pd.DataFrame) -> np.ndarray:
    return np.column_stack(
        [
            frame["primary_probability"].map(logit).to_numpy(float),
            frame["primary_ev"].to_numpy(float),
            frame["break_even_probability"].to_numpy(float),
            frame["relevant_disagreement"].to_numpy(float),
        ]
    )


def fit_continuous_models(
    train: pd.DataFrame,
) -> dict[str, ContinuousGateModel]:
    models: dict[str, ContinuousGateModel] = {}

    for market_type in MARKETS:
        market = train[
            (train["market_type"] == market_type)
            & (train["result_type"] == "decision")
        ].dropna(
            subset=[
                "actual_result",
                "primary_probability",
                "primary_ev",
                "break_even_probability",
                "relevant_disagreement",
            ]
        ).copy()

        if len(market) < 75:
            continue
        if market["actual_result"].nunique() < 2:
            continue

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
        model.fit(
            continuous_features(market),
            market["actual_result"].to_numpy(int),
        )
        models[market_type] = ContinuousGateModel(
            market_type=market_type,
            model=model,
        )

    return models


def apply_continuous_strategy(
    candidates: pd.DataFrame,
    models: dict[str, ContinuousGateModel],
) -> pd.DataFrame:
    parts = []

    for market_type in MARKETS:
        market = candidates[
            candidates["market_type"] == market_type
        ].copy()
        if market.empty:
            continue

        model_bundle = models.get(market_type)
        valid = market.dropna(
            subset=[
                "primary_probability",
                "primary_ev",
                "break_even_probability",
                "relevant_disagreement",
            ]
        ).copy()

        if model_bundle is None or valid.empty:
            market["strategy_probability"] = market["primary_probability"]
            parts.append(market)
            continue

        probabilities = model_bundle.model.predict_proba(
            continuous_features(valid)
        )[:, 1]
        valid["strategy_probability"] = probabilities
        valid = valid[
            valid["strategy_probability"]
            > valid["break_even_probability"]
        ].copy()

        missing_index = market.index.difference(valid.index)
        fallback = market.loc[missing_index].copy()
        fallback["strategy_probability"] = fallback["primary_probability"]

        parts.extend([valid, fallback])

    if not parts:
        return candidates.iloc[0:0].copy()

    return pd.concat(parts, ignore_index=True).sort_values(
        ["game_date", "game_id", "market_type", "bet_side"]
    ).reset_index(drop=True)


def ece_score(probabilities: np.ndarray, outcomes: np.ndarray) -> float:
    if len(probabilities) == 0:
        return np.nan

    frame = pd.DataFrame(
        {
            "p": probabilities,
            "y": outcomes,
        }
    )
    frame["bucket"] = pd.cut(
        frame["p"],
        bins=np.linspace(0.0, 1.0, 11),
        include_lowest=True,
        duplicates="drop",
    )

    total = len(frame)
    ece = 0.0
    for _, group in frame.groupby("bucket", observed=False):
        if group.empty:
            continue
        ece += (
            len(group) / total
            * abs(group["p"].mean() - group["y"].mean())
        )
    return float(ece)


def max_drawdown(profits: np.ndarray) -> float:
    if len(profits) == 0:
        return np.nan
    cumulative = np.concatenate(
        [[0.0], np.cumsum(np.asarray(profits, dtype=float))]
    )
    peaks = np.maximum.accumulate(cumulative)
    drawdowns = cumulative - peaks
    return float(abs(drawdowns.min()))


def metric_row(
    frame: pd.DataFrame,
    *,
    strategy: str,
    quantile: float | None,
    period: str,
    fold: int | None,
    market_scope: str,
) -> dict[str, Any]:
    if market_scope == "all":
        selected = frame.copy()
    else:
        selected = frame[frame["market_type"] == market_scope].copy()

    selected = selected.sort_values(
        ["game_date", "game_id", "market_type", "bet_side"]
    )

    decisions = selected[
        selected["result_type"] == "decision"
    ].dropna(subset=["actual_result"]).copy()

    probability_col = (
        "strategy_probability"
        if "strategy_probability" in decisions.columns
        else "primary_probability"
    )

    if decisions.empty:
        brier = np.nan
        log_loss = np.nan
        calibration_gap = np.nan
        ece = np.nan
        hit_rate = np.nan
    else:
        p = np.clip(
            decisions[probability_col].to_numpy(float),
            EPS,
            1.0 - EPS,
        )
        y = decisions["actual_result"].to_numpy(float)

        brier = float(np.mean((p - y) ** 2))
        log_loss = float(
            -np.mean(
                y * np.log(p)
                + (1.0 - y) * np.log(1.0 - p)
            )
        )
        calibration_gap = float(y.mean() - p.mean())
        ece = ece_score(p, y)
        hit_rate = float(y.mean())

    profits = selected["realized_profit"].to_numpy(float)

    return {
        "period": period,
        "fold": fold,
        "strategy": strategy,
        "quantile": quantile,
        "market_scope": market_scope,
        "bet_count": len(selected),
        "decision_count": len(decisions),
        "push_count": int((selected["result_type"] == "push").sum()),
        "hit_rate_decisions": hit_rate,
        "brier": brier,
        "log_loss": log_loss,
        "calibration_gap": calibration_gap,
        "ece": ece,
        "profit_units": float(profits.sum()) if len(profits) else 0.0,
        "roi": float(profits.mean()) if len(profits) else np.nan,
        "maximum_drawdown_units": max_drawdown(profits),
        "mean_primary_ev": (
            float(selected["primary_ev"].mean())
            if len(selected)
            else np.nan
        ),
        "clv_count": 0,
        "mean_clv": np.nan,
    }


def ev_bucket_rows(
    frame: pd.DataFrame,
    *,
    strategy: str,
    quantile: float | None,
    period: str,
    fold: int | None,
) -> list[dict[str, Any]]:
    rows = []
    for lower, upper, label in EV_BUCKETS:
        if math.isinf(lower):
            mask = frame["primary_ev"] <= upper
        elif math.isinf(upper):
            mask = frame["primary_ev"] > lower
        else:
            mask = (
                (frame["primary_ev"] > lower)
                & (frame["primary_ev"] <= upper)
            )

        bucket = frame[mask].copy()
        decisions = bucket[
            bucket["result_type"] == "decision"
        ].dropna(subset=["actual_result"]).copy()

        rows.append(
            {
                "period": period,
                "fold": fold,
                "strategy": strategy,
                "quantile": quantile,
                "ev_bucket": label,
                "bet_count": len(bucket),
                "decision_count": len(decisions),
                "push_count": int((bucket["result_type"] == "push").sum()),
                "hit_rate_decisions": (
                    float(decisions["actual_result"].mean())
                    if len(decisions)
                    else np.nan
                ),
                "profit_units": (
                    float(bucket["realized_profit"].sum())
                    if len(bucket)
                    else 0.0
                ),
                "roi": (
                    float(bucket["realized_profit"].mean())
                    if len(bucket)
                    else np.nan
                ),
            }
        )
    return rows


def chronological_split(
    candidates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    games = (
        candidates[["game_id", "game_date"]]
        .drop_duplicates("game_id")
        .sort_values(["game_date", "game_id"])
        .reset_index(drop=True)
    )

    index = int(
        math.floor(len(games) * (1.0 - FINAL_TEST_FRACTION))
    )
    index = min(max(index, 1), len(games) - 1)
    final_start = pd.Timestamp(games.iloc[index]["game_date"])

    development = candidates[
        candidates["game_date"] < final_start
    ].copy()
    final_test = candidates[
        candidates["game_date"] >= final_start
    ].copy()

    if development.empty or final_test.empty:
        raise RuntimeError("Chronological development/final split is empty")

    return development, final_test, final_start


def development_folds(
    development: pd.DataFrame,
) -> list[dict[str, Any]]:
    if development.empty:
        raise RuntimeError("Development candidate set is empty")

    daily = (
        development.groupby("game_date", as_index=False)
        .size()
        .sort_values("game_date")
        .reset_index(drop=True)
    )

    if len(daily) < N_FOLDS + 1:
        raise RuntimeError(
            "Insufficient unique development dates for "
            f"{N_FOLDS} chronological folds: dates={len(daily)}"
        )

    total_candidates = int(daily["size"].sum())
    target_initial_candidates = max(
        MIN_FOLD_TRAIN_CANDIDATES,
        int(
            math.ceil(
                total_candidates
                * INITIAL_TRAIN_DATE_FRACTION
            )
        ),
    )

    cumulative = daily["size"].cumsum().to_numpy()
    initial_index = int(
        np.searchsorted(
            cumulative,
            target_initial_candidates,
            side="left",
        )
    )

    # Leave at least one entire date for each validation fold.
    initial_index = min(
        initial_index,
        len(daily) - N_FOLDS - 1,
    )
    initial_index = max(initial_index, 0)

    validation_daily = daily.iloc[
        initial_index + 1 :
    ].copy()

    if len(validation_daily) < N_FOLDS:
        raise RuntimeError(
            "Insufficient validation dates after initial training window: "
            f"initial_train_dates={initial_index + 1} "
            f"validation_dates={len(validation_daily)}"
        )

    # Partition whole dates by candidate counts, not by date counts.
    # This keeps same-day rows together while avoiding tiny folds when
    # betting-candidate density varies sharply through the season.
    remaining_candidates = int(
        validation_daily["size"].sum()
    )
    if remaining_candidates < N_FOLDS:
        raise RuntimeError(
            "Insufficient validation candidates for chronological folds: "
            f"remaining_candidates={remaining_candidates}"
        )

    target_per_fold = (
        remaining_candidates / N_FOLDS
    )
    chunks: list[list[pd.Timestamp]] = []
    current: list[pd.Timestamp] = []
    current_count = 0
    assigned_count = 0

    records = validation_daily.to_dict("records")
    for position, record in enumerate(records):
        current.append(
            pd.Timestamp(record["game_date"])
        )
        current_count += int(record["size"])

        folds_left_after_close = (
            N_FOLDS - len(chunks) - 1
        )
        dates_left = len(records) - position - 1

        threshold = target_per_fold * (
            len(chunks) + 1
        )
        reached_target = (
            assigned_count + current_count
            >= threshold
        )
        enough_dates_left = (
            dates_left >= folds_left_after_close
        )

        if (
            len(chunks) < N_FOLDS - 1
            and reached_target
            and enough_dates_left
        ):
            chunks.append(current)
            assigned_count += current_count
            current = []
            current_count = 0

    if current:
        chunks.append(current)

    if len(chunks) != N_FOLDS:
        # Deterministic fallback: split whole remaining dates into exactly
        # N_FOLDS non-empty chronological chunks.
        date_values = [
            pd.Timestamp(value)
            for value in validation_daily[
                "game_date"
            ].tolist()
        ]
        chunks = [
            list(chunk)
            for chunk in np.array_split(
                np.array(
                    date_values,
                    dtype=object,
                ),
                N_FOLDS,
            )
            if len(chunk)
        ]

    if len(chunks) != N_FOLDS:
        raise RuntimeError(
            "Unable to construct exactly "
            f"{N_FOLDS} chronological validation folds"
        )

    folds = []
    for fold_number, chunk in enumerate(
        chunks,
        start=1,
    ):
        start = pd.Timestamp(chunk[0])
        end = pd.Timestamp(chunk[-1])

        train = development[
            development["game_date"] < start
        ].copy()
        validation = development[
            (
                development["game_date"] >= start
            )
            & (
                development["game_date"] <= end
            )
        ].copy()

        if len(train) < MIN_FOLD_TRAIN_CANDIDATES:
            raise RuntimeError(
                "Fold training set below minimum after "
                "candidate-balanced construction: "
                f"fold={fold_number} train={len(train)} "
                f"minimum={MIN_FOLD_TRAIN_CANDIDATES}"
            )

        if len(validation) < MIN_FOLD_VALIDATION_CANDIDATES:
            raise RuntimeError(
                "Fold validation set is empty after "
                "candidate-balanced construction: "
                f"fold={fold_number}"
            )

        folds.append(
            {
                "fold": fold_number,
                "train": train,
                "validation": validation,
                "validation_start": start,
                "validation_end": end,
            }
        )

    return folds


def evaluate_validation_folds(
    development: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    folds = development_folds(development)
    metric_rows = []
    ev_rows = []

    for fold in folds:
        train = fold["train"]
        validation = fold["validation"]

        primary = validation.copy()
        primary["strategy_probability"] = primary["primary_probability"]

        for market_scope in ("all", *MARKETS):
            metric_rows.append(
                metric_row(
                    primary,
                    strategy="primary_only",
                    quantile=None,
                    period="development_validation",
                    fold=fold["fold"],
                    market_scope=market_scope,
                )
            )
        ev_rows.extend(
            ev_bucket_rows(
                primary,
                strategy="primary_only",
                quantile=None,
                period="development_validation",
                fold=fold["fold"],
            )
        )

        for quantile in QUANTILES:
            for strategy in BINARY_STRATEGIES:
                selected = apply_binary_strategy(
                    validation,
                    strategy,
                    quantile,
                )
                selected["strategy_probability"] = selected[
                    "primary_probability"
                ]

                for market_scope in ("all", *MARKETS):
                    metric_rows.append(
                        metric_row(
                            selected,
                            strategy=strategy,
                            quantile=quantile,
                            period="development_validation",
                            fold=fold["fold"],
                            market_scope=market_scope,
                        )
                    )

                ev_rows.extend(
                    ev_bucket_rows(
                        selected,
                        strategy=strategy,
                        quantile=quantile,
                        period="development_validation",
                        fold=fold["fold"],
                    )
                )

        continuous_models = fit_continuous_models(train)
        continuous = apply_continuous_strategy(
            validation,
            continuous_models,
        )

        for market_scope in ("all", *MARKETS):
            metric_rows.append(
                metric_row(
                    continuous,
                    strategy="continuous_disagreement",
                    quantile=None,
                    period="development_validation",
                    fold=fold["fold"],
                    market_scope=market_scope,
                )
            )

        ev_rows.extend(
            ev_bucket_rows(
                continuous,
                strategy="continuous_disagreement",
                quantile=None,
                period="development_validation",
                fold=fold["fold"],
            )
        )

    return pd.DataFrame(metric_rows), pd.DataFrame(ev_rows)


def aggregate_development(
    fold_metrics: pd.DataFrame,
) -> pd.DataFrame:
    combined = fold_metrics[
        fold_metrics["market_scope"] == "all"
    ].copy()

    group_cols = ["strategy", "quantile"]
    rows = []

    for (strategy, quantile), group in combined.groupby(
        group_cols,
        dropna=False,
    ):
        weights = group["decision_count"].to_numpy(float)
        if weights.sum() <= 0:
            continue

        def weighted(column: str) -> float:
            values = group[column].to_numpy(float)
            valid = np.isfinite(values) & (weights > 0)
            if not valid.any():
                return np.nan
            return float(
                np.average(values[valid], weights=weights[valid])
            )

        bet_count = int(group["bet_count"].sum())
        profit = float(group["profit_units"].sum())

        rows.append(
            {
                "strategy": strategy,
                "quantile": (
                    None
                    if pd.isna(quantile)
                    else float(quantile)
                ),
                "folds": len(group),
                "bet_count": bet_count,
                "decision_count": int(group["decision_count"].sum()),
                "push_count": int(group["push_count"].sum()),
                "brier": weighted("brier"),
                "log_loss": weighted("log_loss"),
                "calibration_gap": weighted("calibration_gap"),
                "ece": weighted("ece"),
                "profit_units": profit,
                "roi": (
                    profit / bet_count
                    if bet_count
                    else np.nan
                ),
                "mean_fold_maximum_drawdown_units": float(
                    group["maximum_drawdown_units"].mean()
                ),
            }
        )

    return pd.DataFrame(rows)


def choose_development_locked_strategies(
    aggregate: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    locked_rows = []

    for strategy in ALL_STRATEGIES:
        options = aggregate[
            aggregate["strategy"] == strategy
        ].copy()
        if options.empty:
            continue

        options["abs_calibration_gap"] = options[
            "calibration_gap"
        ].abs()
        options = options.sort_values(
            [
                "log_loss",
                "brier",
                "abs_calibration_gap",
                "roi",
            ],
            ascending=[True, True, True, False],
        ).reset_index(drop=True)

        winner = options.iloc[0].to_dict()
        locked_rows.append(winner)

    locked = pd.DataFrame(locked_rows)
    locked["abs_calibration_gap"] = locked["calibration_gap"].abs()
    locked = locked.sort_values(
        [
            "log_loss",
            "brier",
            "abs_calibration_gap",
            "roi",
        ],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)
    locked["development_rank"] = np.arange(len(locked)) + 1

    overall = locked.iloc[0].to_dict()

    decision = {
        "selection_data": "development_validation_only",
        "primary_selection_metric": "lowest log_loss",
        "tie_break_1": "lowest brier",
        "tie_break_2": "lowest absolute calibration_gap",
        "tie_break_3": "highest roi",
        "overall_strategy": overall["strategy"],
        "overall_quantile": (
            None
            if pd.isna(overall.get("quantile"))
            else float(overall["quantile"])
        ),
        "development_log_loss": float(overall["log_loss"]),
        "development_brier": float(overall["brier"]),
        "development_calibration_gap": float(
            overall["calibration_gap"]
        ),
        "development_roi": float(overall["roi"]),
    }

    return locked, decision


def evaluate_locked_final(
    development: pd.DataFrame,
    final_test: pd.DataFrame,
    locked: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metric_rows = []
    ev_rows = []

    continuous_models = fit_continuous_models(development)

    for lock in locked.to_dict("records"):
        strategy = lock["strategy"]
        quantile = lock.get("quantile")
        if pd.isna(quantile):
            quantile = None

        if strategy == "primary_only":
            selected = final_test.copy()
            selected["strategy_probability"] = selected[
                "primary_probability"
            ]
        elif strategy == "continuous_disagreement":
            selected = apply_continuous_strategy(
                final_test,
                continuous_models,
            )
        else:
            selected = apply_binary_strategy(
                final_test,
                strategy,
                float(quantile),
            )
            selected["strategy_probability"] = selected[
                "primary_probability"
            ]

        for market_scope in ("all", *MARKETS):
            metric_rows.append(
                metric_row(
                    selected,
                    strategy=strategy,
                    quantile=quantile,
                    period="final_test",
                    fold=None,
                    market_scope=market_scope,
                )
            )

        ev_rows.extend(
            ev_bucket_rows(
                selected,
                strategy=strategy,
                quantile=quantile,
                period="final_test",
                fold=None,
            )
        )

    return pd.DataFrame(metric_rows), pd.DataFrame(ev_rows)


def serialize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in out.columns:
        if "date" in str(column).lower():
            out[column] = out[column].map(
                lambda x: (
                    x.strftime("%Y-%m-%d")
                    if isinstance(x, (pd.Timestamp, datetime))
                    else x
                )
            )
    return out


def main() -> None:
    root = find_repo_root()
    output_dir = root / OUTPUT_REL
    output_dir.mkdir(parents=True, exist_ok=True)

    stage03 = load_module(
        root / STAGE03_REL,
        "nhl_stage03_secondary_for_disagreement_research",
    )
    stage04 = load_module(
        root / STAGE04_REL,
        "nhl_stage04_select_for_disagreement_research",
    )

    history = load_history(root)

    market_frames = {}
    detected_clv = {}
    for market_type in MARKETS:
        frame, clv_fields = load_market_archive(root, market_type)
        market_frames[market_type] = frame
        detected_clv[market_type] = clv_fields

    candidates = build_primary_candidates(
        history,
        market_frames,
    )

    candidates, threshold_reproduction = prepare_point_in_time_features(
        stage03,
        stage04,
        history,
        candidates,
        market_frames,
    )

    development, final_test, final_start = chronological_split(
        candidates
    )

    fold_metrics, development_ev_buckets = evaluate_validation_folds(
        development
    )
    development_aggregate = aggregate_development(
        fold_metrics
    )
    locked, locked_decision = choose_development_locked_strategies(
        development_aggregate
    )

    final_metrics, final_ev_buckets = evaluate_locked_final(
        development,
        final_test,
        locked,
    )

    p75_max_diff = {
        "probability": (
            float(threshold_reproduction["prob_abs_diff"].max())
            if not threshold_reproduction.empty
            else np.nan
        ),
        "margin": (
            float(threshold_reproduction["margin_abs_diff"].max())
            if not threshold_reproduction.empty
            else np.nan
        ),
        "total": (
            float(threshold_reproduction["total_abs_diff"].max())
            if not threshold_reproduction.empty
            else np.nan
        ),
    }

    clv_available = any(detected_clv.values())

    split_manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "research_only": True,
        "production_files_modified": False,
        "markets_yaml_read": False,
        "candidate_rule": (
            "archived primary model side probability > "
            "1 / offered decimal odds"
        ),
        "history_rows": len(history),
        "candidate_rows": len(candidates),
        "candidate_games": int(candidates["game_id"].nunique()),
        "development_candidates": len(development),
        "final_test_candidates": len(final_test),
        "development_start": development["game_date"].min().strftime(
            "%Y-%m-%d"
        ),
        "development_end": development["game_date"].max().strftime(
            "%Y-%m-%d"
        ),
        "final_test_start": final_start.strftime("%Y-%m-%d"),
        "final_test_end": final_test["game_date"].max().strftime(
            "%Y-%m-%d"
        ),
        "quantiles_tested": list(QUANTILES),
        "minimum_prior_rows": MIN_PRIOR_ROWS,
        "same_day_history_excluded": True,
    }

    run_manifest = {
        **split_manifest,
        "package_versions": {
            "python": platform.python_version(),
            "numpy": package_version("numpy"),
            "pandas": package_version("pandas"),
            "scikit-learn": package_version("scikit-learn"),
        },
        "disagreement_definitions": {
            "probability": "abs(sdv_home_win_prob - drat_home_win_prob)",
            "margin": "abs(sdv_exp_margin - drat_exp_margin)",
            "total": "abs(sdv_exp_total - drat_exp_total)",
        },
        "current_threshold_definition": (
            "75th percentile of strictly prior completed history"
        ),
        "strategies": list(ALL_STRATEGIES),
        "price_aware_secondary_semantics": (
            "on high disagreement, keep if SDV OR configured derived source "
            "(moneyline=weighted,puck_line=meta,total=meta) has explicit "
            "side probability above offered break-even"
        ),
        "continuous_features": [
            "logit(primary_probability)",
            "primary_ev",
            "break_even_probability",
            "continuous disagreement",
        ],
        "continuous_model": (
            "per-market standardized logistic regression; "
            "keep when predicted probability > sportsbook break-even"
        ),
        "development_selection_rule": locked_decision,
        "final_test_used_for_selection": False,
        "clv_available": clv_available,
        "detected_clv_fields": detected_clv,
        "p75_reproduction_max_abs_diff": p75_max_diff,
    }

    (output_dir / "split_manifest.json").write_text(
        json.dumps(split_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "locked_strategy.json").write_text(
        json.dumps(locked_decision, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    serialize_dates(candidates).to_csv(
        output_dir / "candidate_dataset.csv",
        index=False,
    )
    serialize_dates(threshold_reproduction).to_csv(
        output_dir / "p75_threshold_reproduction.csv",
        index=False,
    )
    fold_metrics.to_csv(
        output_dir / "development_fold_metrics.csv",
        index=False,
    )
    development_aggregate.to_csv(
        output_dir / "development_strategy_quantile_metrics.csv",
        index=False,
    )
    locked.to_csv(
        output_dir / "development_locked_strategies.csv",
        index=False,
    )
    development_ev_buckets.to_csv(
        output_dir / "development_ev_bucket_performance.csv",
        index=False,
    )
    final_metrics.to_csv(
        output_dir / "final_test_locked_strategy_metrics.csv",
        index=False,
    )
    final_ev_buckets.to_csv(
        output_dir / "final_test_ev_bucket_performance.csv",
        index=False,
    )

    final_all = final_metrics[
        final_metrics["market_scope"] == "all"
    ].copy().sort_values(
        ["log_loss", "brier", "roi"],
        ascending=[True, True, False],
    )

    selected_strategy = locked_decision["overall_strategy"]
    selected_quantile = locked_decision["overall_quantile"]
    selected_final = final_all[
        final_all["strategy"] == selected_strategy
    ]
    if selected_quantile is not None:
        selected_final = selected_final[
            np.isclose(
                pd.to_numeric(
                    selected_final["quantile"],
                    errors="coerce",
                ),
                selected_quantile,
                equal_nan=False,
            )
        ]
    if selected_final.empty:
        selected_final = final_all[
            final_all["strategy"] == selected_strategy
        ]

    summary = [
        "NHL HIGH-DISAGREEMENT GATE AUDIT",
        "================================",
        "research_only=true",
        "production_files_modified=false",
        "markets_yaml_read=false",
        (
            "candidate_rule=primary model side probability "
            "> 1 / offered decimal odds"
        ),
        (
            "current_gate_threshold=75th percentile of strictly "
            "prior completed history"
        ),
        "quantiles_tested=0.60,0.70,0.75,0.80,0.85,0.90",
        (
            f"candidate_rows={len(candidates)} | "
            f"candidate_games={candidates['game_id'].nunique()}"
        ),
        (
            f"development_candidates={len(development)} | "
            f"final_test_candidates={len(final_test)}"
        ),
        f"final_test_start={final_start.strftime('%Y-%m-%d')}",
        (
            "p75_reproduction_max_abs_diff="
            f"prob:{p75_max_diff['probability']:.12g},"
            f"margin:{p75_max_diff['margin']:.12g},"
            f"total:{p75_max_diff['total']:.12g}"
        ),
        (
            "clv_status="
            + (
                "available"
                if clv_available
                else "unavailable_no_explicit_clv_or_closing_field_detected"
            )
        ),
        "",
        "LOCKED DEVELOPMENT DECISION",
        f"strategy={selected_strategy}",
        f"quantile={selected_quantile}",
        (
            f"development_log_loss="
            f"{locked_decision['development_log_loss']:.6f}"
        ),
        (
            f"development_brier="
            f"{locked_decision['development_brier']:.6f}"
        ),
        (
            f"development_calibration_gap="
            f"{locked_decision['development_calibration_gap']:.6f}"
        ),
        (
            f"development_roi="
            f"{locked_decision['development_roi']:.6f}"
        ),
        "",
        "FINAL TEST — DEVELOPMENT-LOCKED STRATEGIES",
    ]

    for row in final_all.to_dict("records"):
        summary.append(
            f"{row['strategy']} q={row['quantile']}: "
            f"bets={int(row['bet_count'])} | "
            f"Brier={row['brier']:.6f} | "
            f"log_loss={row['log_loss']:.6f} | "
            f"cal_gap={row['calibration_gap']:.6f} | "
            f"ECE={row['ece']:.6f} | "
            f"ROI={row['roi']:.6f} | "
            f"max_DD={row['maximum_drawdown_units']:.3f}"
        )

    summary += [
        "",
        "Artifacts:",
        str(output_dir / "split_manifest.json"),
        str(output_dir / "run_manifest.json"),
        str(output_dir / "locked_strategy.json"),
        str(output_dir / "candidate_dataset.csv"),
        str(output_dir / "p75_threshold_reproduction.csv"),
        str(output_dir / "development_fold_metrics.csv"),
        str(output_dir / "development_strategy_quantile_metrics.csv"),
        str(output_dir / "development_locked_strategies.csv"),
        str(output_dir / "development_ev_bucket_performance.csv"),
        str(output_dir / "final_test_locked_strategy_metrics.csv"),
        str(output_dir / "final_test_ev_bucket_performance.csv"),
    ]

    (output_dir / "summary.txt").write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8",
    )

    print("HIGH-DISAGREEMENT GATE AUDIT COMPLETE")
    print(f"Output: {output_dir}")
    print(
        "P75 reproduction max abs diff: "
        f"prob={p75_max_diff['probability']:.12g} | "
        f"margin={p75_max_diff['margin']:.12g} | "
        f"total={p75_max_diff['total']:.12g}"
    )
    print(
        f"Development-locked strategy: {selected_strategy} | "
        f"quantile={selected_quantile}"
    )
    print(f"Final test starts: {final_start.strftime('%Y-%m-%d')}")
    print()
    print(
        final_all[
            [
                "strategy",
                "quantile",
                "bet_count",
                "brier",
                "log_loss",
                "calibration_gap",
                "ece",
                "roi",
                "maximum_drawdown_units",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
