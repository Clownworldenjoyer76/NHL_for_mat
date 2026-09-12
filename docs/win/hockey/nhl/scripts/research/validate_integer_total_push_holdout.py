#!/usr/bin/env python3
"""
Research-only untouched integer-total push validation.

Uses only integer NHL totals. The earliest ~80% of games are development data.
The latest ~20%, aligned to a date boundary, are never used for source/method
selection or fitting until the winner has been locked.

Requires:
  docs/win/hockey/nhl/scripts/research/research_total_secondary_probability.py

Production files are not modified.
"""

from __future__ import annotations

import importlib.util
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

NHL_REL = Path("docs/win/hockey/nhl")
BASE_SCRIPT_REL = NHL_REL / "scripts/research/research_total_secondary_probability.py"
OUTPUT_REL = NHL_REL / "research/total_secondary_probability/integer_push_holdout"

HOLDOUT_FRACTION = 0.20
EPS = 1e-12


def find_repo_root() -> Path:
    start = Path.cwd().resolve()
    for candidate in (start, *start.parents):
        if (candidate / NHL_REL).is_dir():
            return candidate
    raise RuntimeError("Unable to locate NHL repository root")


def load_base_module(root: Path):
    path = root / BASE_SCRIPT_REL
    if not path.exists():
        raise RuntimeError(f"Required research module not found: {path}")

    spec = importlib.util.spec_from_file_location(
        "research_total_secondary_probability",
        path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def split_integer_games(integer_df: pd.DataFrame):
    ordered = integer_df.sort_values(
        ["game_date", "game_id"]
    ).reset_index(drop=True)

    target_index = int(
        math.floor(len(ordered) * (1.0 - HOLDOUT_FRACTION))
    )
    target_index = min(max(target_index, 1), len(ordered) - 1)

    holdout_start = pd.Timestamp(
        ordered.iloc[target_index]["game_date"]
    )

    development = ordered[
        ordered["game_date"] < holdout_start
    ].copy()
    holdout = ordered[
        ordered["game_date"] >= holdout_start
    ].copy()

    if development.empty or holdout.empty:
        raise RuntimeError("Integer-total chronological split is empty")

    if development["game_date"].max() >= holdout["game_date"].min():
        raise RuntimeError("Chronological split overlap detected")

    return development, holdout, holdout_start


def aggregate_development(
    base,
    fold_metrics: pd.DataFrame,
    predictions: pd.DataFrame,
    sides: pd.DataFrame,
    current: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    expected_folds = fold_metrics["fold"].nunique()

    for source in base.SOURCES:
        current_source = current[
            current["source"] == source
        ].copy()
        current_metrics = base.current_rule_metrics(current_source)

        for method in base.METHODS:
            metric_slice = fold_metrics[
                (fold_metrics["source"] == source)
                & (fold_metrics["method"] == method)
            ].copy()

            successful_folds = int(
                (metric_slice["status"] == "ok").sum()
            )

            pred = predictions[
                (predictions["source"] == source)
                & (predictions["method"] == method)
            ].copy()
            side = sides[
                (sides["source"] == source)
                & (sides["method"] == method)
            ].copy()

            probability_metrics = base.probability_metrics(pred)
            betting_metrics = base.betting_metrics(side)

            eligible = (
                len(metric_slice) == expected_folds
                and successful_folds == expected_folds
                and probability_metrics["game_count"] > 0
                and np.isfinite(
                    probability_metrics["multiclass_log_loss"]
                )
            )

            rows.append(
                {
                    "source": source,
                    "method": method,
                    "expected_folds": expected_folds,
                    "successful_folds": successful_folds,
                    "eligible_for_selection": eligible,
                    **probability_metrics,
                    **betting_metrics,
                    **current_metrics,
                }
            )

    ranking = pd.DataFrame(rows)
    ranking = ranking[
        ranking["eligible_for_selection"]
    ].copy()

    if ranking.empty:
        raise RuntimeError(
            "No source/method combination completed every development fold"
        )

    ranking = ranking.sort_values(
        [
            "multiclass_log_loss",
            "push_brier",
            "decision_log_loss",
            "multiclass_brier",
            "source",
            "method",
        ]
    ).reset_index(drop=True)

    ranking["development_rank"] = np.arange(len(ranking)) + 1
    return ranking


def locked_holdout_evaluation(
    base,
    development: pd.DataFrame,
    holdout: pd.DataFrame,
    *,
    source: str,
    method: str,
):
    bundle = base.fit_derived_bundle(development)

    train_predicted = base.source_prediction(
        source,
        development,
        bundle,
    )
    holdout_predicted = base.source_prediction(
        source,
        holdout,
        bundle,
    )

    fitted = base.fit_method(
        method,
        train_predicted=train_predicted,
        train_actual=development["actual_total"].to_numpy(float),
        train_lines=development["total_line"].to_numpy(float),
        train_outcome=development["outcome_class"].to_numpy(int),
    )

    probabilities = base.predict_method(
        method,
        fitted,
        predicted=holdout_predicted,
        lines=holdout["total_line"].to_numpy(float),
        seed=base.SEED + 990000,
    )

    prediction = holdout[
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
    prediction["predicted_total"] = holdout_predicted
    prediction["p_under"] = probabilities[:, base.UNDER_CLASS]
    prediction["p_push"] = probabilities[:, base.PUSH_CLASS]
    prediction["p_over"] = probabilities[:, base.OVER_CLASS]

    sides = base.side_economics(
        holdout,
        probabilities,
        source=source,
        method=method,
        period="integer_push_holdout",
        fold=None,
    )

    current = base.current_point_rule_rows(
        holdout,
        holdout_predicted,
        source=source,
        period="integer_push_holdout",
        fold=None,
    )

    return prediction, sides, current


def binary_log_loss(y, p):
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    return float(
        -np.mean(
            y * np.log(p)
            + (1.0 - y) * np.log(1.0 - p)
        )
    )


def push_reliability(prediction: pd.DataFrame):
    frame = prediction.dropna(
        subset=["p_push", "actual_push"]
    ).copy()

    if frame.empty:
        raise RuntimeError("No push probabilities in locked holdout")

    edges = np.linspace(0.0, 0.35, 8)
    max_probability = float(frame["p_push"].max())
    if max_probability > edges[-1]:
        edges = np.append(edges, 1.0)

    frame["bucket"] = pd.cut(
        frame["p_push"],
        bins=np.unique(edges),
        include_lowest=True,
        right=True,
        duplicates="drop",
    ).astype(str)

    rows = []
    weighted_error = 0.0

    for bucket, group in frame.groupby("bucket", observed=False):
        if group.empty:
            continue
        mean_probability = float(group["p_push"].mean())
        realized_rate = float(group["actual_push"].mean())
        gap = realized_rate - mean_probability
        weight = len(group) / len(frame)
        weighted_error += weight * abs(gap)

        rows.append(
            {
                "bucket": bucket,
                "sample_size": len(group),
                "mean_push_probability": mean_probability,
                "realized_push_rate": realized_rate,
                "calibration_gap": gap,
            }
        )

    reliability = pd.DataFrame(rows)
    return reliability, float(weighted_error)


def wilson_interval(successes: int, n: int, z: float = 1.959963984540054):
    if n <= 0:
        return np.nan, np.nan
    phat = successes / n
    denominator = 1.0 + z * z / n
    center = (phat + z * z / (2.0 * n)) / denominator
    half = (
        z
        * math.sqrt(
            phat * (1.0 - phat) / n
            + z * z / (4.0 * n * n)
        )
        / denominator
    )
    return center - half, center + half


def line_summary(prediction: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for line, group in prediction.groupby("total_line"):
        pushes = int(group["actual_push"].sum())
        lower, upper = wilson_interval(pushes, len(group))
        rows.append(
            {
                "total_line": line,
                "sample_size": len(group),
                "realized_pushes": pushes,
                "realized_push_rate": float(group["actual_push"].mean()),
                "mean_predicted_push_probability": float(
                    group["p_push"].mean()
                ),
                "push_calibration_gap": float(
                    group["actual_push"].mean()
                    - group["p_push"].mean()
                ),
                "realized_push_rate_ci95_low": lower,
                "realized_push_rate_ci95_high": upper,
            }
        )
    return pd.DataFrame(rows).sort_values("total_line")


def main() -> None:
    root = find_repo_root()
    base = load_base_module(root)

    output_dir = root / OUTPUT_REL
    output_dir.mkdir(parents=True, exist_ok=True)

    history = base.load_history(root)
    prices = base.load_archived_total_prices(root)
    dataset, coverage = base.build_dataset(history, prices)

    integer_df = dataset[
        dataset["total_line"].map(base.is_integer_line)
    ].copy()

    if len(integer_df) < 200:
        raise RuntimeError(
            f"Only {len(integer_df)} integer-total games available"
        )

    development, holdout, holdout_start = split_integer_games(
        integer_df
    )

    folds = base.expanding_folds(development)

    metric_parts = []
    prediction_parts = []
    side_parts = []
    current_parts = []

    for fold in folds:
        metrics, predictions, sides, current = base.evaluate_period(
            fold["train"],
            fold["validation"],
            period="integer_development_validation",
            fold=fold["fold"],
            seed_base=base.SEED + 500000 + fold["fold"] * 1000,
        )
        metric_parts.append(metrics)
        prediction_parts.append(predictions)
        side_parts.append(sides)
        current_parts.append(current)

    fold_metrics = pd.concat(metric_parts, ignore_index=True)
    development_predictions = pd.concat(
        prediction_parts,
        ignore_index=True,
    )
    development_sides = pd.concat(
        side_parts,
        ignore_index=True,
    )
    development_current = pd.concat(
        current_parts,
        ignore_index=True,
    )

    ranking = aggregate_development(
        base,
        fold_metrics,
        development_predictions,
        development_sides,
        development_current,
    )

    winner = ranking.iloc[0]
    locked_source = str(winner["source"])
    locked_method = str(winner["method"])

    locked_manifest = {
        "locked_before_holdout_evaluation": True,
        "source": locked_source,
        "method": locked_method,
        "selection_order": [
            "lowest development multiclass_log_loss",
            "lowest development push_brier",
            "lowest development decision_log_loss",
            "lowest development multiclass_brier",
        ],
        "development_rank": int(winner["development_rank"]),
        "development_multiclass_brier": float(
            winner["multiclass_brier"]
        ),
        "development_multiclass_log_loss": float(
            winner["multiclass_log_loss"]
        ),
        "development_decision_brier": float(
            winner["decision_brier"]
        ),
        "development_decision_log_loss": float(
            winner["decision_log_loss"]
        ),
        "development_push_brier": float(
            winner["push_brier"]
        ),
        "development_positive_ev_roi": float(
            winner["positive_ev_roi"]
        ),
    }

    (
        output_dir / "locked_model.json"
    ).write_text(
        json.dumps(locked_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    prediction, sides, current = locked_holdout_evaluation(
        base,
        development,
        holdout,
        source=locked_source,
        method=locked_method,
    )

    probability_metrics = base.probability_metrics(prediction)
    betting_metrics = base.betting_metrics(sides)
    current_metrics = base.current_rule_metrics(current)

    realized_pushes = int(prediction["actual_push"].sum())
    n_holdout = len(prediction)
    realized_push_rate = float(prediction["actual_push"].mean())
    mean_push_probability = float(prediction["p_push"].mean())
    push_gap = realized_push_rate - mean_push_probability
    push_log_loss = binary_log_loss(
        prediction["actual_push"],
        prediction["p_push"],
    )
    push_ci_low, push_ci_high = wilson_interval(
        realized_pushes,
        n_holdout,
    )

    reliability, push_ece = push_reliability(prediction)
    by_line = line_summary(prediction)

    summary_metrics = {
        "holdout_games": n_holdout,
        "holdout_realized_pushes": realized_pushes,
        "holdout_realized_push_rate": realized_push_rate,
        "holdout_realized_push_rate_ci95_low": push_ci_low,
        "holdout_realized_push_rate_ci95_high": push_ci_high,
        "holdout_mean_predicted_push_probability": mean_push_probability,
        "holdout_push_calibration_gap": push_gap,
        "holdout_push_brier": float(probability_metrics["push_brier"]),
        "holdout_push_log_loss": push_log_loss,
        "holdout_push_ece": push_ece,
        **probability_metrics,
        **betting_metrics,
        **current_metrics,
    }

    split_manifest = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "research_only": True,
        "production_files_modified": False,
        "integer_total_games": len(integer_df),
        "requested_holdout_fraction": HOLDOUT_FRACTION,
        "development_games": len(development),
        "holdout_games": len(holdout),
        "actual_holdout_fraction": len(holdout) / len(integer_df),
        "development_start": development["game_date"].min().strftime(
            "%Y-%m-%d"
        ),
        "development_end": development["game_date"].max().strftime(
            "%Y-%m-%d"
        ),
        "holdout_start": holdout_start.strftime("%Y-%m-%d"),
        "holdout_end": holdout["game_date"].max().strftime("%Y-%m-%d"),
        "development_validation_folds": len(folds),
        "chronology_rule": (
            "all development game dates strictly precede holdout start date"
        ),
        "coverage_from_parent_research": coverage,
    }

    (
        output_dir / "split_manifest.json"
    ).write_text(
        json.dumps(split_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    base.serialize_dates(fold_metrics).to_csv(
        output_dir / "development_fold_metrics.csv",
        index=False,
    )
    ranking.to_csv(
        output_dir / "development_ranking.csv",
        index=False,
    )
    base.serialize_dates(prediction).to_csv(
        output_dir / "push_holdout_predictions.csv",
        index=False,
    )
    base.serialize_dates(sides).to_csv(
        output_dir / "push_holdout_side_economics.csv",
        index=False,
    )
    reliability.to_csv(
        output_dir / "push_holdout_reliability.csv",
        index=False,
    )
    by_line.to_csv(
        output_dir / "push_holdout_by_total_line.csv",
        index=False,
    )
    pd.DataFrame([summary_metrics]).to_csv(
        output_dir / "push_holdout_metrics.csv",
        index=False,
    )

    summary = [
        "NHL INTEGER-TOTAL UNTOUCHED PUSH VALIDATION",
        "==========================================",
        "research_only=true",
        "production_files_modified=false",
        f"integer_total_games={len(integer_df)}",
        f"development_games={len(development)}",
        f"holdout_games={len(holdout)}",
        f"actual_holdout_fraction={len(holdout)/len(integer_df):.6f}",
        f"holdout_start={holdout_start.strftime('%Y-%m-%d')}",
        f"holdout_end={holdout['game_date'].max().strftime('%Y-%m-%d')}",
        f"development_validation_folds={len(folds)}",
        "",
        "LOCKED DEVELOPMENT WINNER",
        f"source={locked_source}",
        f"method={locked_method}",
        f"development_multiclass_brier={winner['multiclass_brier']:.6f}",
        f"development_multiclass_log_loss={winner['multiclass_log_loss']:.6f}",
        f"development_decision_brier={winner['decision_brier']:.6f}",
        f"development_decision_log_loss={winner['decision_log_loss']:.6f}",
        f"development_push_brier={winner['push_brier']:.6f}",
        f"development_positive_ev_roi={winner['positive_ev_roi']:.6f}",
        "",
        "UNTOUCHED INTEGER-TOTAL HOLDOUT",
        f"realized_pushes={realized_pushes}",
        f"realized_push_rate={realized_push_rate:.6f}",
        (
            f"realized_push_rate_ci95="
            f"[{push_ci_low:.6f},{push_ci_high:.6f}]"
        ),
        f"mean_predicted_push_probability={mean_push_probability:.6f}",
        f"push_calibration_gap={push_gap:.6f}",
        f"push_brier={probability_metrics['push_brier']:.6f}",
        f"push_log_loss={push_log_loss:.6f}",
        f"push_ece={push_ece:.6f}",
        f"multiclass_brier={probability_metrics['multiclass_brier']:.6f}",
        f"multiclass_log_loss={probability_metrics['multiclass_log_loss']:.6f}",
        f"decision_brier={probability_metrics['decision_brier']:.6f}",
        f"decision_log_loss={probability_metrics['decision_log_loss']:.6f}",
        f"positive_ev_bets={betting_metrics['positive_ev_bets']}",
        f"positive_ev_roi={betting_metrics['positive_ev_roi']:.6f}",
        f"current_point_rule_roi={current_metrics['current_rule_roi']:.6f}",
        "",
        "Artifacts:",
        str(output_dir / "split_manifest.json"),
        str(output_dir / "locked_model.json"),
        str(output_dir / "development_fold_metrics.csv"),
        str(output_dir / "development_ranking.csv"),
        str(output_dir / "push_holdout_predictions.csv"),
        str(output_dir / "push_holdout_side_economics.csv"),
        str(output_dir / "push_holdout_reliability.csv"),
        str(output_dir / "push_holdout_by_total_line.csv"),
        str(output_dir / "push_holdout_metrics.csv"),
    ]

    (
        output_dir / "summary.txt"
    ).write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8",
    )

    print("INTEGER-TOTAL PUSH HOLDOUT VALIDATION COMPLETE")
    print(f"Output: {output_dir}")
    print(
        f"Development games: {len(development)} | "
        f"Holdout games: {len(holdout)}"
    )
    print(f"Holdout starts: {holdout_start.strftime('%Y-%m-%d')}")
    print(
        f"Locked winner: {locked_source} / {locked_method}"
    )
    print(
        f"Pushes: {realized_pushes}/{n_holdout} "
        f"({realized_push_rate:.4f})"
    )
    print(
        f"Mean predicted P(push): {mean_push_probability:.4f}"
    )
    print(
        f"Push Brier: {probability_metrics['push_brier']:.6f} | "
        f"Push log loss: {push_log_loss:.6f} | "
        f"Push ECE: {push_ece:.6f}"
    )
    print(
        f"Decision Brier: {probability_metrics['decision_brier']:.6f} | "
        f"Decision log loss: {probability_metrics['decision_log_loss']:.6f}"
    )
    print(
        f"Positive-EV ROI: {betting_metrics['positive_ev_roi']:.6f} | "
        f"Current point-rule ROI: {current_metrics['current_rule_roi']:.6f}"
    )


if __name__ == "__main__":
    main()

