#!/usr/bin/env python3
"""
Item 12 — evaluate weighted versus meta secondary models separately by target.

Repository path:
    docs/win/hockey/nhl/scripts/research/evaluate_weighted_vs_meta_by_target.py

Required inputs:
    docs/win/hockey/nhl/research/sdv_challenger/season_2025/standalone_metrics.csv
    docs/win/hockey/nhl/research/sdv_challenger/season_2025/ensemble_metrics.csv
    docs/win/hockey/nhl/research/sdv_challenger/season_2025/summary.json

Purpose
-------
1. Compare moneyline, margin, and total independently.
2. Do not require one model family to win every target.
3. Retain diverse target candidates rather than only the single lowest-error model.
4. Evaluate mixed retained-component bundles with a transparent joint research score.
5. Explicitly include requested weighted/meta mixes and simple baseline bundles.

Important architectural note
----------------------------
Current NHL Stage 04 consumes the secondary target signals independently by market:
moneyline -> home-win probability
puck line -> expected margin
total -> expected total

The referenced research artifacts do not define a cross-target transform that converts a
moneyline/margin/total bundle into a new downstream probability. Therefore this script
DOES NOT invent one. Joint bundle ranking is a research composite of target-specific
validation losses on the common walk-forward validation population.

This script is research-only and never changes production configuration.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SCRIPT_VERSION = "ITEM12-WEIGHTED-VS-META-2026-09-12-v1"

REPO_ROOT = Path(__file__).resolve().parents[6]
NHL_ROOT = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"

DEFAULT_SEASON_DIR = NHL_ROOT / "research" / "sdv_challenger" / "season_2025"
DEFAULT_OUTPUT_DIR = NHL_ROOT / "research" / "model_search" / "item12_weighted_vs_meta"

MARKETS = ("moneyline", "margin", "total")

PRIMARY_METRIC = {
    "moneyline": "log_loss",
    "margin": "rmse",
    "total": "rmse",
}

SECONDARY_METRIC = {
    "moneyline": "brier",
    "margin": "mae",
    "total": "mae",
}

# Candidate diversity policy:
# - weighted and meta are always retained because Item 12 explicitly compares them;
# - the stronger raw baseline for each target is retained as a simpler comparator.
FAMILY_ORDER = ("weighted", "meta", "drat", "sdv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Item 12: evaluate weighted versus meta models independently by target."
    )
    parser.add_argument(
        "--season-dir",
        type=Path,
        default=DEFAULT_SEASON_DIR,
        help="Directory containing standalone_metrics.csv, ensemble_metrics.csv, summary.json.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    return parser.parse_args()


def require_file(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"Required Item 12 input not found: {path}")


def load_inputs(
    season_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    standalone_path = season_dir / "standalone_metrics.csv"
    ensemble_path = season_dir / "ensemble_metrics.csv"
    summary_path = season_dir / "summary.json"

    for path in (standalone_path, ensemble_path, summary_path):
        require_file(path)

    standalone = pd.read_csv(standalone_path)
    ensemble = pd.read_csv(ensemble_path)

    with summary_path.open("r", encoding="utf-8") as fh:
        summary = json.load(fh)

    required_metric_columns = {"model", "market", "metric", "value", "rows"}
    for label, frame in (("standalone", standalone), ("ensemble", ensemble)):
        missing = sorted(required_metric_columns - set(frame.columns))
        if missing:
            raise RuntimeError(
                f"{label} metrics missing required columns: {', '.join(missing)}"
            )

    return standalone, ensemble, summary


def validate_summary(
    standalone: pd.DataFrame,
    ensemble: pd.DataFrame,
    summary: dict[str, Any],
) -> None:
    if summary.get("ensemble_status") != "evaluated_walkforward":
        raise RuntimeError(
            "summary.json does not report ensemble_status=evaluated_walkforward"
        )

    if summary.get("same_day_source_games") != "excluded":
        raise RuntimeError("summary.json does not confirm same-day exclusion.")

    leakage_rule = str(summary.get("leakage_rule", ""))
    if leakage_rule != "source_game_date < target_game_date":
        raise RuntimeError(
            "Unexpected leakage rule in summary.json: "
            f"{leakage_rule!r}"
        )

    walkforward_rows = int(summary.get("ensemble_walkforward_test_rows", 0))
    if walkforward_rows <= 0:
        raise RuntimeError("summary.json has no positive ensemble walk-forward row count.")

    target_ensemble = ensemble[
        ensemble["model"].isin(["weighted", "meta"])
        & ensemble["market"].isin(MARKETS)
    ].copy()

    if target_ensemble.empty:
        raise RuntimeError("ensemble_metrics.csv has no weighted/meta target metrics.")

    bad_rows = target_ensemble[
        pd.to_numeric(target_ensemble["rows"], errors="coerce") != walkforward_rows
    ]
    if not bad_rows.empty:
        raise RuntimeError(
            "Weighted/meta metrics are not evaluated on the common walk-forward population."
        )

    # Cross-check values in summary.json against ensemble_metrics.csv where present.
    summary_rows = summary.get("ensemble_metrics", [])
    summary_lookup: dict[tuple[str, str, str], tuple[float, int]] = {}
    for row in summary_rows:
        try:
            key = (str(row["model"]), str(row["market"]), str(row["metric"]))
            summary_lookup[key] = (float(row["value"]), int(row["rows"]))
        except Exception:
            continue

    for row in target_ensemble.itertuples(index=False):
        key = (str(row.model), str(row.market), str(row.metric))
        if key not in summary_lookup:
            raise RuntimeError(f"summary.json missing ensemble metric {key}")
        value, rows = summary_lookup[key]
        if rows != int(row.rows) or not math.isclose(
            value,
            float(row.value),
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise RuntimeError(
                f"summary.json metric mismatch for {key}: "
                f"summary=({value},{rows}) csv=({row.value},{row.rows})"
            )

    # Standalone metrics are 1291 rows while ensemble walk-forward begins after
    # the prior-row sufficiency gate. This difference is expected. We do not
    # compare raw standalone metrics directly against weighted/meta metrics for
    # winner selection because that would be unequal-population comparison.
    standalone_rows = sorted(
        set(pd.to_numeric(standalone["rows"], errors="coerce").dropna().astype(int))
    )
    if not standalone_rows:
        raise RuntimeError("standalone_metrics.csv contains no row counts.")


def metric_value(
    frame: pd.DataFrame,
    *,
    model: str,
    market: str,
    metric: str,
) -> float:
    rows = frame[
        (frame["model"] == model)
        & (frame["market"] == market)
        & (frame["metric"] == metric)
    ]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one metric row for model={model} market={market} metric={metric}; "
            f"found {len(rows)}"
        )
    return float(rows.iloc[0]["value"])


def metric_rows(
    frame: pd.DataFrame,
    *,
    model: str,
    market: str,
    metric: str,
) -> int:
    rows = frame[
        (frame["model"] == model)
        & (frame["market"] == market)
        & (frame["metric"] == metric)
    ]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected one metric row for model={model} market={market} metric={metric}; "
            f"found {len(rows)}"
        )
    return int(rows.iloc[0]["rows"])


def build_component_results(
    standalone: pd.DataFrame,
    ensemble: pd.DataFrame,
    summary: dict[str, Any],
) -> pd.DataFrame:
    """
    Component selection uses the common 1189-row weighted/meta validation population.

    Raw DRAT/SDV rows from standalone_metrics.csv cover 1291 rows and are therefore
    kept as contextual baselines only in this table. They are NOT eligible to defeat
    weighted/meta in equal-population component selection.

    The 1189-row raw baseline values are not contained in the three Item 12 source
    artifacts. We therefore do not silently substitute unequal-population values.
    """
    walkforward_rows = int(summary["ensemble_walkforward_test_rows"])

    out: list[dict[str, Any]] = []

    for market in MARKETS:
        primary = PRIMARY_METRIC[market]
        secondary = SECONDARY_METRIC[market]

        for model in ("weighted", "meta"):
            out.append(
                {
                    "market": market,
                    "candidate": model,
                    "candidate_class": "ensemble",
                    "comparison_population": "common_walkforward",
                    "rows": metric_rows(
                        ensemble, model=model, market=market, metric=primary
                    ),
                    "primary_metric": primary,
                    "primary_value": metric_value(
                        ensemble, model=model, market=market, metric=primary
                    ),
                    "secondary_metric": secondary,
                    "secondary_value": metric_value(
                        ensemble, model=model, market=market, metric=secondary
                    ),
                    "equal_population_selection_eligible": True,
                }
            )

        for model in ("drat", "sdv"):
            out.append(
                {
                    "market": market,
                    "candidate": model,
                    "candidate_class": "raw_baseline",
                    "comparison_population": "full_standalone",
                    "rows": metric_rows(
                        standalone, model=model, market=market, metric=primary
                    ),
                    "primary_metric": primary,
                    "primary_value": metric_value(
                        standalone, model=model, market=market, metric=primary
                    ),
                    "secondary_metric": secondary,
                    "secondary_value": metric_value(
                        standalone, model=model, market=market, metric=secondary
                    ),
                    "equal_population_selection_eligible": False,
                }
            )

    result = pd.DataFrame(out)

    # Rank weighted/meta independently within each target.
    result["equal_population_rank"] = pd.NA
    for market in MARKETS:
        mask = (
            (result["market"] == market)
            & result["equal_population_selection_eligible"]
        )
        ranked = result.loc[mask].sort_values(
            ["primary_value", "secondary_value", "candidate"]
        )
        for rank, idx in enumerate(ranked.index, start=1):
            result.loc[idx, "equal_population_rank"] = rank

    return result


def build_retained_components(component_results: pd.DataFrame) -> pd.DataFrame:
    """
    Weighted and meta are both retained for every target.

    Raw baselines are also retained as simple comparison references, but they remain
    explicitly marked unequal-population because the listed Item 12 sources do not
    contain their 1189-row walk-forward metrics.
    """
    retained = component_results.copy()
    retained["retained"] = True
    retained["retention_reason"] = ""

    for idx, row in retained.iterrows():
        if row["candidate"] in {"weighted", "meta"}:
            rank = row["equal_population_rank"]
            if int(rank) == 1:
                reason = "best_weighted_meta_for_target"
            else:
                reason = "retain_diverse_weighted_meta_alternative"
        else:
            reason = "simple_baseline_context_unequal_population"
        retained.loc[idx, "retention_reason"] = reason

    return retained


def candidate_metrics_lookup(
    component_results: pd.DataFrame,
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in component_results.to_dict("records"):
        lookup[(row["market"], row["candidate"])] = row
    return lookup


def weighted_meta_best(component_results: pd.DataFrame, market: str) -> str:
    eligible = component_results[
        (component_results["market"] == market)
        & component_results["equal_population_selection_eligible"]
    ].sort_values(["primary_value", "secondary_value", "candidate"])
    if eligible.empty:
        raise RuntimeError(f"No weighted/meta candidates for {market}")
    return str(eligible.iloc[0]["candidate"])


def build_weighted_meta_combinations(
    component_results: pd.DataFrame,
) -> pd.DataFrame:
    """
    Exhaustive 2 x 2 x 2 weighted/meta component combinations.

    The joint score is the mean relative primary loss versus the independently best
    weighted/meta candidate for each target:
        mean(component_primary / best_target_primary)

    1.0 is the theoretical best achievable from the retained weighted/meta choices.
    Lower is better. The secondary joint score is constructed the same way.
    """
    lookup = candidate_metrics_lookup(component_results)

    best_primary: dict[str, float] = {}
    best_secondary: dict[str, float] = {}
    for market in MARKETS:
        eligible = component_results[
            (component_results["market"] == market)
            & component_results["equal_population_selection_eligible"]
        ]
        best_primary[market] = float(eligible["primary_value"].min())
        best_secondary[market] = float(eligible["secondary_value"].min())

    rows: list[dict[str, Any]] = []

    for moneyline, margin, total in itertools.product(
        ("weighted", "meta"),
        repeat=3,
    ):
        choices = {
            "moneyline": moneyline,
            "margin": margin,
            "total": total,
        }

        record: dict[str, Any] = {
            "moneyline_candidate": moneyline,
            "margin_candidate": margin,
            "total_candidate": total,
        }

        primary_ratios: list[float] = []
        secondary_ratios: list[float] = []

        for market in MARKETS:
            candidate = choices[market]
            row = lookup[(market, candidate)]
            record[f"{market}_{row['primary_metric']}"] = float(row["primary_value"])
            record[f"{market}_{row['secondary_metric']}"] = float(
                row["secondary_value"]
            )
            primary_ratios.append(
                float(row["primary_value"]) / best_primary[market]
            )
            secondary_ratios.append(
                float(row["secondary_value"]) / best_secondary[market]
            )

        record["joint_primary_relative_loss"] = float(np.mean(primary_ratios))
        record["joint_secondary_relative_loss"] = float(np.mean(secondary_ratios))
        record["joint_primary_excess_pct"] = (
            record["joint_primary_relative_loss"] - 1.0
        ) * 100.0
        record["joint_secondary_excess_pct"] = (
            record["joint_secondary_relative_loss"] - 1.0
        ) * 100.0
        rows.append(record)

    return pd.DataFrame(rows).sort_values(
        [
            "joint_primary_relative_loss",
            "joint_secondary_relative_loss",
            "moneyline_candidate",
            "margin_candidate",
            "total_candidate",
        ]
    ).reset_index(drop=True)


def build_named_combinations(
    component_results: pd.DataFrame,
    weighted_meta_combinations: pd.DataFrame,
) -> pd.DataFrame:
    lookup = candidate_metrics_lookup(component_results)

    best = {
        market: weighted_meta_best(component_results, market)
        for market in MARKETS
    }

    named = [
        (
            "independent_best_weighted_meta",
            best["moneyline"],
            best["margin"],
            best["total"],
            "best weighted/meta candidate selected independently for each target",
        ),
        (
            "requested_weighted_ml_meta_margin_meta_total",
            "weighted",
            "meta",
            "meta",
            "explicit Item 12 requested combination",
        ),
        (
            "requested_meta_ml_weighted_margin_meta_total",
            "meta",
            "weighted",
            "meta",
            "explicit Item 12 requested combination",
        ),
        (
            "weighted_all",
            "weighted",
            "weighted",
            "weighted",
            "single-family weighted baseline",
        ),
        (
            "meta_all",
            "meta",
            "meta",
            "meta",
            "single-family meta baseline",
        ),
        (
            "drat_all_full_standalone_context",
            "drat",
            "drat",
            "drat",
            "simple raw baseline; unequal validation population",
        ),
        (
            "sdv_all_full_standalone_context",
            "sdv",
            "sdv",
            "sdv",
            "simple raw baseline; unequal validation population",
        ),
    ]

    rows: list[dict[str, Any]] = []

    for name, ml, margin, total, note in named:
        record: dict[str, Any] = {
            "combination": name,
            "moneyline_candidate": ml,
            "margin_candidate": margin,
            "total_candidate": total,
            "note": note,
        }

        if {ml, margin, total}.issubset({"weighted", "meta"}):
            match = weighted_meta_combinations[
                (weighted_meta_combinations["moneyline_candidate"] == ml)
                & (weighted_meta_combinations["margin_candidate"] == margin)
                & (weighted_meta_combinations["total_candidate"] == total)
            ]
            if len(match) != 1:
                raise RuntimeError(f"Could not resolve weighted/meta combination {name}")
            for key, value in match.iloc[0].to_dict().items():
                if key not in {
                    "moneyline_candidate",
                    "margin_candidate",
                    "total_candidate",
                }:
                    record[key] = value
            record["comparison_population"] = "common_walkforward_1189"
            record["joint_score_comparable"] = True
        else:
            # Raw baseline context from standalone_metrics.csv.
            primary_values = []
            secondary_values = []
            rows_counts = []
            for market, candidate in (
                ("moneyline", ml),
                ("margin", margin),
                ("total", total),
            ):
                row = lookup[(market, candidate)]
                record[f"{market}_{row['primary_metric']}"] = float(
                    row["primary_value"]
                )
                record[f"{market}_{row['secondary_metric']}"] = float(
                    row["secondary_value"]
                )
                primary_values.append(float(row["primary_value"]))
                secondary_values.append(float(row["secondary_value"]))
                rows_counts.append(int(row["rows"]))

            record["comparison_population"] = f"full_standalone_{min(rows_counts)}"
            record["joint_score_comparable"] = False
            record["joint_primary_relative_loss"] = np.nan
            record["joint_secondary_relative_loss"] = np.nan
            record["joint_primary_excess_pct"] = np.nan
            record["joint_secondary_excess_pct"] = np.nan

        rows.append(record)

    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    season_dir = args.season_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    standalone, ensemble, summary = load_inputs(season_dir)
    validate_summary(standalone, ensemble, summary)

    components = build_component_results(standalone, ensemble, summary)
    retained = build_retained_components(components)
    combos = build_weighted_meta_combinations(components)
    named = build_named_combinations(components, combos)

    components.to_csv(
        output_dir / "item12_component_results.csv",
        index=False,
    )
    retained.to_csv(
        output_dir / "item12_retained_components.csv",
        index=False,
    )
    combos.to_csv(
        output_dir / "item12_weighted_meta_combinations.csv",
        index=False,
    )
    named.to_csv(
        output_dir / "item12_named_combinations.csv",
        index=False,
    )

    winners = {
        market: weighted_meta_best(components, market)
        for market in MARKETS
    }

    best_combo = combos.iloc[0].to_dict()

    report = {
        "script_version": SCRIPT_VERSION,
        "status": "COMPLETE",
        "purpose": (
            "Item 12 weighted-versus-meta evaluation by target; research only; "
            "no automatic production change."
        ),
        "inputs": {
            "standalone_metrics": str(season_dir / "standalone_metrics.csv"),
            "ensemble_metrics": str(season_dir / "ensemble_metrics.csv"),
            "summary": str(season_dir / "summary.json"),
        },
        "walkforward_rows": int(summary["ensemble_walkforward_test_rows"]),
        "walkforward_rule": summary.get("leakage_rule"),
        "same_day_source_games": summary.get("same_day_source_games"),
        "independent_weighted_meta_winners": winners,
        "retention_policy": (
            "retain weighted and meta for every target; raw DRAT/SDV kept as "
            "simpler unequal-population context because the listed sources do not "
            "contain their 1189-row metrics"
        ),
        "joint_evaluation": {
            "weighted_meta_combinations": int(len(combos)),
            "primary_score": (
                "mean(target primary loss / independently best weighted-meta "
                "primary loss); lower is better"
            ),
            "secondary_score": (
                "mean(target secondary loss / independently best weighted-meta "
                "secondary loss); lower is better"
            ),
            "best_combination": {
                "moneyline": best_combo["moneyline_candidate"],
                "margin": best_combo["margin_candidate"],
                "total": best_combo["total_candidate"],
                "joint_primary_relative_loss": float(
                    best_combo["joint_primary_relative_loss"]
                ),
                "joint_secondary_relative_loss": float(
                    best_combo["joint_secondary_relative_loss"]
                ),
            },
        },
        "architecture_note": (
            "Current Stage 04 consumes secondary moneyline, margin, and total "
            "signals independently by market. No repository-defined cross-target "
            "probability transform is present in the referenced Item 12 artifacts, "
            "so none is invented here."
        ),
        "cross_target_probability_transform_evaluated": False,
        "promotion_decision": None,
        "outputs": {
            "component_results": str(
                output_dir / "item12_component_results.csv"
            ),
            "retained_components": str(
                output_dir / "item12_retained_components.csv"
            ),
            "weighted_meta_combinations": str(
                output_dir / "item12_weighted_meta_combinations.csv"
            ),
            "named_combinations": str(
                output_dir / "item12_named_combinations.csv"
            ),
            "report": str(output_dir / "item12_report.json"),
        },
    }

    with (output_dir / "item12_report.json").open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)

    print(
        "Item 12 evaluation complete: "
        f"walkforward_rows={report['walkforward_rows']} "
        f"moneyline_winner={winners['moneyline']} "
        f"margin_winner={winners['margin']} "
        f"total_winner={winners['total']} "
        f"weighted_meta_combinations={len(combos)}"
    )
    print(f"Components: {output_dir / 'item12_component_results.csv'}")
    print(f"Named combinations: {output_dir / 'item12_named_combinations.csv'}")
    print(f"Report: {output_dir / 'item12_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
