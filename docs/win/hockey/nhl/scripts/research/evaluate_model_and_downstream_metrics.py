#!/usr/bin/env python3
"""
Item 13 â€” evaluate direct model metrics and downstream betting metrics together.

Destination:
    docs/win/hockey/nhl/scripts/research/evaluate_model_and_downstream_metrics.py

Candidate set
-------------
The eight weighted/meta target mappings retained by Item 12:
    moneyline: weighted | meta
    margin:    weighted | meta
    total:     weighted | meta

Why Item 11 models are not included here
----------------------------------------
Item 11 was a development search over 2025-26. Its locked winners require genuinely
unseen 2026-27 chronological validation. Re-selecting those models using 2025-26
realized betting returns would violate that validation discipline.

Evaluation design
-----------------
Direct model metrics are calculated from:
    research/sdv_challenger/season_2025/ensemble_walkforward_predictions.csv

Candidate downstream behavior is calculated by taking every graded primary-qualified
bet from the isolated 2025-26 full-season replay and applying the exact current Stage 04
secondary support rule post-hoc:
    - normal disagreement -> keep primary-qualified bet
    - high disagreement -> keep only when SDV OR chosen derived model supports the side
    - unavailable secondary history -> use primary

The replay itself intentionally had secondary_model_enabled=false, so this post-hoc
gating isolates candidate secondary-model behavior without changing production or
re-running primary pricing.

Research only. No production configuration is modified.
"""

from __future__ import annotations

import argparse
import importlib.util
import itertools
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


SCRIPT_VERSION = "ITEM13-DIRECT-DOWNSTREAM-2026-09-12-v7"


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve()]
    seen: set[Path] = set()
    rel = Path("docs") / "win" / "hockey" / "nhl"
    for start in starts:
        for candidate in [start, *start.parents]:
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / rel).is_dir():
                return candidate
    # Allows --help / syntax inspection before the file is installed.
    # Normal execution from the repository root resolves above.
    return Path.cwd().resolve()


REPO_ROOT = find_repo_root()
NHL_ROOT = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"

DEFAULT_ENSEMBLE_PREDICTIONS = (
    NHL_ROOT
    / "research"
    / "sdv_challenger"
    / "season_2025"
    / "ensemble_walkforward_predictions.csv"
)
DEFAULT_ENSEMBLE_METRICS = (
    NHL_ROOT
    / "research"
    / "sdv_challenger"
    / "season_2025"
    / "ensemble_metrics.csv"
)
DEFAULT_STANDALONE_COMPARISON = (
    NHL_ROOT
    / "research"
    / "sdv_challenger"
    / "season_2025"
    / "standalone_comparison.csv"
)
DEFAULT_CHALLENGER_SUMMARY = (
    NHL_ROOT
    / "research"
    / "sdv_challenger"
    / "season_2025"
    / "summary.json"
)
DEFAULT_CHALLENGER_BUILDER = (
    NHL_ROOT
    / "scripts"
    / "research"
    / "build_sdv_challenger.py"
)
DEFAULT_ITEM12_COMBINATIONS = (
    NHL_ROOT
    / "research"
    / "model_search"
    / "item12_weighted_vs_meta"
    / "item12_weighted_meta_combinations.csv"
)
DEFAULT_BACKTEST_ROOT = NHL_ROOT / "backtest" / "2025_2026"
DEFAULT_CONFIG = NHL_ROOT / "config" / "markets.yaml"
DEFAULT_OUTPUT_DIR = (
    NHL_ROOT
    / "research"
    / "model_search"
    / "item13_direct_and_downstream"
)

EV_BANDS = [
    (-999.0, 0.00, "lt_0"),
    (0.00, 0.01, "0_to_0.01"),
    (0.01, 0.02, "0.01_to_0.02"),
    (0.02, 0.03, "0.02_to_0.03"),
    (0.03, 0.04, "0.03_to_0.04"),
    (0.04, 0.05, "0.04_to_0.05"),
    (0.05, 0.075, "0.05_to_0.075"),
    (0.075, 0.10, "0.075_to_0.10"),
    (0.10, 999.0, "0.10_plus"),
]

PROBABILITY_COLUMNS = {
    "weighted": "weighted_home_win_prob",
    "meta": "meta_home_win_prob",
}
MARGIN_COLUMNS = {
    "weighted": "weighted_exp_margin",
    "meta": "meta_exp_margin",
}
TOTAL_COLUMNS = {
    "weighted": "weighted_exp_total",
    "meta": "meta_exp_total",
}

HIGH_FLAG_BY_MARKET = {
    "moneyline": "high_prob_disagreement_flag",
    "puck_line": "high_margin_disagreement_flag",
    "total": "high_total_disagreement_flag",
}
CHALLENGER_BY_MARKET = {
    "moneyline": "sdv_home_win_prob",
    "puck_line": "sdv_exp_margin",
    "total": "sdv_exp_total",
}

EPS = 1e-15


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Item 13: evaluate direct model metrics and downstream betting metrics "
            "for Item 12 weighted/meta candidate mappings."
        )
    )
    parser.add_argument(
        "--ensemble-predictions",
        type=Path,
        default=DEFAULT_ENSEMBLE_PREDICTIONS,
    )
    parser.add_argument(
        "--ensemble-metrics",
        type=Path,
        default=DEFAULT_ENSEMBLE_METRICS,
    )
    parser.add_argument(
        "--standalone-comparison",
        type=Path,
        default=DEFAULT_STANDALONE_COMPARISON,
        help=(
            "Fallback source used only when ensemble_walkforward_predictions.csv "
            "is missing."
        ),
    )
    parser.add_argument(
        "--challenger-summary",
        type=Path,
        default=DEFAULT_CHALLENGER_SUMMARY,
        help="Existing SDV challenger summary.json used to recover the walk-forward gate.",
    )
    parser.add_argument(
        "--challenger-builder",
        type=Path,
        default=DEFAULT_CHALLENGER_BUILDER,
        help=(
            "Existing build_sdv_challenger.py used only to reconstruct the missing "
            "ensemble predictions in memory."
        ),
    )
    parser.add_argument(
        "--item12-combinations",
        type=Path,
        default=DEFAULT_ITEM12_COMBINATIONS,
    )
    parser.add_argument(
        "--backtest-root",
        type=Path,
        default=DEFAULT_BACKTEST_ROOT,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    return parser.parse_args()


def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"Missing {label}: {path}")


def normalize_game_id(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def normalize_market(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"moneyline", "ml"}:
        return "moneyline"
    if text in {"puck_line", "puckline", "spread"}:
        return "puck_line"
    if text in {"total", "totals"}:
        return "total"
    return text


def normalize_side(value: Any) -> str:
    return str(value).strip().lower()


def finite_numeric(series: pd.Series) -> pd.Series:
    out = pd.to_numeric(series, errors="coerce")
    return out.where(np.isfinite(out), np.nan)


def load_reference_mapping(config_path: Path) -> dict[str, str]:
    require_file(config_path, "markets.yaml")
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    mapping = (
        payload.get("markets", {})
        .get("nhl", {})
        .get("secondary_model", {})
        .get("derived_signal_by_market", {})
    )
    result = {
        "moneyline": str(mapping.get("moneyline", "")).strip().lower(),
        "margin": str(mapping.get("puck_line", "")).strip().lower(),
        "total": str(mapping.get("total", "")).strip().lower(),
    }
    if any(value not in {"weighted", "meta"} for value in result.values()):
        raise RuntimeError(
            f"Current production derived_signal_by_market is not weighted/meta: {result}"
        )
    return result


def load_candidates(path: Path) -> list[dict[str, str]]:
    require_file(path, "Item 12 weighted/meta combinations")
    frame = pd.read_csv(path)

    required = {
        "moneyline_candidate",
        "margin_candidate",
        "total_candidate",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"Item 12 combinations missing columns: {missing}"
        )

    candidates: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    for row in frame.to_dict("records"):
        ml = str(row["moneyline_candidate"]).strip().lower()
        margin = str(row["margin_candidate"]).strip().lower()
        total = str(row["total_candidate"]).strip().lower()
        values = (ml, margin, total)
        if any(value not in {"weighted", "meta"} for value in values):
            continue
        if values in seen:
            continue
        seen.add(values)
        candidates.append(
            {
                "moneyline": ml,
                "margin": margin,
                "total": total,
                "candidate_id": (
                    f"ml_{ml}__margin_{margin}__total_{total}"
                ),
            }
        )

    if len(candidates) != 8:
        raise RuntimeError(
            f"Expected exactly 8 unique weighted/meta combinations; found {len(candidates)}"
        )
    return candidates


def validate_ensemble_predictions(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "game_id",
        "game_date",
        "actual_home_win",
        "actual_margin",
        "actual_total",
        "sdv_home_win_prob",
        "sdv_exp_margin",
        "sdv_exp_total",
        "weighted_home_win_prob",
        "weighted_exp_margin",
        "weighted_exp_total",
        "meta_home_win_prob",
        "meta_exp_margin",
        "meta_exp_total",
        "high_prob_disagreement_flag",
        "high_margin_disagreement_flag",
        "high_total_disagreement_flag",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"ensemble walk-forward predictions missing columns: {missing}"
        )

    frame = frame.copy()
    frame["game_id"] = normalize_game_id(frame["game_id"])

    if frame["game_id"].eq("").any():
        raise RuntimeError("Blank game_id in ensemble predictions.")
    if frame["game_id"].duplicated().any():
        dupes = sorted(
            frame.loc[frame["game_id"].duplicated(keep=False), "game_id"]
            .unique()
            .tolist()
        )
        raise RuntimeError(
            f"Duplicate game_id in ensemble predictions: {dupes[:20]}"
        )

    numeric_cols = sorted(required - {"game_id", "game_date"})
    for col in numeric_cols:
        frame[col] = finite_numeric(frame[col])

    return frame


def load_challenger_builder(path: Path):
    require_file(path, "build_sdv_challenger.py")
    module_name = "_item13_build_sdv_challenger"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not import challenger builder: {path}")

    module = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    if not hasattr(module, "build_walkforward_ensemble"):
        raise RuntimeError(
            "build_sdv_challenger.py has no build_walkforward_ensemble()"
        )
    return module


def load_or_reconstruct_ensemble_predictions(
    *,
    ensemble_path: Path,
    standalone_path: Path,
    summary_path: Path,
    builder_path: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if ensemble_path.exists():
        frame = pd.read_csv(ensemble_path, dtype={"game_id": str})
        return (
            validate_ensemble_predictions(frame),
            {
                "mode": "existing_file",
                "path": str(ensemble_path),
                "reconstructed": False,
            },
        )

    require_file(standalone_path, "standalone_comparison.csv")
    require_file(summary_path, "SDV challenger summary.json")

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    min_train_rows = int(summary.get("ensemble_min_prior_rows", 100))
    expected_rows = int(summary.get("ensemble_walkforward_test_rows", 0))

    if min_train_rows < 20:
        raise RuntimeError(
            f"Invalid ensemble_min_prior_rows in summary.json: {min_train_rows}"
        )

    standalone = pd.read_csv(
        standalone_path,
        dtype={"game_id": str},
    )
    builder = load_challenger_builder(builder_path)

    reconstructed = builder.build_walkforward_ensemble(
        standalone,
        min_train_rows,
    )
    if reconstructed is None or reconstructed.empty:
        raise RuntimeError(
            "Could not reconstruct ensemble walk-forward predictions from "
            "standalone_comparison.csv"
        )

    reconstructed = validate_ensemble_predictions(reconstructed)

    if expected_rows and len(reconstructed) != expected_rows:
        raise RuntimeError(
            "Reconstructed ensemble row count does not match summary.json: "
            f"{len(reconstructed)} != {expected_rows}"
        )

    return (
        reconstructed,
        {
            "mode": "reconstructed_in_memory",
            "path": str(standalone_path),
            "builder": str(builder_path),
            "builder_version": str(
                getattr(builder, "SCRIPT_VERSION", "unknown")
            ),
            "min_train_rows": min_train_rows,
            "expected_rows": expected_rows,
            "reconstructed": True,
        },
    )


def crosscheck_ensemble_metrics(
    predictions: pd.DataFrame,
    metrics_path: Path,
) -> None:
    require_file(metrics_path, "ensemble_metrics.csv")
    metrics = pd.read_csv(metrics_path)

    required = {"model", "market", "metric", "value", "rows"}
    missing = sorted(required - set(metrics.columns))
    if missing:
        raise RuntimeError(
            f"ensemble_metrics.csv missing columns: {missing}"
        )

    for model in ("weighted", "meta"):
        p = predictions[PROBABILITY_COLUMNS[model]].to_numpy(float)
        y = predictions["actual_home_win"].to_numpy(float)
        brier = float(np.mean((p - y) ** 2))
        clipped = np.clip(p, EPS, 1.0 - EPS)
        logloss = float(
            -np.mean(
                y * np.log(clipped)
                + (1.0 - y) * np.log(1.0 - clipped)
            )
        )

        actual_margin = predictions["actual_margin"].to_numpy(float)
        pred_margin = predictions[MARGIN_COLUMNS[model]].to_numpy(float)
        margin_mae = float(np.mean(np.abs(pred_margin - actual_margin)))
        margin_rmse = float(
            np.sqrt(np.mean((pred_margin - actual_margin) ** 2))
        )

        actual_total = predictions["actual_total"].to_numpy(float)
        pred_total = predictions[TOTAL_COLUMNS[model]].to_numpy(float)
        total_mae = float(np.mean(np.abs(pred_total - actual_total)))
        total_rmse = float(
            np.sqrt(np.mean((pred_total - actual_total) ** 2))
        )

        checks = {
            ("moneyline", "brier"): brier,
            ("moneyline", "log_loss"): logloss,
            ("margin", "mae"): margin_mae,
            ("margin", "rmse"): margin_rmse,
            ("total", "mae"): total_mae,
            ("total", "rmse"): total_rmse,
        }

        for (market, metric), calculated in checks.items():
            row = metrics[
                (metrics["model"] == model)
                & (metrics["market"] == market)
                & (metrics["metric"] == metric)
            ]
            if len(row) != 1:
                raise RuntimeError(
                    f"Missing unique ensemble metric: {model}/{market}/{metric}"
                )
            expected = float(row.iloc[0]["value"])
            rows = int(row.iloc[0]["rows"])
            if rows != len(predictions):
                raise RuntimeError(
                    f"Metric row count mismatch for {model}/{market}/{metric}: "
                    f"{rows} != {len(predictions)}"
                )
            if not math.isclose(
                calculated,
                expected,
                rel_tol=1e-8,
                abs_tol=1e-7,
            ):
                raise RuntimeError(
                    f"Metric mismatch for {model}/{market}/{metric}: "
                    f"calculated={calculated} expected={expected}"
                )


def load_graded_primary_bets(backtest_root: Path) -> pd.DataFrame:
    graded_dir = backtest_root / "graded"
    if not graded_dir.exists():
        raise SystemExit(f"Missing backtest graded directory: {graded_dir}")

    files = sorted(graded_dir.glob("*_results_NHL.csv"))
    if not files:
        raise SystemExit(
            f"No daily graded replay files found in {graded_dir}"
        )

    frames: list[pd.DataFrame] = []
    for path in files:
        frame = pd.read_csv(path, dtype={"game_id": str})
        if not frame.empty:
            frame["_source_file"] = path.name
            frames.append(frame)

    if not frames:
        raise RuntimeError("Graded replay files contain no rows.")

    bets = pd.concat(frames, ignore_index=True)

    required = {
        "game_date",
        "game_time",
        "game_id",
        "market_type",
        "bet_side",
        "line",
        "take_bet",
        "dk_odds_american",
        "dk_odds_decimal",
        "model_prob",
        "edge",
        "ev",
        "kelly",
        "bet_result",
    }
    missing = sorted(required - set(bets.columns))
    if missing:
        raise RuntimeError(
            f"Graded backtest rows missing columns: {missing}"
        )

    bets = bets.copy()
    bets["game_id"] = normalize_game_id(bets["game_id"])
    bets["market_type"] = bets["market_type"].map(normalize_market)
    bets["bet_side"] = bets["bet_side"].map(normalize_side)

    numeric_cols = [
        "line",
        "dk_odds_american",
        "dk_odds_decimal",
        "model_prob",
        "edge",
        "ev",
        "kelly",
    ]
    for col in numeric_cols:
        bets[col] = finite_numeric(bets[col])

    valid_markets = {"moneyline", "puck_line", "total"}
    invalid = sorted(
        set(bets["market_type"].dropna().astype(str)) - valid_markets
    )
    if invalid:
        raise RuntimeError(f"Unexpected market types in graded replay: {invalid}")

    return bets


def probability_calibration(
    y: np.ndarray,
    p: np.ndarray,
    bins: int = 10,
) -> dict[str, float]:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = p[valid]

    if len(y) == 0:
        return {
            "calibration_gap_signed": np.nan,
            "calibration_gap_abs": np.nan,
            "ece_10": np.nan,
        }

    gap = float(np.mean(p) - np.mean(y))
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

    return {
        "calibration_gap_signed": gap,
        "calibration_gap_abs": abs(gap),
        "ece_10": float(ece),
    }


def direct_model_metrics(
    predictions: pd.DataFrame,
    candidates: list[dict[str, str]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    y = predictions["actual_home_win"].to_numpy(float)
    actual_margin = predictions["actual_margin"].to_numpy(float)
    actual_total = predictions["actual_total"].to_numpy(float)

    for candidate in candidates:
        ml_model = candidate["moneyline"]
        margin_model = candidate["margin"]
        total_model = candidate["total"]

        p = predictions[PROBABILITY_COLUMNS[ml_model]].to_numpy(float)
        clipped = np.clip(p, EPS, 1.0 - EPS)

        margin_pred = predictions[MARGIN_COLUMNS[margin_model]].to_numpy(float)
        total_pred = predictions[TOTAL_COLUMNS[total_model]].to_numpy(float)

        calibration = probability_calibration(y, p)

        rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "moneyline_model": ml_model,
                "margin_model": margin_model,
                "total_model": total_model,
                "model_rows": len(predictions),
                "moneyline_brier": float(np.mean((p - y) ** 2)),
                "moneyline_log_loss": float(
                    -np.mean(
                        y * np.log(clipped)
                        + (1.0 - y) * np.log(1.0 - clipped)
                    )
                ),
                **calibration,
                "margin_mae": float(
                    np.mean(np.abs(margin_pred - actual_margin))
                ),
                "margin_rmse": float(
                    np.sqrt(np.mean((margin_pred - actual_margin) ** 2))
                ),
                "total_mae": float(
                    np.mean(np.abs(total_pred - actual_total))
                ),
                "total_rmse": float(
                    np.sqrt(np.mean((total_pred - actual_total) ** 2))
                ),
            }
        )

    return pd.DataFrame(rows)


def derived_prediction_column(
    market: str,
    candidate: dict[str, str],
) -> str:
    if market == "moneyline":
        return PROBABILITY_COLUMNS[candidate["moneyline"]]
    if market == "puck_line":
        return MARGIN_COLUMNS[candidate["margin"]]
    if market == "total":
        return TOTAL_COLUMNS[candidate["total"]]
    raise RuntimeError(f"Unknown market: {market}")


def support_label(
    *,
    market: str,
    side: str,
    prediction: Any,
    line: Any,
) -> str:
    try:
        value = float(prediction)
    except Exception:
        return "unavailable"
    if not np.isfinite(value):
        return "unavailable"

    if market == "moneyline":
        if abs(value - 0.5) < 1e-12:
            return "neutral"
        supports_home = value > 0.5
        supports = supports_home if side == "home" else not supports_home
        return "supports" if supports else "opposes"

    try:
        line_value = float(line)
    except Exception:
        return "unavailable"
    if not np.isfinite(line_value):
        return "unavailable"

    if market == "puck_line":
        cover_margin = (
            value + line_value
            if side == "home"
            else -value + line_value
        )
        if abs(cover_margin) < 1e-12:
            return "neutral"
        return "supports" if cover_margin > 0 else "opposes"

    if market == "total":
        if abs(value - line_value) < 1e-12:
            return "neutral"
        supports_over = value > line_value
        supports = supports_over if side == "over" else not supports_over
        return "supports" if supports else "opposes"

    raise RuntimeError(f"Unknown market: {market}")


def apply_candidate_gate(
    base_bets: pd.DataFrame,
    ensemble: pd.DataFrame,
    candidate: dict[str, str],
) -> pd.DataFrame:
    signal_columns = [
        "game_id",
        "sdv_home_win_prob",
        "sdv_exp_margin",
        "sdv_exp_total",
        "weighted_home_win_prob",
        "weighted_exp_margin",
        "weighted_exp_total",
        "meta_home_win_prob",
        "meta_exp_margin",
        "meta_exp_total",
        "high_prob_disagreement_flag",
        "high_margin_disagreement_flag",
        "high_total_disagreement_flag",
    ]
    signals = ensemble[signal_columns].copy()

    # Replay rows already contain placeholder secondary-signal columns.
    # Remove them before joining the reconstructed walk-forward signals so
    # pandas does not create _x/_y suffixed duplicates.
    stale_signal_columns = [
        col
        for col in signal_columns
        if col != "game_id" and col in base_bets.columns
    ]
    base_for_join = base_bets.drop(
        columns=stale_signal_columns,
        errors="ignore",
    ).copy()

    work = base_for_join.merge(
        signals,
        on="game_id",
        how="left",
        validate="many_to_one",
        indicator="ensemble_join",
    )

    keep = np.ones(len(work), dtype=bool)
    reasons: list[str] = []

    for pos, row in enumerate(work.itertuples(index=False)):
        market = normalize_market(getattr(row, "market_type"))
        side = normalize_side(getattr(row, "bet_side"))

        if getattr(row, "ensemble_join") != "both":
            reasons.append("fallback_primary:secondary_unavailable")
            continue

        high_field = HIGH_FLAG_BY_MARKET[market]
        high_raw = getattr(row, high_field)

        try:
            high = float(high_raw) >= 0.5
        except Exception:
            high = False

        if not high:
            reasons.append("normal_disagreement_primary")
            continue

        challenger_field = CHALLENGER_BY_MARKET[market]
        derived_field = derived_prediction_column(market, candidate)

        challenger_support = support_label(
            market=market,
            side=side,
            prediction=getattr(row, challenger_field),
            line=getattr(row, "line"),
        )
        derived_support = support_label(
            market=market,
            side=side,
            prediction=getattr(row, derived_field),
            line=getattr(row, "line"),
        )

        if (
            challenger_support == "supports"
            or derived_support == "supports"
        ):
            reasons.append("high_disagreement_supported")
            continue

        keep[pos] = False
        reasons.append("high_disagreement_no_secondary_support")

    work["_candidate_decision"] = reasons
    work["_candidate_keep"] = keep

    return work.loc[work["_candidate_keep"]].copy()


def american_profit_per_unit(value: Any) -> float:
    try:
        odds = float(value)
    except Exception:
        return np.nan
    if not np.isfinite(odds) or odds == 0:
        return np.nan
    if odds > 0:
        return odds / 100.0
    return 100.0 / abs(odds)


def realized_units(row: pd.Series) -> float:
    result = str(row.get("bet_result", "")).strip().lower()
    if result == "win":
        decimal = row.get("dk_odds_decimal")
        try:
            decimal = float(decimal)
        except Exception:
            decimal = np.nan
        if np.isfinite(decimal) and decimal > 1.0:
            return decimal - 1.0
        return american_profit_per_unit(row.get("dk_odds_american"))
    if result == "loss":
        return -1.0
    if result == "push":
        return 0.0
    return np.nan


def ev_bucket(value: Any) -> str:
    try:
        v = float(value)
    except Exception:
        return "missing"
    if not np.isfinite(v):
        return "missing"

    for low, high, label in EV_BANDS:
        if low <= v <= high:
            return label
    return "out_of_range"


def max_daily_drawdown_units(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    daily = (
        frame.groupby("game_date", dropna=False)["realized_units"]
        .sum(min_count=1)
        .fillna(0.0)
    )
    daily.index = pd.to_datetime(
        daily.index.astype(str).str.replace("_", "-", regex=False),
        errors="coerce",
    )
    daily = daily.sort_index()
    cumulative = daily.cumsum().to_numpy(float)
    if len(cumulative) == 0:
        return 0.0
    path = np.concatenate([[0.0], cumulative])
    running_peak = np.maximum.accumulate(path)
    drawdown = running_peak - path
    return float(np.max(drawdown))


def hhi(shares: pd.Series) -> float:
    values = finite_numeric(shares).dropna().to_numpy(float)
    return float(np.sum(values ** 2)) if len(values) else np.nan


def bet_key_frame(frame: pd.DataFrame) -> pd.Series:
    line = finite_numeric(frame["line"]).round(4).fillna(999999.0)
    return (
        frame["game_id"].astype(str)
        + "|"
        + frame["market_type"].astype(str)
        + "|"
        + frame["bet_side"].astype(str)
        + "|"
        + line.astype(str)
    )


def jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 1.0
    return len(left & right) / len(union)


def daily_count_mae(
    candidate: pd.DataFrame,
    reference: pd.DataFrame,
) -> float:
    left = candidate.groupby("game_date").size().rename("candidate")
    right = reference.groupby("game_date").size().rename("reference")
    joined = pd.concat([left, right], axis=1).fillna(0.0)
    return float(np.mean(np.abs(joined["candidate"] - joined["reference"])))


def find_clv(backtest_root: Path) -> tuple[pd.DataFrame | None, str]:
    path = backtest_root / "clv" / "NHL_clv.csv"
    if not path.exists():
        return None, "unavailable:no_backtest_clv_file"

    frame = pd.read_csv(path, dtype={"game_id": str})
    required = {
        "game_id",
        "market_type",
        "bet_side",
        "line",
        "clv_implied_probability",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        return None, f"unavailable:clv_missing_columns:{','.join(missing)}"

    frame = frame.copy()
    frame["game_id"] = normalize_game_id(frame["game_id"])
    frame["market_type"] = frame["market_type"].map(normalize_market)
    frame["bet_side"] = frame["bet_side"].map(normalize_side)
    frame["line"] = finite_numeric(frame["line"])
    frame["clv_implied_probability"] = finite_numeric(
        frame["clv_implied_probability"]
    )
    frame["_bet_key"] = bet_key_frame(frame)
    frame = frame.drop_duplicates("_bet_key", keep="last")
    return frame, "available"


def downstream_metrics(
    frame: pd.DataFrame,
    *,
    candidate_id: str,
    clv: pd.DataFrame | None,
    clv_status: str,
) -> dict[str, Any]:
    work = frame.copy()
    work["realized_units"] = work.apply(realized_units, axis=1)
    work["ev_bucket"] = work["ev"].map(ev_bucket)
    work["_bet_key"] = bet_key_frame(work)

    result = work["bet_result"].astype(str).str.strip().str.lower()
    wins = int((result == "win").sum())
    losses = int((result == "loss").sum())
    pushes = int((result == "push").sum())
    decided = wins + losses
    bets = wins + losses + pushes

    units = float(work["realized_units"].sum(skipna=True))
    roi = units / bets if bets else np.nan
    hit_rate = wins / decided if decided else np.nan

    market_counts = work.groupby("market_type").size()
    market_shares = (
        market_counts / market_counts.sum()
        if market_counts.sum()
        else pd.Series(dtype=float)
    )

    side_counts = work.groupby(["market_type", "bet_side"]).size()
    side_shares = (
        side_counts / side_counts.sum()
        if side_counts.sum()
        else pd.Series(dtype=float)
    )

    kelly = finite_numeric(work["kelly"]).dropna()

    payload: dict[str, Any] = {
        "candidate_id": candidate_id,
        "bet_count": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "hit_rate": hit_rate,
        "realized_units": units,
        "realized_roi": roi,
        "average_expected_ev": float(finite_numeric(work["ev"]).mean()),
        "total_expected_ev_units": float(finite_numeric(work["ev"]).sum()),
        "max_drawdown_units": max_daily_drawdown_units(work),
        "kelly_mean": float(kelly.mean()) if len(kelly) else np.nan,
        "kelly_median": float(kelly.median()) if len(kelly) else np.nan,
        "kelly_p90": float(kelly.quantile(0.90)) if len(kelly) else np.nan,
        "kelly_max": float(kelly.max()) if len(kelly) else np.nan,
        "kelly_zero_share": float((kelly <= 1e-15).mean())
        if len(kelly)
        else np.nan,
        "max_market_share": float(market_shares.max())
        if len(market_shares)
        else np.nan,
        "market_hhi": hhi(market_shares),
        "max_market_side_share": float(side_shares.max())
        if len(side_shares)
        else np.nan,
        "market_side_hhi": hhi(side_shares),
        "clv_status": clv_status,
        "clv_available_bets": 0,
        "mean_clv_implied_probability": np.nan,
        "positive_clv_rate": np.nan,
    }

    if clv is not None:
        joined = work[["_bet_key"]].merge(
            clv[["_bet_key", "clv_implied_probability"]],
            on="_bet_key",
            how="left",
        )
        values = finite_numeric(joined["clv_implied_probability"]).dropna()
        payload["clv_available_bets"] = int(len(values))
        if len(values):
            payload["mean_clv_implied_probability"] = float(values.mean())
            payload["positive_clv_rate"] = float((values > 0.0).mean())

    return payload


def ev_bucket_metrics(
    frame: pd.DataFrame,
    candidate_id: str,
) -> pd.DataFrame:
    work = frame.copy()
    work["realized_units"] = work.apply(realized_units, axis=1)
    work["ev_bucket"] = work["ev"].map(ev_bucket)
    result = work["bet_result"].astype(str).str.strip().str.lower()
    work["_win"] = (result == "win").astype(int)
    work["_loss"] = (result == "loss").astype(int)
    work["_push"] = (result == "push").astype(int)

    rows: list[dict[str, Any]] = []
    order = [label for _, _, label in EV_BANDS] + ["missing", "out_of_range"]

    for bucket in order:
        part = work[work["ev_bucket"] == bucket]
        if part.empty:
            continue
        wins = int(part["_win"].sum())
        losses = int(part["_loss"].sum())
        pushes = int(part["_push"].sum())
        decided = wins + losses
        bets = wins + losses + pushes
        units = float(part["realized_units"].sum(skipna=True))
        rows.append(
            {
                "candidate_id": candidate_id,
                "ev_bucket": bucket,
                "bet_count": bets,
                "wins": wins,
                "losses": losses,
                "pushes": pushes,
                "hit_rate": wins / decided if decided else np.nan,
                "average_predicted_ev": float(finite_numeric(part["ev"]).mean()),
                "realized_units": units,
                "realized_roi": units / bets if bets else np.nan,
            }
        )
    return pd.DataFrame(rows)


def concentration_metrics(
    frame: pd.DataFrame,
    candidate_id: str,
) -> pd.DataFrame:
    work = frame.copy()
    work["realized_units"] = work.apply(realized_units, axis=1)
    result = work["bet_result"].astype(str).str.strip().str.lower()
    work["_win"] = (result == "win").astype(int)
    work["_loss"] = (result == "loss").astype(int)
    work["_push"] = (result == "push").astype(int)

    total = len(work)
    rows: list[dict[str, Any]] = []
    for (market, side), part in work.groupby(
        ["market_type", "bet_side"],
        dropna=False,
    ):
        wins = int(part["_win"].sum())
        losses = int(part["_loss"].sum())
        pushes = int(part["_push"].sum())
        decided = wins + losses
        bets = wins + losses + pushes
        units = float(part["realized_units"].sum(skipna=True))
        rows.append(
            {
                "candidate_id": candidate_id,
                "market_type": market,
                "bet_side": side,
                "bet_count": bets,
                "bet_share": bets / total if total else np.nan,
                "hit_rate": wins / decided if decided else np.nan,
                "realized_units": units,
                "realized_roi": units / bets if bets else np.nan,
                "average_expected_ev": float(finite_numeric(part["ev"]).mean()),
                "kelly_mean": float(finite_numeric(part["kelly"]).mean()),
            }
        )
    return pd.DataFrame(rows)


def add_stability_and_guardrails(
    combined: pd.DataFrame,
    selected_by_candidate: dict[str, pd.DataFrame],
    reference_id: str,
) -> pd.DataFrame:
    if reference_id not in selected_by_candidate:
        raise RuntimeError(
            f"Reference candidate not evaluated: {reference_id}"
        )

    out = combined.copy()
    reference_selected = selected_by_candidate[reference_id]
    reference_keys = set(bet_key_frame(reference_selected))

    stability: dict[str, tuple[float, float]] = {}
    for candidate_id, frame in selected_by_candidate.items():
        keys = set(bet_key_frame(frame))
        stability[candidate_id] = (
            jaccard(keys, reference_keys),
            daily_count_mae(frame, reference_selected),
        )

    out["selection_jaccard_vs_reference"] = out["candidate_id"].map(
        lambda value: stability[value][0]
    )
    out["daily_bet_count_mae_vs_reference"] = out["candidate_id"].map(
        lambda value: stability[value][1]
    )

    ref_row = out[out["candidate_id"] == reference_id]
    if len(ref_row) != 1:
        raise RuntimeError("Reference candidate summary is not unique.")
    ref = ref_row.iloc[0]

    def materially_worse(row: pd.Series) -> tuple[list[str], str]:
        if row["candidate_id"] == reference_id:
            return [], "REFERENCE"

        flags: list[str] = []

        ref_bets = float(ref["bet_count"])
        bets = float(row["bet_count"])
        if ref_bets > 0:
            ratio = bets / ref_bets
            if ratio < 0.75 or ratio > 1.25:
                flags.append("bet_count_outside_75_125pct_reference")

        if float(row["selection_jaccard_vs_reference"]) < 0.75:
            flags.append("selection_jaccard_below_0.75")

        ref_ev = float(ref["average_expected_ev"])
        row_ev = float(row["average_expected_ev"])
        if np.isfinite(ref_ev) and np.isfinite(row_ev):
            if row_ev < ref_ev - 0.01:
                flags.append("average_expected_ev_down_gt_0.01")

        ref_dd = float(ref["max_drawdown_units"])
        row_dd = float(row["max_drawdown_units"])
        if np.isfinite(ref_dd) and np.isfinite(row_dd):
            dd_limit = max(ref_dd * 1.25, ref_dd + 1.0)
            if row_dd > dd_limit:
                flags.append("max_drawdown_materially_higher")

        ref_conc = float(ref["max_market_side_share"])
        row_conc = float(row["max_market_side_share"])
        if np.isfinite(ref_conc) and np.isfinite(row_conc):
            if row_conc > 0.50 and row_conc > ref_conc + 0.10:
                flags.append("market_side_concentration_materially_higher")

        # Realized ROI is deliberately not used alone. It becomes a guardrail
        # only when hit rate also deteriorates materially.
        ref_roi = float(ref["realized_roi"])
        row_roi = float(row["realized_roi"])
        ref_hit = float(ref["hit_rate"])
        row_hit = float(row["hit_rate"])
        if all(np.isfinite(v) for v in (ref_roi, row_roi, ref_hit, row_hit)):
            if row_roi < ref_roi - 0.05 and row_hit < ref_hit - 0.03:
                flags.append("roi_and_hit_rate_materially_worse")

        status = (
            "REJECT_DOWNSTREAM_GUARDRAIL"
            if flags
            else "RETAIN_FOR_COMPARISON"
        )
        return flags, status

    flags_col: list[str] = []
    status_col: list[str] = []
    for _, row in out.iterrows():
        flags, status = materially_worse(row)
        flags_col.append("|".join(flags))
        status_col.append(status)

    out["downstream_guardrail_flags"] = flags_col
    out["item13_status"] = status_col
    return out


def main() -> int:
    args = parse_args()
    ensemble_path = args.ensemble_predictions.resolve()
    metrics_path = args.ensemble_metrics.resolve()
    standalone_path = args.standalone_comparison.resolve()
    challenger_summary_path = args.challenger_summary.resolve()
    challenger_builder_path = args.challenger_builder.resolve()
    item12_path = args.item12_combinations.resolve()
    backtest_root = args.backtest_root.resolve()
    config_path = args.config.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates = load_candidates(item12_path)
    reference_mapping = load_reference_mapping(config_path)
    reference_id = (
        f"ml_{reference_mapping['moneyline']}"
        f"__margin_{reference_mapping['margin']}"
        f"__total_{reference_mapping['total']}"
    )

    ensemble, ensemble_source = load_or_reconstruct_ensemble_predictions(
        ensemble_path=ensemble_path,
        standalone_path=standalone_path,
        summary_path=challenger_summary_path,
        builder_path=challenger_builder_path,
    )
    crosscheck_ensemble_metrics(ensemble, metrics_path)

    reconstructed_output_path = None
    if ensemble_source.get("reconstructed"):
        reconstructed_output_path = (
            output_dir / "item13_reconstructed_ensemble_walkforward_predictions.csv"
        )
        ensemble.to_csv(reconstructed_output_path, index=False)

    direct = direct_model_metrics(ensemble, candidates)

    base_bets = load_graded_primary_bets(backtest_root)
    clv, clv_status = find_clv(backtest_root)

    downstream_rows: list[dict[str, Any]] = []
    ev_frames: list[pd.DataFrame] = []
    concentration_frames: list[pd.DataFrame] = []
    selected_by_candidate: dict[str, pd.DataFrame] = {}

    for candidate in candidates:
        selected = apply_candidate_gate(
            base_bets,
            ensemble,
            candidate,
        )
        candidate_id = candidate["candidate_id"]
        selected_by_candidate[candidate_id] = selected

        row = downstream_metrics(
            selected,
            candidate_id=candidate_id,
            clv=clv,
            clv_status=clv_status,
        )
        row.update(
            {
                "moneyline_model": candidate["moneyline"],
                "margin_model": candidate["margin"],
                "total_model": candidate["total"],
            }
        )
        downstream_rows.append(row)
        ev_frames.append(ev_bucket_metrics(selected, candidate_id))
        concentration_frames.append(
            concentration_metrics(selected, candidate_id)
        )

    downstream = pd.DataFrame(downstream_rows)

    combined = direct.merge(
        downstream,
        on=[
            "candidate_id",
            "moneyline_model",
            "margin_model",
            "total_model",
        ],
        how="inner",
        validate="one_to_one",
    )
    combined = add_stability_and_guardrails(
        combined,
        selected_by_candidate,
        reference_id,
    )

    # Direct-metric comparison to the current production/reference mapping.
    ref_direct = combined[combined["candidate_id"] == reference_id].iloc[0]
    direct_metric_cols = [
        "moneyline_brier",
        "moneyline_log_loss",
        "calibration_gap_abs",
        "ece_10",
        "margin_mae",
        "margin_rmse",
        "total_mae",
        "total_rmse",
    ]

    def count_direct_worse(row: pd.Series) -> int:
        worse = 0
        for col in direct_metric_cols:
            candidate_value = float(row[col])
            reference_value = float(ref_direct[col])
            if candidate_value > reference_value + 1e-12:
                worse += 1
        return worse

    combined["direct_metrics_worse_than_reference_count"] = combined.apply(
        count_direct_worse,
        axis=1,
    )
    combined["is_current_production_mapping"] = (
        combined["candidate_id"] == reference_id
    )

    # Sort reference first, then candidates with no downstream rejection,
    # then direct-metric degradation count. This is presentation only; it is
    # not an automatic promotion rule.
    combined["_sort_reference"] = (~combined["is_current_production_mapping"]).astype(int)
    combined["_sort_reject"] = (
        combined["item13_status"] == "REJECT_DOWNSTREAM_GUARDRAIL"
    ).astype(int)
    combined = combined.sort_values(
        [
            "_sort_reference",
            "_sort_reject",
            "direct_metrics_worse_than_reference_count",
            "candidate_id",
        ]
    ).drop(columns=["_sort_reference", "_sort_reject"])

    ev_buckets = (
        pd.concat(ev_frames, ignore_index=True)
        if ev_frames
        else pd.DataFrame()
    )
    concentration = (
        pd.concat(concentration_frames, ignore_index=True)
        if concentration_frames
        else pd.DataFrame()
    )

    summary_path = output_dir / "item13_candidate_summary.csv"
    ev_path = output_dir / "item13_ev_bucket_returns.csv"
    concentration_path = output_dir / "item13_market_side_concentration.csv"
    decisions_path = output_dir / "item13_candidate_decisions.csv"
    report_path = output_dir / "item13_report.json"

    combined.to_csv(summary_path, index=False)
    ev_buckets.to_csv(ev_path, index=False)
    concentration.to_csv(concentration_path, index=False)

    decisions = combined[
        [
            "candidate_id",
            "moneyline_model",
            "margin_model",
            "total_model",
            "is_current_production_mapping",
            "direct_metrics_worse_than_reference_count",
            "bet_count",
            "hit_rate",
            "realized_roi",
            "average_expected_ev",
            "max_drawdown_units",
            "selection_jaccard_vs_reference",
            "max_market_side_share",
            "clv_status",
            "downstream_guardrail_flags",
            "item13_status",
        ]
    ].copy()
    decisions.to_csv(decisions_path, index=False)

    report = {
        "script_version": SCRIPT_VERSION,
        "status": "COMPLETE",
        "purpose": (
            "Item 13 direct-model plus downstream betting evaluation; "
            "research only; no automatic production promotion."
        ),
        "candidate_scope": (
            "Eight Item 12 weighted/meta target mappings. Item 11 development "
            "winners excluded because they are locked for genuinely unseen "
            "2026-27 validation."
        ),
        "candidate_count": int(len(combined)),
        "ensemble_walkforward_rows": int(len(ensemble)),
        "ensemble_prediction_source": ensemble_source,
        "reconstructed_ensemble_output": (
            str(reconstructed_output_path)
            if reconstructed_output_path is not None
            else None
        ),
        "ensemble_metrics_crosscheck": "passed",
        "graded_primary_bet_rows": int(len(base_bets)),
        "reference_mapping": reference_mapping,
        "reference_candidate_id": reference_id,
        "model_metrics": [
            "moneyline_brier",
            "moneyline_log_loss",
            "calibration_gap_signed",
            "calibration_gap_abs",
            "ece_10",
            "margin_mae",
            "margin_rmse",
            "total_mae",
            "total_rmse",
        ],
        "downstream_metrics": [
            "bet_count",
            "hit_rate",
            "realized_roi",
            "average_expected_ev",
            "realized_return_by_predicted_ev_bucket",
            "kelly_distribution",
            "max_drawdown_units",
            "clv_where_available",
            "selection_stability",
            "market_side_concentration",
        ],
        "selection_stability_definition": {
            "selection_jaccard_vs_reference": (
                "Jaccard similarity of candidate selected-bet set versus current "
                "production mapping."
            ),
            "daily_bet_count_mae_vs_reference": (
                "Mean absolute difference in daily selected-bet counts versus "
                "current production mapping."
            ),
        },
        "max_drawdown_definition": (
            "Maximum peak-to-trough decline in cumulative flat-risk realized "
            "units after aggregating results by game date."
        ),
        "guardrails": {
            "bet_count": "reject if outside 75%-125% of reference",
            "selection_stability": "reject if Jaccard < 0.75",
            "expected_ev": "reject if average EV is >0.01 below reference",
            "max_drawdown": (
                "reject if above max(1.25x reference, reference + 1 unit)"
            ),
            "concentration": (
                "reject if max market/side share >50% and >10 percentage points "
                "above reference"
            ),
            "realized_performance": (
                "ROI alone never rejects; reject only if ROI is >5 percentage "
                "points worse AND hit rate is >3 percentage points worse"
            ),
        },
        "clv_status": clv_status,
        "rejected_candidates": int(
            (combined["item13_status"] == "REJECT_DOWNSTREAM_GUARDRAIL").sum()
        ),
        "retained_for_comparison": int(
            (combined["item13_status"] == "RETAIN_FOR_COMPARISON").sum()
        ),
        "promotion_decision": None,
        "outputs": {
            "candidate_summary": str(summary_path),
            "ev_bucket_returns": str(ev_path),
            "market_side_concentration": str(concentration_path),
            "candidate_decisions": str(decisions_path),
            "report": str(report_path),
        },
    }

    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(
        "Item 13 evaluation complete: "
        f"candidates={len(combined)} "
        f"ensemble_rows={len(ensemble)} "
        f"ensemble_source={ensemble_source['mode']} "
        f"graded_primary_bets={len(base_bets)} "
        f"rejected={report['rejected_candidates']} "
        f"retained={report['retained_for_comparison']} "
        f"clv={clv_status}"
    )
    print(f"Candidate summary: {summary_path}")
    print(f"Candidate decisions: {decisions_path}")
    print(f"EV buckets: {ev_path}")
    print(f"Concentration: {concentration_path}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



