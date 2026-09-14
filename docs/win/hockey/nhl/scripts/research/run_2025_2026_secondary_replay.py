#!/usr/bin/env python3
"""
Item 15 — True end-to-end 2025-26 NHL replay with production secondary models enabled.

Destination:
    docs/win/hockey/nhl/scripts/research/run_2025_2026_secondary_replay.py

Pipeline executed in an isolated temporary repository:
    1. Stage 01 probability construction (build_juice_files.py)
    2. Stage 02 calibration / juice
    3. Stage 03 edges
    4. Stage 03 EV/Kelly
    5. build_secondary_model_signals.py
    6. Stage 04 final selection
    7. Stage 05 grading, analysis, and reports

Historical discipline:
    - D-Ratings predictions and archived sportsbook prices come only from the
      2025-26 season_master files used by the existing replay.
    - Historical SDV target-game predictions come only from the existing
      season_2025 standalone_comparison.csv and must declare
      "source_game_date < target_game_date".
    - The secondary builder is executed unchanged inside the isolated replay.
      Its target-date fits use only historical rows with game_date < target_date.
    - Missing historical fatigue, strength, goalie, lineup, provenance timestamp,
      or SDV rows remain explicitly unavailable. They are not reconstructed.
    - Final scores are copied only for Stage 05 grading and for prior-only
      secondary training through the builder's chronological filter.
    - Live docs/win/hockey/nhl/04_select is hashed before and after the replay and
      the run aborts if it changes.

Research output:
    docs/win/hockey/nhl/backtest/2025_2026_secondary_enabled/

No live production Stage 01-05 output folders are written.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

SCRIPT_VERSION = "ITEM15-SECONDARY-E2E-2026-09-12-v3"
NHL_REL = Path("docs/win/hockey/nhl")
SEASON = "2025_2026"

FATIGUE_FEATURE_COLUMNS = [
    "home_days_rest", "away_days_rest",
    "home_back_to_back", "away_back_to_back",
    "home_games_in_4_days", "away_games_in_4_days",
    "home_three_in_four", "away_three_in_four",
    "home_games_in_6_days", "away_games_in_6_days",
    "home_four_in_six", "away_four_in_six",
    "home_games_in_7_days", "away_games_in_7_days",
    "rest_differential",
]

TEAM_STRENGTH_FEATURE_COLUMNS = [
    "home_adj_xgf", "away_adj_xgf", "adj_xgf_differential",
    "home_adj_xga", "away_adj_xga", "adj_xga_differential",
    "home_adj_xg_net", "away_adj_xg_net", "adj_xg_net_differential",
    "home_adj_gf", "away_adj_gf", "adj_gf_differential",
    "home_adj_ga", "away_adj_ga", "adj_ga_differential",
    "home_off_rank", "away_off_rank", "off_rank_differential",
    "home_def_rank", "away_def_rank", "def_rank_differential",
    "home_net_rank", "away_net_rank", "net_rank_differential",
    "home_net_z", "away_net_z", "net_z_differential",
]

GOALIE_FEATURE_COLUMNS = [
    "home_expected_starter", "away_expected_starter",
    "home_starter_gsax", "away_starter_gsax",
    "home_backup_gsax", "away_backup_gsax",
    "starter_gsax_differential",
    "home_goalie_status", "away_goalie_status",
    "home_goalie_status_observed_at", "away_goalie_status_observed_at",
    "home_goalie_status_source", "away_goalie_status_source",
]

LINEUP_FEATURE_COLUMNS = [
    "home_skater_rapm", "away_skater_rapm", "skater_rapm_differential",
    "home_skater_war", "away_skater_war", "skater_war_differential",
    "home_pp_value", "away_pp_value", "pp_value_differential",
    "home_pk_value", "away_pk_value", "pk_value_differential",
    "home_forward_line_strength", "away_forward_line_strength",
    "forward_line_strength_differential",
    "home_defense_pair_strength", "away_defense_pair_strength",
    "defense_pair_strength_differential",
    "home_lineup_status", "away_lineup_status",
    "home_lineup_observed_at", "away_lineup_observed_at",
    "home_lineup_source", "away_lineup_source",
]

SDV_PREDICTION_COLUMNS = [
    "sdv_home_win_prob",
    "sdv_exp_margin",
    "sdv_exp_total",
]

SPORTSBOOK_FIELDS = [
    "home_dk_moneyline_american",
    "away_dk_moneyline_american",
    "home_puck_line",
    "away_puck_line",
    "total",
    "home_dk_puck_line_american",
    "away_dk_puck_line_american",
    "dk_total_over_american",
    "dk_total_under_american",
    "home_dk_moneyline_decimal",
    "away_dk_moneyline_decimal",
    "home_dk_puck_line_decimal",
    "away_dk_puck_line_decimal",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]

PROVENANCE_FIELDS = [
    "odds_source",
    "moneyline_provider_id",
    "moneyline_provider_name",
    "puck_line_provider_id",
    "puck_line_provider_name",
    "total_provider_id",
    "total_provider_name",
    "pulled_at",
]


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
    raise RuntimeError(
        f"Could not find repository root containing {NHL_REL.as_posix()}"
    )


def require(path: Path, label: str) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing {label}: {path}")


def canonical_game_id(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def normalize_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        series.astype(str).str.strip().str.replace("_", "-", regex=False),
        errors="coerce",
    ).dt.normalize()


def american_to_decimal(value):
    try:
        a = float(value)
    except Exception:
        return np.nan
    if not np.isfinite(a) or a == 0:
        return np.nan
    if a > 0:
        return 1.0 + a / 100.0
    return 1.0 + 100.0 / abs(a)


def validate_unique_game_ids(df: pd.DataFrame, label: str) -> None:
    if "game_id" not in df.columns:
        raise RuntimeError(f"{label} missing game_id")
    ids = canonical_game_id(df["game_id"])
    if ids.eq("").any():
        raise RuntimeError(f"{label} contains blank game_id")
    dupes = ids[ids.duplicated(keep=False)]
    if not dupes.empty:
        raise RuntimeError(
            f"{label} contains duplicate game_id values: "
            + ", ".join(sorted(dupes.unique())[:20])
        )


def read_master(path: Path, label: str) -> pd.DataFrame:
    require(path, label)
    df = pd.read_csv(path, dtype={"game_id": str}, encoding="utf-8-sig")
    validate_unique_game_ids(df, label)
    df["game_id"] = canonical_game_id(df["game_id"])
    return df


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def directory_manifest(root: Path) -> dict[str, str]:
    if not root.exists():
        return {}
    manifest: dict[str, str] = {}
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = path.relative_to(root).as_posix()
        manifest[rel] = file_sha256(path)
    return manifest


def copy_file(src: Path, dst: Path) -> None:
    require(src, "required file")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def copy_tree(src: Path, dst: Path) -> None:
    require(src, "required directory")
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def run_script(fake_repo_root: Path, rel: Path) -> None:
    script = fake_repo_root / NHL_REL / rel
    require(script, f"replay script {rel.as_posix()}")
    print(f"RUN  {rel.as_posix()}")
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=fake_repo_root,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Script failed ({result.returncode}): {rel.as_posix()}"
        )


def load_historical_sdv(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    require(path, "historical SDV standalone comparison")
    df = pd.read_csv(path, dtype={"game_id": str}, encoding="utf-8-sig")
    validate_unique_game_ids(df, "standalone_comparison.csv")

    required = {
        "game_id",
        "game_date",
        "sdv_home_win_prob",
        "sdv_exp_margin",
        "sdv_exp_total",
        "actual_home_win",
        "actual_margin",
        "actual_total",
        "sdv_as_of_rule",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(
            "standalone_comparison.csv cannot prove historical SDV chronology; "
            f"missing columns: {missing}"
        )

    df = df.copy()
    df["game_id"] = canonical_game_id(df["game_id"])
    df["_date"] = normalize_date(df["game_date"])
    if df["_date"].isna().any():
        raise RuntimeError("standalone_comparison.csv has invalid game_date values.")

    rules = (
        df["sdv_as_of_rule"]
        .astype(str)
        .str.strip()
        .replace({"nan": ""})
    )
    expected_rule = "source_game_date < target_game_date"
    bad_rule = rules.ne(expected_rule)
    if bad_rule.any():
        values = sorted(rules[bad_rule].unique().tolist())
        raise RuntimeError(
            "Historical SDV chronology rule is not uniformly prior-only. "
            f"Expected {expected_rule!r}; found {values[:10]}"
        )

    for col in (
        "sdv_home_win_prob",
        "sdv_exp_margin",
        "sdv_exp_total",
        "actual_home_win",
        "actual_margin",
        "actual_total",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    incomplete = df[
        [
            "sdv_home_win_prob",
            "sdv_exp_margin",
            "sdv_exp_total",
            "actual_home_win",
            "actual_margin",
            "actual_total",
        ]
    ].isna().any(axis=1)
    if incomplete.any():
        raise RuntimeError(
            f"Historical SDV comparison has {int(incomplete.sum())} incomplete rows."
        )

    audit = {
        "rows": int(len(df)),
        "first_date": df["_date"].min().date().isoformat(),
        "last_date": df["_date"].max().date().isoformat(),
        "sdv_as_of_rule": expected_rule,
        "chronology_rule_verified_for_every_row": True,
    }
    return df, audit


def build_full_season_merged_inputs(
    season_master: Path,
    sdv_history: pd.DataFrame,
    fake_merge_dir: Path,
) -> dict[str, Any]:
    games = read_master(season_master / "games.csv", "season games")
    preds = read_master(season_master / "predictions.csv", "season predictions")
    book = read_master(season_master / "sportsbook.csv", "season sportsbook")

    required_games = {
        "game_id", "sport", "league", "game_date",
        "game_time", "home_team", "away_team",
    }
    required_preds = {
        "game_id", "home_prob_moneyline", "away_prob_moneyline",
        "away_projected_goals", "home_projected_goals",
        "total_projected_goals",
    }
    required_book = {"game_id", *SPORTSBOOK_FIELDS}

    for df, required_cols, label in [
        (games, required_games, "games.csv"),
        (preds, required_preds, "predictions.csv"),
        (book, required_book, "sportsbook.csv"),
    ]:
        missing = sorted(required_cols - set(df.columns))
        if missing:
            raise RuntimeError(f"{label} missing required columns: {missing}")

    base = games[
        [
            "game_id", "sport", "league", "game_date", "game_time",
            "away_team", "home_team",
        ]
    ].copy()

    pred_cols = [
        "game_id", "home_prob_moneyline", "away_prob_moneyline",
        "away_projected_goals", "home_projected_goals",
        "total_projected_goals",
    ]
    book_cols = ["game_id", *SPORTSBOOK_FIELDS]

    merged = base.merge(
        preds[pred_cols],
        on="game_id",
        how="left",
        validate="one_to_one",
    )
    merged = merged.merge(
        book[book_cols],
        on="game_id",
        how="left",
        validate="one_to_one",
    )

    # Insert only the three historical PRE-GAME SDV predictions.
    sdv_target = sdv_history[
        [
            "game_id",
            "game_date",
            "sdv_home_win_prob",
            "sdv_exp_margin",
            "sdv_exp_total",
        ]
    ].copy()
    sdv_target = sdv_target.rename(columns={"game_date": "_sdv_game_date"})

    merged = merged.merge(
        sdv_target,
        on="game_id",
        how="left",
        validate="one_to_one",
    )

    has_sdv = merged["sdv_home_win_prob"].notna()
    merged_date = normalize_date(merged["game_date"])
    sdv_date = normalize_date(merged["_sdv_game_date"])
    date_mismatch = has_sdv & sdv_date.ne(merged_date)
    if date_mismatch.any():
        sample = merged.loc[
            date_mismatch,
            ["game_id", "game_date", "_sdv_game_date"],
        ].head(10)
        raise RuntimeError(
            "Historical SDV target rows do not match season-master game dates: "
            f"{sample.to_dict(orient='records')}"
        )
    merged = merged.drop(columns=["_sdv_game_date"])

    decimal_pairs = [
        ("home_dk_moneyline_american", "home_dk_moneyline_decimal"),
        ("away_dk_moneyline_american", "away_dk_moneyline_decimal"),
        ("home_dk_puck_line_american", "home_dk_puck_line_decimal"),
        ("away_dk_puck_line_american", "away_dk_puck_line_decimal"),
        ("dk_total_over_american", "dk_total_over_decimal"),
        ("dk_total_under_american", "dk_total_under_decimal"),
    ]
    decimals_derived = {}
    for american_col, decimal_col in decimal_pairs:
        existing = pd.to_numeric(merged[decimal_col], errors="coerce")
        derived = merged[american_col].map(american_to_decimal)
        fill_mask = existing.isna() & derived.notna()
        decimals_derived[decimal_col] = int(fill_mask.sum())
        merged[decimal_col] = existing.where(~fill_mask, derived)

    # These features did not exist in the 2025-26 season master. Leave blank.
    unavailable_feature_cols = (
        FATIGUE_FEATURE_COLUMNS
        + TEAM_STRENGTH_FEATURE_COLUMNS
        + GOALIE_FEATURE_COLUMNS
        + LINEUP_FEATURE_COLUMNS
    )
    for col in unavailable_feature_cols:
        if col not in merged.columns:
            merged[col] = pd.NA

    # Do not fabricate an odds timestamp or provider identity.
    merged["odds_source"] = "legacy_2025_26_season_master"
    merged["moneyline_provider_id"] = ""
    merged["moneyline_provider_name"] = "legacy_archive"
    merged["puck_line_provider_id"] = ""
    merged["puck_line_provider_name"] = "legacy_archive"
    merged["total_provider_id"] = ""
    merged["total_provider_name"] = "legacy_archive"
    merged["pulled_at"] = ""

    columns = [
        "sport", "league", "game_date", "game_time", "game_id",
        "away_team", "home_team",
        *FATIGUE_FEATURE_COLUMNS,
        *TEAM_STRENGTH_FEATURE_COLUMNS,
        *GOALIE_FEATURE_COLUMNS,
        *LINEUP_FEATURE_COLUMNS,
        *SDV_PREDICTION_COLUMNS,
        "away_prob_moneyline", "home_prob_moneyline",
        "away_projected_goals", "home_projected_goals",
        "total_projected_goals",
        "away_puck_line", "home_puck_line", "total",
        "away_dk_moneyline_american", "home_dk_moneyline_american",
        "away_dk_moneyline_decimal", "home_dk_moneyline_decimal",
        "away_dk_puck_line_american", "home_dk_puck_line_american",
        "away_dk_puck_line_decimal", "home_dk_puck_line_decimal",
        "dk_total_over_american", "dk_total_under_american",
        "dk_total_over_decimal", "dk_total_under_decimal",
        *PROVENANCE_FIELDS,
    ]
    for col in columns:
        if col not in merged.columns:
            merged[col] = pd.NA
    merged = merged[columns]

    dates = merged["game_date"].astype(str).str.strip()
    valid_dates = dates.str.fullmatch(r"\d{4}_\d{2}_\d{2}")
    if not valid_dates.all():
        bad = sorted(dates[~valid_dates].unique().tolist())
        raise RuntimeError(f"Invalid game_date values: {bad[:20]}")

    fake_merge_dir.mkdir(parents=True, exist_ok=True)
    files_written = 0
    for game_date, part in merged.groupby("game_date", sort=True):
        out = fake_merge_dir / f"{game_date}_NHL_merged.csv"
        part.sort_values(["game_time", "game_id"]).to_csv(out, index=False)
        files_written += 1

    feature_rows = []
    for col in unavailable_feature_cols:
        feature_rows.append(
            {
                "feature": col,
                "historical_status": "unavailable",
                "available_rows": int(merged[col].notna().sum()),
                "total_rows": int(len(merged)),
                "source": "not present in 2025-26 season master; left blank",
            }
        )
    for col in SDV_PREDICTION_COLUMNS:
        feature_rows.append(
            {
                "feature": col,
                "historical_status": "partially_available",
                "available_rows": int(merged[col].notna().sum()),
                "total_rows": int(len(merged)),
                "source": (
                    "research/sdv_challenger/season_2025/"
                    "standalone_comparison.csv; verified prior-only SDV rule"
                ),
            }
        )
    feature_rows.append(
        {
            "feature": "pulled_at",
            "historical_status": "unavailable",
            "available_rows": 0,
            "total_rows": int(len(merged)),
            "source": "season master has no trustworthy historical pull timestamp; left blank",
        }
    )

    coverage = {
        "games": int(len(games)),
        "prediction_rows": int(len(preds)),
        "sportsbook_rows": int(len(book)),
        "merged_rows": int(len(merged)),
        "merged_files": int(files_written),
        "first_date": dates.min(),
        "last_date": dates.max(),
        "games_with_prediction": int(
            pd.to_numeric(
                merged["home_prob_moneyline"], errors="coerce"
            ).notna().sum()
        ),
        "games_with_historical_sdv": int(
            pd.to_numeric(
                merged["sdv_home_win_prob"], errors="coerce"
            ).notna().sum()
        ),
        "games_without_historical_sdv": int(
            pd.to_numeric(
                merged["sdv_home_win_prob"], errors="coerce"
            ).isna().sum()
        ),
        "games_with_moneyline": int(
            pd.to_numeric(
                merged["home_dk_moneyline_american"], errors="coerce"
            ).notna().sum()
        ),
        "games_with_puck_line": int(
            pd.to_numeric(
                merged["home_puck_line"], errors="coerce"
            ).notna().sum()
        ),
        "games_with_total": int(
            pd.to_numeric(
                merged["total"], errors="coerce"
            ).notna().sum()
        ),
        "derived_decimal_odds_counts": decimals_derived,
        "historical_feature_policy": (
            "missing historical features remain blank; no future or synthetic "
            "feature reconstruction"
        ),
        "season_master_cutoff_timestamp_status": (
            "unavailable; source rows are the existing historical replay masters "
            "and no timestamp is fabricated"
        ),
    }
    return {"coverage": coverage, "feature_rows": feature_rows}


def copy_secondary_history(
    source_path: Path,
    fake_nhl: Path,
) -> Path:
    dst = (
        fake_nhl
        / "research"
        / "sdv_challenger"
        / "season_2025"
        / "standalone_comparison.csv"
    )
    copy_file(source_path, dst)
    return dst


def copy_final_scores_and_build_status(
    archived_scores: Path,
    fake_final_root: Path,
) -> dict[str, Any]:
    score_dst = fake_final_root / "final_scores"
    score_dst.mkdir(parents=True, exist_ok=True)

    files = sorted(archived_scores.glob("*_NHL_final_scores.csv"))
    if not files:
        raise RuntimeError(f"No archived final scores in {archived_scores}")

    status_parts = []
    score_rows = 0

    for src in files:
        df = pd.read_csv(src, dtype={"game_id": str})
        if df.empty:
            continue

        required_cols = [
            "sport", "league", "game_date", "game_id",
            "away_team", "home_team",
            "away_score", "home_score",
            "total_score", "away_puck_line_result",
            "home_puck_line_result",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise RuntimeError(f"{src} missing columns: {missing}")

        shutil.copy2(src, score_dst / src.name)
        score_rows += len(df)

        status = df[
            [
                "sport", "league", "game_date", "game_id",
                "away_team", "home_team",
            ]
        ].copy()
        status["game_state"] = "FINAL"
        status["game_schedule_state"] = "FINAL"
        status["is_final"] = "true"
        status["status_observed_at"] = "historical_2025_26_archive"
        status_parts.append(status)

    if not status_parts:
        raise RuntimeError("Archived final-score files contained no rows.")

    status_df = pd.concat(status_parts, ignore_index=True)
    status_df["game_id"] = canonical_game_id(status_df["game_id"])
    duplicates = status_df[
        status_df.duplicated(subset=["game_id"], keep=False)
    ]
    if not duplicates.empty:
        raise RuntimeError(
            "Duplicate final-score game_id values: "
            + ", ".join(
                sorted(duplicates["game_id"].unique().tolist())[:20]
            )
        )

    intermediate = fake_final_root / "intermediate"
    intermediate.mkdir(parents=True, exist_ok=True)
    status_df.to_csv(
        intermediate / "nhl_game_status.csv",
        index=False,
    )

    return {
        "final_score_files": int(len(files)),
        "final_score_rows": int(score_rows),
        "final_score_first_date": status_df["game_date"].astype(str).min(),
        "final_score_last_date": status_df["game_date"].astype(str).max(),
    }


PUCK_LINE_SECONDARY_PROBABILITY_COLUMNS = [
    "sdv_home_cover_prob_puck_line",
    "sdv_away_cover_prob_puck_line",
    "weighted_home_cover_prob_puck_line",
    "weighted_away_cover_prob_puck_line",
    "meta_home_cover_prob_puck_line",
    "meta_away_cover_prob_puck_line",
]


def suppress_isolated_ev_performance_warnings(fake_nhl: Path) -> dict[str, Any]:
    """
    Suppress only pandas PerformanceWarning in the TEMPORARY replay copy of
    compute_ev_kelly.py. Production/local pipeline files are not modified.
    """
    path = (
        fake_nhl
        / "scripts"
        / "03_edges"
        / "compute_ev_kelly.py"
    )
    require(path, "isolated compute_ev_kelly.py")

    text = path.read_text(encoding="utf-8")

    if "pd.errors.PerformanceWarning" in text:
        return {
            "patched": False,
            "reason": "warning filter already present",
            "path": str(path),
        }

    marker = "import pandas as pd"
    if marker not in text:
        raise RuntimeError(
            "Could not safely install PerformanceWarning filter in isolated "
            "compute_ev_kelly.py: pandas import marker not found."
        )

    replacement = (
        marker
        + "\nimport warnings\n"
        + "warnings.filterwarnings("
        + '"ignore", category=pd.errors.PerformanceWarning'
        + ")\n"
    )

    text = text.replace(marker, replacement, 1)
    path.write_text(text, encoding="utf-8")

    return {
        "patched": True,
        "warning_class": "pandas.errors.PerformanceWarning",
        "scope": "isolated temporary compute_ev_kelly.py only",
        "path": str(path),
    }


def ensure_selector_secondary_schema(
    secondary_dir: Path,
) -> dict[str, Any]:
    """
    Selector requires six puck-line secondary probability columns that the
    current secondary builder does not emit.

    Never fabricate them. Add them blank and make otherwise-ready puck-line
    rows unavailable so markets.yaml use_primary fallback is exercised.
    """
    files = sorted(secondary_dir.glob("*_NHL_*.csv"))
    if not files:
        raise RuntimeError(
            f"No secondary signal files found in {secondary_dir}"
        )

    files_changed = 0
    columns_added = 0
    puck_line_rows_downgraded = 0
    affected_files: list[str] = []

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})

        missing = [
            col
            for col in PUCK_LINE_SECONDARY_PROBABILITY_COLUMNS
            if col not in df.columns
        ]

        if not missing:
            continue

        if missing:
            # Add all compatibility columns in one operation to avoid pandas
            # DataFrame fragmentation / PerformanceWarning spam.
            df = df.copy()
            blank_columns = pd.DataFrame(
                {
                    col: pd.Series(pd.NA, index=df.index, dtype="object")
                    for col in missing
                },
                index=df.index,
            )
            df = pd.concat([df, blank_columns], axis=1)
            columns_added += len(missing)

        if path.name.endswith("_NHL_puck_line.csv"):
            if "secondary_model_status" not in df.columns:
                raise RuntimeError(
                    f"{path.name} missing secondary_model_status"
                )

            ready = (
                df["secondary_model_status"]
                .astype(str)
                .eq("ready")
            )

            puck_line_rows_downgraded += int(ready.sum())

            df.loc[
                ready,
                "secondary_model_status",
            ] = "puck_line_probability_unavailable"

        df.to_csv(path, index=False)
        files_changed += 1
        affected_files.append(path.name)

    return {
        "required_columns": PUCK_LINE_SECONDARY_PROBABILITY_COLUMNS,
        "policy": (
            "missing selector-required puck-line secondary probabilities "
            "remain blank; no historical values are fabricated"
        ),
        "puck_line_behavior": (
            "otherwise-ready puck-line rows are marked "
            "puck_line_probability_unavailable so Stage 04 uses use_primary"
        ),
        "moneyline_secondary_remains_enabled": True,
        "total_secondary_remains_enabled": True,
        "files_changed": files_changed,
        "column_insertions": columns_added,
        "puck_line_rows_marked_unavailable": puck_line_rows_downgraded,
        "affected_files": affected_files,
    }


def load_secondary_outputs(
    secondary_dir: Path,
) -> tuple[pd.DataFrame, dict[str, int]]:
    files = sorted(secondary_dir.glob("*_NHL_*.csv"))
    if not files:
        raise RuntimeError(
            f"build_secondary_model_signals.py produced no files in {secondary_dir}"
        )

    frames = []
    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        if df.empty:
            continue
        needed = {
            "game_id",
            "game_date",
            "secondary_model_status",
            "secondary_history_max_game_date",
        }
        missing = sorted(needed - set(df.columns))
        if missing:
            raise RuntimeError(
                f"Secondary output {path.name} missing columns: {missing}"
            )
        frames.append(
            df[
                [
                    "game_id",
                    "game_date",
                    "secondary_model_status",
                    "secondary_history_max_game_date",
                ]
            ].copy()
        )

    if not frames:
        raise RuntimeError("Secondary output files contain no rows.")

    combined = pd.concat(frames, ignore_index=True)
    combined["game_id"] = canonical_game_id(combined["game_id"])

    # Same game appears in each market file; collapse after confirming status/history.
    conflicts = (
        combined.groupby("game_id")[
            ["secondary_model_status", "secondary_history_max_game_date"]
        ]
        .nunique(dropna=False)
    )
    bad = conflicts[
        (conflicts["secondary_model_status"] > 1)
        | (conflicts["secondary_history_max_game_date"] > 1)
    ]
    if not bad.empty:
        raise RuntimeError(
            "Secondary signal status/history differs across market files for "
            f"{len(bad)} game_ids."
        )

    games = combined.drop_duplicates("game_id", keep="first").copy()
    target_date = normalize_date(games["game_date"])
    history_date = normalize_date(games["secondary_history_max_game_date"])

    ready = games["secondary_model_status"].astype(str).eq("ready")
    if games.loc[ready, "secondary_history_max_game_date"].isna().any():
        raise RuntimeError(
            "Ready secondary rows contain blank secondary_history_max_game_date."
        )

    leakage = ready & (
        history_date.isna()
        | target_date.isna()
        | history_date.ge(target_date)
    )
    if leakage.any():
        sample = games.loc[
            leakage,
            [
                "game_id", "game_date", "secondary_model_status",
                "secondary_history_max_game_date",
            ],
        ].head(20)
        raise RuntimeError(
            "Secondary historical leakage assertion failed: "
            f"{sample.to_dict(orient='records')}"
        )

    statuses = (
        games["secondary_model_status"]
        .fillna("<blank>")
        .astype(str)
        .value_counts()
        .to_dict()
    )
    statuses = {str(k): int(v) for k, v in statuses.items()}

    games["_target_date"] = target_date
    games["_history_date"] = history_date
    return games, statuses


def count_csv_rows(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        try:
            total += len(pd.read_csv(path))
        except pd.errors.EmptyDataError:
            pass
    return total


def read_selected_decisions(selected_dir: Path) -> tuple[pd.DataFrame, dict[str, int]]:
    files = sorted(selected_dir.glob("*_NHL.csv"))
    frames = []
    for path in files:
        try:
            df = pd.read_csv(path, dtype={"game_id": str})
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        df["_source_file"] = path.name
        frames.append(df)

    if not frames:
        return pd.DataFrame(), {}

    selected = pd.concat(frames, ignore_index=True)
    if "secondary_decision" in selected.columns:
        counts = (
            selected["secondary_decision"]
            .fillna("<blank>")
            .astype(str)
            .value_counts()
            .to_dict()
        )
        counts = {str(k): int(v) for k, v in counts.items()}
    else:
        counts = {}
    return selected, counts


def read_secondary_rejections(error_dir: Path) -> tuple[int, pd.DataFrame]:
    path = error_dir / "selection_rejections.csv"
    if not path.exists():
        return 0, pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty or "failing_condition" not in df.columns:
        return 0, df
    secondary = df[
        df["failing_condition"].astype(str).eq("secondary_model")
    ].copy()
    if "rejection_count" in secondary.columns:
        total = int(
            pd.to_numeric(
                secondary["rejection_count"], errors="coerce"
            ).fillna(0).sum()
        )
    else:
        total = int(len(secondary))
    return total, secondary


def read_config(path: Path) -> dict[str, Any]:
    require(path, "production markets.yaml")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    try:
        config = payload["markets"]["nhl"]
        secondary = config["secondary_model"]
    except Exception as exc:
        raise RuntimeError(
            f"Invalid NHL secondary config in {path}: {exc}"
        )
    if not bool(secondary.get("enabled", False)):
        raise RuntimeError(
            "Item 15 requires production secondary_model.enabled=true."
        )
    return config


def main() -> int:
    repo_root = find_repo_root()
    nhl = repo_root / NHL_REL

    season_master = nhl / "season_master" / SEASON
    archived_scores = (
        nhl / "archive" / "2025_26" / "05_final_scores" / "final_scores"
    )
    production_config = nhl / "config" / "markets.yaml"
    sdv_history_path = (
        nhl
        / "research"
        / "sdv_challenger"
        / "season_2025"
        / "standalone_comparison.csv"
    )
    output_root = nhl / "backtest" / "2025_2026_secondary_enabled"

    scripts = [
        Path("scripts/01_merge/build_juice_files.py"),
        Path("scripts/02_juice/apply_moneyline_juice.py"),
        Path("scripts/02_juice/apply_puck_line_juice.py"),
        Path("scripts/02_juice/apply_total_juice.py"),
        Path("scripts/03_edges/compute_edges.py"),
        Path("scripts/03_edges/compute_ev_kelly.py"),
        Path("scripts/03_edges/build_secondary_model_signals.py"),
        Path("scripts/04_select/hockey_select_bets.py"),
        Path("scripts/05_final_scores/01_nhl_results_grade.py"),
        Path("scripts/05_final_scores/02_nhl_results_analyze.py"),
        Path("scripts/05_final_scores/03_nhl_results_reports.py"),
    ]

    require(season_master, "season master")
    require(archived_scores, "archived final scores")
    require(sdv_history_path, "season_2025 standalone comparison")
    config = read_config(production_config)

    for rel in scripts:
        require(nhl / rel, rel.as_posix())

    live_select = nhl / "04_select"
    live_select_before = directory_manifest(live_select)

    sdv_history, sdv_audit = load_historical_sdv(sdv_history_path)

    temp_root = Path(
        tempfile.mkdtemp(prefix="nhl_item15_secondary_replay_")
    )
    fake_nhl = temp_root / NHL_REL

    print(f"Repository: {repo_root}")
    print(f"Isolated replay: {temp_root}")
    print(
        "Secondary config: "
        f"enabled={config['secondary_model']['enabled']} "
        f"mode={config['secondary_model']['selection_mode']} "
        f"unavailable={config['secondary_model']['unavailable_behavior']}"
    )

    try:
        # Copy the current LOCAL pipeline scripts, including any validated local
        # hardening not yet represented in the remote repository.
        for rel in scripts:
            copy_file(nhl / rel, fake_nhl / rel)

        # Suppress pandas fragmentation PerformanceWarning messages only in
        # the isolated replay copy.
        performance_warning_patch = suppress_isolated_ev_performance_warnings(
            fake_nhl
        )

        # Copy the entire local config tree, preserving the exact current
        # production markets.yaml and juice/config dependencies.
        copy_tree(nhl / "config", fake_nhl / "config")

        # Copy only the verified season_2025 comparison history needed by the
        # production secondary builder.
        fake_history_path = copy_secondary_history(
            sdv_history_path,
            fake_nhl,
        )

        built = build_full_season_merged_inputs(
            season_master,
            sdv_history,
            fake_nhl / "01_merge",
        )
        coverage = built["coverage"]
        feature_rows = built["feature_rows"]

        print(
            "Season master coverage: "
            f"{coverage['first_date']} -> {coverage['last_date']} | "
            f"games={coverage['games']} | "
            f"historical_sdv={coverage['games_with_historical_sdv']} | "
            f"sdv_unavailable={coverage['games_without_historical_sdv']}"
        )

        # Stages 01 -> 03.
        for rel in scripts[:6]:
            run_script(temp_root, rel)

        # True production secondary signal construction.
        run_script(
            temp_root,
            Path("scripts/03_edges/build_secondary_model_signals.py"),
        )

        secondary_games, secondary_status_counts = load_secondary_outputs(
            fake_nhl / "03_edges" / "secondary_signals"
        )

        # Selector schema currently expects six puck-line probability fields
        # that the builder cannot historically supply. Keep them unavailable
        # rather than fabricating values.
        selector_schema_compatibility = ensure_selector_secondary_schema(
            fake_nhl / "03_edges" / "secondary_signals"
        )

        score_coverage = copy_final_scores_and_build_status(
            archived_scores,
            fake_nhl / "05_final_scores",
        )

        # Stage 04 + Stage 05.
        for rel in scripts[7:]:
            run_script(temp_root, rel)

        selected, decision_counts = read_selected_decisions(
            fake_nhl / "04_select"
        )
        secondary_rejection_count, secondary_rejections = (
            read_secondary_rejections(
                fake_nhl / "errors" / "04_select"
            )
        )

        # Verify the live production 04_select directory was untouched.
        live_select_after = directory_manifest(live_select)
        if live_select_before != live_select_after:
            raise RuntimeError(
                "LIVE 04_select DIRECTORY CHANGED DURING ITEM 15. "
                "Replay output has not been published."
            )

        # Publish only to the dedicated isolated backtest output.
        if output_root.exists():
            shutil.rmtree(output_root)
        output_root.mkdir(parents=True, exist_ok=True)

        copy_map = [
            ("01_merge", "workspace/01_merge"),
            ("02_juice", "workspace/02_juice"),
            ("03_edges", "workspace/03_edges"),
            ("04_select", "selected"),
            ("05_final_scores/graded", "graded"),
            ("05_final_scores/intermediate", "intermediate"),
            ("05_final_scores/reports", "reports"),
            ("errors/01_merge", "errors/01_merge"),
            ("errors/02_juice", "errors/02_juice"),
            ("errors/03_edges", "errors/03_edges"),
            ("errors/04_select", "errors/04_select"),
            ("05_final_scores/errors", "errors/05_final_scores"),
        ]
        for src_rel, dst_rel in copy_map:
            src = fake_nhl / src_rel
            if src.exists():
                copy_tree(src, output_root / dst_rel)

        tally = fake_nhl / "05_final_scores" / "nhl_market_tally.csv"
        if tally.exists():
            shutil.copy2(tally, output_root / "nhl_market_tally.csv")

        feature_availability = pd.DataFrame(feature_rows)
        feature_availability.to_csv(
            output_root / "item15_feature_availability.csv",
            index=False,
        )

        secondary_games[
            [
                "game_id",
                "game_date",
                "secondary_model_status",
                "secondary_history_max_game_date",
            ]
        ].to_csv(
            output_root / "item15_secondary_status_by_game.csv",
            index=False,
        )

        leakage_audit = pd.DataFrame(
            [
                {
                    "check": "ready_history_strictly_before_target",
                    "rows_checked": int(
                        secondary_games[
                            "secondary_model_status"
                        ].astype(str).eq("ready").sum()
                    ),
                    "violations": 0,
                    "rule": "secondary_history_max_game_date < game_date",
                    "passed": True,
                },
                {
                    "check": "sdv_source_as_of_rule",
                    "rows_checked": int(sdv_audit["rows"]),
                    "violations": 0,
                    "rule": sdv_audit["sdv_as_of_rule"],
                    "passed": True,
                },
                {
                    "check": "live_04_select_unchanged",
                    "rows_checked": int(len(live_select_before)),
                    "violations": 0,
                    "rule": "before_manifest == after_manifest",
                    "passed": True,
                },
            ]
        )
        leakage_audit.to_csv(
            output_root / "item15_leakage_audit.csv",
            index=False,
        )

        if not secondary_rejections.empty:
            secondary_rejections.to_csv(
                output_root / "item15_secondary_gate_rejections.csv",
                index=False,
            )
        else:
            pd.DataFrame(
                columns=[
                    "game_date",
                    "market_type",
                    "bet_side",
                    "failing_condition",
                    "rejection_count",
                ]
            ).to_csv(
                output_root / "item15_secondary_gate_rejections.csv",
                index=False,
            )

        selected_files = sorted(
            (fake_nhl / "04_select").glob("*_NHL.csv")
        )
        selected_rows = count_csv_rows(selected_files)

        graded_daily_files = sorted(
            (fake_nhl / "05_final_scores" / "graded").glob(
                "*_results_NHL.csv"
            )
        )
        graded_daily_rows = count_csv_rows(graded_daily_files)

        report = {
            "script_version": SCRIPT_VERSION,
            "status": "COMPLETE",
            "purpose": (
                "Item 15 true end-to-end 2025-26 historical replay with "
                "production secondary-model selection enabled."
            ),
            "pipeline_executed": [
                "Stage 01 probability construction",
                "Stage 02 calibration/juice",
                "Stage 03 edges",
                "Stage 03 EV/Kelly",
                "build_secondary_model_signals.py",
                "Stage 04 final selection",
                "Stage 05 grading",
                "Stage 05 analysis",
                "Stage 05 reports",
            ],
            "existing_replay_secondary_limitation_confirmed": True,
            "existing_replay_limitation": (
                "run_2025_2026_replay.py bypasses build_secondary_model_signals.py "
                "and injects secondary_model_status=backtest_disabled before Stage 04."
            ),
            "secondary_config": config["secondary_model"],
            "secondary_enabled": True,
            "secondary_builder_source": (
                "current local scripts/03_edges/build_secondary_model_signals.py"
            ),
            "isolated_performance_warning_patch": performance_warning_patch,
            "selector_schema_compatibility": selector_schema_compatibility,
            "historical_sdv_source": str(sdv_history_path),
            "historical_sdv_copy_in_isolated_workspace": str(
                fake_history_path
            ),
            "historical_sdv_audit": sdv_audit,
            "season_master_coverage": coverage,
            "feature_availability_file": str(
                output_root / "item15_feature_availability.csv"
            ),
            "missing_historical_feature_policy": (
                "unavailable values remain blank/unavailable; no fabrication"
            ),
            "secondary_status_counts_by_game": secondary_status_counts,
            "secondary_ready_rows": int(
                secondary_games[
                    "secondary_model_status"
                ].astype(str).eq("ready").sum()
            ),
            "secondary_selected_decision_counts": decision_counts,
            "secondary_gate_rejection_count": int(
                secondary_rejection_count
            ),
            "secondary_gate_exercised": bool(
                decision_counts
                or secondary_rejection_count > 0
            ),
            "leakage_rule": (
                "secondary history for target D must satisfy "
                "historical_game_date < D"
            ),
            "leakage_violations": 0,
            "future_secondary_inputs_used": False,
            "historical_features_fabricated": False,
            "season_master_cutoff_timestamp_note": (
                "The legacy season master does not contain a trustworthy pulled_at "
                "timestamp. Item 15 uses the existing historical replay masters as-is "
                "and leaves pulled_at blank rather than inventing a cutoff timestamp."
            ),
            "final_score_usage": (
                "grading and prior-only secondary training outcomes; target-date "
                "secondary fitting is enforced by build_secondary_model_signals.py"
            ),
            "final_score_coverage": score_coverage,
            "selected_files": int(len(selected_files)),
            "selected_rows": int(selected_rows),
            "graded_daily_files": int(len(graded_daily_files)),
            "graded_daily_rows": int(graded_daily_rows),
            "live_04_select_before_files": int(len(live_select_before)),
            "live_04_select_after_files": int(len(live_select_after)),
            "live_04_select_unchanged": True,
            "output_root": str(output_root),
            "production_change": "none",
            "promotion_decision": None,
        }

        report_path = output_root / "item15_report.json"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )

        summary_lines = [
            "NHL ITEM 15 — SECONDARY-ENABLED END-TO-END REPLAY",
            "================================================",
            f"script_version={SCRIPT_VERSION}",
            "status=COMPLETE",
            f"season_master_first_date={coverage['first_date']}",
            f"season_master_last_date={coverage['last_date']}",
            f"season_master_games={coverage['games']}",
            f"games_with_historical_sdv={coverage['games_with_historical_sdv']}",
            f"games_without_historical_sdv={coverage['games_without_historical_sdv']}",
            f"secondary_status_counts={secondary_status_counts}",
            f"secondary_ready_rows={report['secondary_ready_rows']}",
            f"secondary_decision_counts={decision_counts}",
            f"secondary_gate_rejection_count={secondary_rejection_count}",
            f"selected_rows={selected_rows}",
            f"graded_daily_rows={graded_daily_rows}",
            "leakage_violations=0",
            "historical_features_fabricated=false",
            "live_04_select_unchanged=true",
            f"output={output_root}",
            "",
        ]
        (output_root / "replay_summary.txt").write_text(
            "\n".join(summary_lines),
            encoding="utf-8",
        )

        print()
        print(
            "ITEM 15 SECONDARY-ENABLED REPLAY COMPLETE: "
            f"games={coverage['games']} "
            f"sdv_available={coverage['games_with_historical_sdv']} "
            f"ready={report['secondary_ready_rows']} "
            f"secondary_rejections={secondary_rejection_count} "
            f"selected={selected_rows} "
            f"graded={graded_daily_rows}"
        )
        print(f"Secondary statuses: {secondary_status_counts}")
        print(f"Selected decisions: {decision_counts}")
        print(f"Output: {output_root}")
        print(f"Report: {report_path}")

    except Exception:
        print()
        print("ITEM 15 SECONDARY-ENABLED REPLAY FAILED")
        print(f"Temporary replay retained: {temp_root}")
        raise
    else:
        shutil.rmtree(temp_root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())



