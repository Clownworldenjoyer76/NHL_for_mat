#!/usr/bin/env python3
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


FATIGUE_FEATURE_COLUMNS = [
    "home_days_rest",
    "away_days_rest",
    "home_back_to_back",
    "away_back_to_back",
    "home_games_in_4_days",
    "away_games_in_4_days",
    "home_three_in_four",
    "away_three_in_four",
    "home_games_in_6_days",
    "away_games_in_6_days",
    "home_four_in_six",
    "away_four_in_six",
    "home_games_in_7_days",
    "away_games_in_7_days",
    "rest_differential",
]

TEAM_STRENGTH_FEATURE_COLUMNS = [
    "home_adj_xgf",
    "away_adj_xgf",
    "adj_xgf_differential",
    "home_adj_xga",
    "away_adj_xga",
    "adj_xga_differential",
    "home_adj_xg_net",
    "away_adj_xg_net",
    "adj_xg_net_differential",
    "home_adj_gf",
    "away_adj_gf",
    "adj_gf_differential",
    "home_adj_ga",
    "away_adj_ga",
    "adj_ga_differential",
    "home_off_rank",
    "away_off_rank",
    "off_rank_differential",
    "home_def_rank",
    "away_def_rank",
    "def_rank_differential",
    "home_net_rank",
    "away_net_rank",
    "net_rank_differential",
    "home_net_z",
    "away_net_z",
    "net_z_differential",
]

LINEUP_NUMERIC_FEATURE_COLUMNS = [
    "home_skater_rapm",
    "away_skater_rapm",
    "skater_rapm_differential",
    "home_skater_war",
    "away_skater_war",
    "skater_war_differential",
    "home_pp_value",
    "away_pp_value",
    "pp_value_differential",
    "home_pk_value",
    "away_pk_value",
    "pk_value_differential",
    "home_forward_line_strength",
    "away_forward_line_strength",
    "forward_line_strength_differential",
    "home_defense_pair_strength",
    "away_defense_pair_strength",
    "defense_pair_strength_differential",
]

LINEUP_METADATA_COLUMNS = [
    "home_lineup_status",
    "away_lineup_status",
    "home_lineup_observed_at",
    "away_lineup_observed_at",
    "home_lineup_source",
    "away_lineup_source",
]

LINEUP_FEATURE_COLUMNS = [
    *LINEUP_NUMERIC_FEATURE_COLUMNS,
    *LINEUP_METADATA_COLUMNS,
]

GOALIE_FEATURE_COLUMNS = [
    "home_expected_starter",
    "away_expected_starter",
    "home_starter_gsax",
    "away_starter_gsax",
    "home_backup_gsax",
    "away_backup_gsax",
    "starter_gsax_differential",
    "home_goalie_status",
    "away_goalie_status",
    "home_goalie_status_observed_at",
    "away_goalie_status_observed_at",
    "home_goalie_status_source",
    "away_goalie_status_source",
]

GOALIE_NUMERIC_FEATURE_COLUMNS = [
    "home_starter_gsax",
    "away_starter_gsax",
    "home_backup_gsax",
    "away_backup_gsax",
    "starter_gsax_differential",
]


def make_logger(
    log_file: Path,
    run_name: str,
) -> tuple[Callable[[], None], Callable[[str], None]]:
    def now() -> str:
        return datetime.now(UTC).isoformat()

    def reset_log() -> None:
        with log_file.open("w", encoding="utf-8") as handle:
            handle.write(f"=== {run_name} RUN {now()} ===\n")

    def log(message: str) -> None:
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(f"{now()} | {message}\n")

    return reset_log, log


def validate_columns(
    path: Path,
    df: pd.DataFrame,
    required_columns: list[str],
) -> None:
    missing = [
        column
        for column in required_columns
        if column not in df.columns
    ]
    if missing:
        raise ValueError(
            f"{path} missing required columns: {missing}"
        )


def load_juice_config(
    juice_file: Path,
    required_columns: list[str],
    *,
    text_columns: list[str],
) -> pd.DataFrame:
    if not juice_file.exists():
        raise FileNotFoundError(
            f"Missing config file: {juice_file}"
        )

    juice_df = pd.read_csv(juice_file)
    validate_columns(
        juice_file,
        juice_df,
        required_columns,
    )

    for column in (
        "band_min",
        "band_max",
        "model_calibration_adjustment",
    ):
        juice_df[column] = pd.to_numeric(
            juice_df[column],
            errors="coerce",
        )

    for column in text_columns:
        juice_df[column] = (
            juice_df[column]
            .astype(str)
            .str.strip()
        )

    if (
        juice_df[
            [
                "band_min",
                "band_max",
                "model_calibration_adjustment",
            ]
        ]
        .isna()
        .any()
        .any()
    ):
        raise ValueError(
            f"{juice_file} has non-numeric "
            "band_min, band_max, or model_calibration_adjustment values"
        )

    return juice_df


def wipe_market_outputs(
    output_dir: Path,
    error_dir: Path,
    *,
    output_glob: str,
    quarantine_glob: str,
    label: str,
    log: Callable[[str], None],
) -> int:
    removed = 0

    for path in output_dir.glob(output_glob):
        path.unlink()
        removed += 1

    for path in error_dir.glob(quarantine_glob):
        path.unlink()
        removed += 1

    log(
        f"Wiped {label} output/quarantine CSVs: {removed}"
    )
    return removed


def quarantine_row(
    original_df: pd.DataFrame,
    idx: Any,
    reason: str,
    quarantine_rows: list[dict],
) -> None:
    rejected = original_df.loc[idx].to_dict()
    rejected["rejection_reason"] = reason
    quarantine_rows.append(rejected)


def write_quarantine(
    error_dir: Path,
    path: Path,
    original_columns: list[str],
    quarantine_rows: list[dict],
) -> Path | None:
    quarantine_path = (
        error_dir
        / f"{path.stem}_quarantine.csv"
    )

    if not quarantine_rows:
        if quarantine_path.exists():
            quarantine_path.unlink()
        return None

    quarantine_df = pd.DataFrame(
        quarantine_rows
    ).reindex(
        columns=(
            original_columns
            + ["rejection_reason"]
        )
    )

    quarantine_df.to_csv(
        quarantine_path,
        index=False,
    )
    return quarantine_path


def finalize_processed_file(
    *,
    path: Path,
    df: pd.DataFrame,
    original_df: pd.DataFrame,
    accepted_indices: list[Any],
    output_columns: list[str],
    output_dir: Path,
    error_dir: Path,
    quarantine_rows: list[dict],
    applied: int,
    skipped_bad: int,
    skipped_noband: int,
    log: Callable[[str], None],
) -> tuple[int, int, int]:
    out_path = output_dir / path.name

    accepted_df = df.loc[
        accepted_indices,
        output_columns,
    ].copy()

    accepted_df.to_csv(
        out_path,
        index=False,
    )

    quarantine_path = write_quarantine(
        error_dir,
        path,
        list(original_df.columns),
        quarantine_rows,
    )

    log(
        f"WROTE {out_path} "
        f"rows={len(accepted_df)} "
        f"applied={applied}"
    )

    if quarantine_path is not None:
        log(
            f"WROTE {quarantine_path} "
            f"rows={len(quarantine_rows)}"
        )

    log(
        f"FILE SUMMARY: {path.name} "
        f"input={len(original_df)} "
        f"accepted={len(accepted_df)} "
        f"quarantined={len(quarantine_rows)} "
        f"bad={skipped_bad} "
        f"no_band={skipped_noband}"
    )

    return (
        applied,
        skipped_bad,
        skipped_noband,
    )


def run_input_files(
    *,
    input_files: list[Path],
    juice_df: pd.DataFrame,
    process_file: Callable[
        [Path, pd.DataFrame],
        tuple[int, int, int],
    ],
    log: Callable[[str], None],
) -> None:
    files_written = 0
    total_applied = 0
    total_skipped_bad = 0
    total_skipped_noband = 0

    for path in input_files:
        log(f"Processing input: {path}")

        (
            applied,
            skipped_bad,
            skipped_noband,
        ) = process_file(
            path,
            juice_df,
        )

        files_written += 1
        total_applied += applied
        total_skipped_bad += skipped_bad
        total_skipped_noband += skipped_noband

    total_quarantined = (
        total_skipped_bad
        + total_skipped_noband
    )

    log("--- SUMMARY ---")
    log(f"Files processed: {len(input_files)}")
    log(f"Files written: {files_written}")
    log(f"Rows applied: {total_applied}")
    log(
        f"Rows quarantined bad: "
        f"{total_skipped_bad}"
    )
    log(
        f"Rows quarantined no band: "
        f"{total_skipped_noband}"
    )
    log(
        f"Rows quarantined total: "
        f"{total_quarantined}"
    )
    log("STATUS: SUCCESS")
