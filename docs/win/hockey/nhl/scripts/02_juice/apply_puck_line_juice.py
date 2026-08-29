#!/usr/bin/env python3
# hockey/nhl/scripts/02_juice/apply_puck_line_juice.py

import math
import sys
import traceback
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "01_merge" / "01_merguiced"
OUTPUT_DIR = BASE_DIR / "02_juice"
JUICE_FILE = BASE_DIR / "config" / "juice" / "nhl_puck_line_juice.csv"

ERROR_DIR = BASE_DIR / "errors" / "02_juice"
LOG_FILE = ERROR_DIR / "apply_puck_line_juice.txt"

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


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


REQUIRED_INPUT_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
    *FATIGUE_FEATURE_COLUMNS,
    *TEAM_STRENGTH_FEATURE_COLUMNS,
    *GOALIE_FEATURE_COLUMNS,
    *LINEUP_FEATURE_COLUMNS,
    "away_puck_line",
    "home_puck_line",
    "away_prob_puck_line",
    "home_prob_puck_line",
    "away_fair_decimal_puck_line",
    "home_fair_decimal_puck_line",
    "away_dk_puck_line_american",
    "home_dk_puck_line_american",
    "away_dk_puck_line_decimal",
    "home_dk_puck_line_decimal",
]

REQUIRED_CONFIG_COLUMNS = [
    "band",
    "band_min",
    "band_max",
    "venue",
    "fav_ud",
    "model_calibration_adjustment",
]

OUTPUT_COLUMNS = REQUIRED_INPUT_COLUMNS + [
    "away_juiced_decimal_puck_line",
    "home_juiced_decimal_puck_line",
    "away_juiced_prob_puck_line",
    "home_juiced_prob_puck_line",
    "away_normalized_prob_puck_line",
    "home_normalized_prob_puck_line",
]


def now() -> str:
    return datetime.now(UTC).isoformat()


def reset_log() -> None:
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== apply_puck_line_juice RUN {now()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{now()} | {msg}\n")


def wipe_outputs() -> int:
    removed = 0

    for path in OUTPUT_DIR.glob("*puck_line*.csv"):
        path.unlink()
        removed += 1

    for path in ERROR_DIR.glob("*puck_line*_quarantine.csv"):
        path.unlink()
        removed += 1

    log(f"Wiped puck_line output/quarantine CSVs: {removed}")
    return removed


def validate_columns(
    path: Path,
    df: pd.DataFrame,
    required_columns: list[str],
) -> None:
    missing = [
        col
        for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{path} missing required columns: {missing}"
        )


def load_config() -> pd.DataFrame:
    if not JUICE_FILE.exists():
        raise FileNotFoundError(
            f"Missing config file: {JUICE_FILE}"
        )

    juice_df = pd.read_csv(JUICE_FILE)

    validate_columns(
        JUICE_FILE,
        juice_df,
        REQUIRED_CONFIG_COLUMNS,
    )

    juice_df["band_min"] = pd.to_numeric(
        juice_df["band_min"],
        errors="coerce",
    )
    juice_df["band_max"] = pd.to_numeric(
        juice_df["band_max"],
        errors="coerce",
    )
    juice_df["model_calibration_adjustment"] = pd.to_numeric(
        juice_df["model_calibration_adjustment"],
        errors="coerce",
    )
    juice_df["venue"] = (
        juice_df["venue"]
        .astype(str)
        .str.strip()
    )
    juice_df["fav_ud"] = (
        juice_df["fav_ud"]
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
            f"{JUICE_FILE} has non-numeric "
            "band_min, band_max, or model_calibration_adjustment values"
        )

    return juice_df


def find_model_calibration_adjustment(
    juice_df: pd.DataFrame,
    puck_line: float,
    venue: str,
    fav_ud: str,
):
    band = juice_df[
        (juice_df["band_min"] <= puck_line)
        & (puck_line <= juice_df["band_max"])
        & (juice_df["venue"] == venue)
        & (juice_df["fav_ud"] == fav_ud)
    ]

    if len(band) != 1:
        return None

    return float(
        band.iloc[0]["model_calibration_adjustment"]
    )


def quarantine_row(
    original_df: pd.DataFrame,
    idx,
    reason: str,
    quarantine_rows: list[dict],
) -> None:
    rejected = original_df.loc[idx].to_dict()
    rejected["rejection_reason"] = reason
    quarantine_rows.append(rejected)


def write_quarantine(
    path: Path,
    original_columns: list[str],
    quarantine_rows: list[dict],
) -> Path | None:
    quarantine_path = (
        ERROR_DIR
        / f"{path.stem}_quarantine.csv"
    )

    if not quarantine_rows:
        if quarantine_path.exists():
            quarantine_path.unlink()
        return None

    quarantine_columns = (
        original_columns
        + ["rejection_reason"]
    )

    quarantine_df = pd.DataFrame(
        quarantine_rows
    )

    quarantine_df = quarantine_df.reindex(
        columns=quarantine_columns
    )

    quarantine_df.to_csv(
        quarantine_path,
        index=False,
    )

    return quarantine_path


def process_file(
    path: Path,
    juice_df: pd.DataFrame,
) -> tuple[int, int, int]:
    original_df = pd.read_csv(path)

    validate_columns(
        path,
        original_df,
        REQUIRED_INPUT_COLUMNS,
    )

    df = original_df.copy()

    for col in [
        *FATIGUE_FEATURE_COLUMNS,
        *TEAM_STRENGTH_FEATURE_COLUMNS,
        *GOALIE_NUMERIC_FEATURE_COLUMNS,
        *LINEUP_NUMERIC_FEATURE_COLUMNS,
        "away_puck_line",
        "home_puck_line",
        "away_prob_puck_line",
        "home_prob_puck_line",
        "away_fair_decimal_puck_line",
        "home_fair_decimal_puck_line",
        "away_dk_puck_line_american",
        "home_dk_puck_line_american",
        "away_dk_puck_line_decimal",
        "home_dk_puck_line_decimal",
    ]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

    for col in [
        "away_juiced_decimal_puck_line",
        "home_juiced_decimal_puck_line",
        "away_juiced_prob_puck_line",
        "home_juiced_prob_puck_line",
        "away_normalized_prob_puck_line",
        "home_normalized_prob_puck_line",
    ]:
        df[col] = pd.NA

    accepted_indices = []
    quarantine_rows = []

    applied = 0
    skipped_bad = 0
    skipped_noband = 0

    for idx, row in df.iterrows():
        try:
            away_line = float(
                row["away_puck_line"]
            )
            home_line = float(
                row["home_puck_line"]
            )
            away_fair = float(
                row[
                    "away_fair_decimal_puck_line"
                ]
            )
            home_fair = float(
                row[
                    "home_fair_decimal_puck_line"
                ]
            )
        except Exception:
            reason = "bad_numeric_parse"
            skipped_bad += 1
            quarantine_row(
                original_df,
                idx,
                reason,
                quarantine_rows,
            )
            log(
                f"ROW QUARANTINE: "
                f"{path.name} idx={idx} "
                f"reason={reason}"
            )
            continue

        if (
            not math.isfinite(away_line)
            or not math.isfinite(home_line)
            or not math.isfinite(away_fair)
            or not math.isfinite(home_fair)
            or away_fair <= 1
            or home_fair <= 1
        ):
            reason = "bad_puck_line_values"
            skipped_bad += 1
            quarantine_row(
                original_df,
                idx,
                reason,
                quarantine_rows,
            )
            log(
                f"ROW QUARANTINE: "
                f"{path.name} idx={idx} "
                f"reason={reason}"
            )
            continue

        away_fav_ud = (
            "favorite"
            if away_line < 0
            else "underdog"
        )
        home_fav_ud = (
            "favorite"
            if home_line < 0
            else "underdog"
        )

        away_adjustment = find_model_calibration_adjustment(
            juice_df,
            away_line,
            "away",
            away_fav_ud,
        )

        home_adjustment = find_model_calibration_adjustment(
            juice_df,
            home_line,
            "home",
            home_fav_ud,
        )

        if (
            away_adjustment is None
            or home_adjustment is None
        ):
            reason = "no_config_band"
            skipped_noband += 1
            quarantine_row(
                original_df,
                idx,
                reason,
                quarantine_rows,
            )
            log(
                f"ROW QUARANTINE: "
                f"{path.name} idx={idx} "
                f"reason={reason} "
                f"away_line={away_line} "
                f"home_line={home_line}"
            )
            continue

        away_juiced_decimal = (
            away_fair
            * (1 - away_adjustment)
        )

        home_juiced_decimal = (
            home_fair
            * (1 - home_adjustment)
        )

        if (
            not math.isfinite(
                away_juiced_decimal
            )
            or not math.isfinite(
                home_juiced_decimal
            )
            or away_juiced_decimal <= 1
            or home_juiced_decimal <= 1
        ):
            reason = "bad_juiced_decimal"
            skipped_bad += 1
            quarantine_row(
                original_df,
                idx,
                reason,
                quarantine_rows,
            )
            log(
                f"ROW QUARANTINE: "
                f"{path.name} idx={idx} "
                f"reason={reason} "
                f"away_juiced_decimal={away_juiced_decimal} "
                f"home_juiced_decimal={home_juiced_decimal}"
            )
            continue

        away_juiced_prob = (
            1 / away_juiced_decimal
        )
        home_juiced_prob = (
            1 / home_juiced_decimal
        )
        prob_total = (
            away_juiced_prob
            + home_juiced_prob
        )

        if (
            not math.isfinite(prob_total)
            or prob_total <= 0
        ):
            reason = "bad_probability_total"
            skipped_bad += 1
            quarantine_row(
                original_df,
                idx,
                reason,
                quarantine_rows,
            )
            log(
                f"ROW QUARANTINE: "
                f"{path.name} idx={idx} "
                f"reason={reason}"
            )
            continue

        df.at[
            idx,
            "away_juiced_decimal_puck_line",
        ] = away_juiced_decimal

        df.at[
            idx,
            "home_juiced_decimal_puck_line",
        ] = home_juiced_decimal

        df.at[
            idx,
            "away_juiced_prob_puck_line",
        ] = away_juiced_prob

        df.at[
            idx,
            "home_juiced_prob_puck_line",
        ] = home_juiced_prob

        df.at[
            idx,
            "away_normalized_prob_puck_line",
        ] = (
            away_juiced_prob
            / prob_total
        )

        df.at[
            idx,
            "home_normalized_prob_puck_line",
        ] = (
            home_juiced_prob
            / prob_total
        )

        accepted_indices.append(idx)
        applied += 1

    out_path = (
        OUTPUT_DIR
        / path.name
    )

    accepted_df = df.loc[
        accepted_indices,
        OUTPUT_COLUMNS,
    ].copy()

    accepted_df.to_csv(
        out_path,
        index=False,
    )

    quarantine_path = write_quarantine(
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


def main() -> None:
    reset_log()

    try:
        wipe_outputs()

        log(f"INPUT_DIR: {INPUT_DIR}")
        log(f"OUTPUT_DIR: {OUTPUT_DIR}")
        log(f"JUICE_FILE: {JUICE_FILE}")
        log(f"QUARANTINE_DIR: {ERROR_DIR}")

        juice_df = load_config()

        input_files = sorted(
            INPUT_DIR.glob(
                "*_NHL_puck_line.csv"
            )
        )

        log(
            f"Input files found: "
            f"{len(input_files)}"
        )

        if not input_files:
            raise FileNotFoundError(
                "No puck-line input files "
                f"found in {INPUT_DIR}"
            )

        files_written = 0
        total_applied = 0
        total_skipped_bad = 0
        total_skipped_noband = 0

        for path in input_files:
            log(
                f"Processing input: {path}"
            )

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
            total_skipped_bad += (
                skipped_bad
            )
            total_skipped_noband += (
                skipped_noband
            )

        total_quarantined = (
            total_skipped_bad
            + total_skipped_noband
        )

        log("--- SUMMARY ---")
        log(
            f"Files processed: "
            f"{len(input_files)}"
        )
        log(
            f"Files written: "
            f"{files_written}"
        )
        log(
            f"Rows applied: "
            f"{total_applied}"
        )
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

        print(
            "apply_puck_line_juice complete."
        )

    except Exception as e:
        log(
            f"FATAL ERROR: {e}\n"
            f"{traceback.format_exc()}"
        )
        log("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()