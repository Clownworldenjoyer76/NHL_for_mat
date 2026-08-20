#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/02_juice/apply_total_juice.py

import math
import sys
import traceback
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd


BASE_DIR = Path("docs/win/hockey/nhl")

INPUT_DIR = BASE_DIR / "01_merge" / "01_merguiced"
OUTPUT_DIR = BASE_DIR / "02_juice"
JUICE_FILE = BASE_DIR / "config" / "juice" / "nhl_total_juice.csv"

ERROR_DIR = BASE_DIR / "errors" / "02_juice"
LOG_FILE = ERROR_DIR / "apply_total_juice.txt"

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
    "total",
    "total_projected_goals",
    "over_prob_total",
    "under_prob_total",
    "over_fair_decimal_total",
    "under_fair_decimal_total",
    "dk_total_over_american",
    "dk_total_under_american",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]

REQUIRED_CONFIG_COLUMNS = [
    "band",
    "band_min",
    "band_max",
    "side",
    "extra_juice",
]

OUTPUT_COLUMNS = REQUIRED_INPUT_COLUMNS + [
    "over_juiced_decimal_total",
    "under_juiced_decimal_total",
    "over_juiced_prob_total",
    "under_juiced_prob_total",
    "over_normalized_prob_total",
    "under_normalized_prob_total",
]


def now() -> str:
    return datetime.now(UTC).isoformat()


def reset_log() -> None:
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== apply_total_juice RUN {now()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{now()} | {msg}\n")


def wipe_outputs() -> int:
    removed = 0

    for path in OUTPUT_DIR.glob("*total*.csv"):
        path.unlink()
        removed += 1

    log(f"Wiped total output CSVs: {removed}")
    return removed


def validate_columns(path: Path, df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")


def load_config() -> pd.DataFrame:
    if not JUICE_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {JUICE_FILE}")

    juice_df = pd.read_csv(JUICE_FILE)
    validate_columns(JUICE_FILE, juice_df, REQUIRED_CONFIG_COLUMNS)

    juice_df["band_min"] = pd.to_numeric(juice_df["band_min"], errors="coerce")
    juice_df["band_max"] = pd.to_numeric(juice_df["band_max"], errors="coerce")
    juice_df["extra_juice"] = pd.to_numeric(juice_df["extra_juice"], errors="coerce")
    juice_df["side"] = juice_df["side"].astype(str).str.strip()

    if juice_df[["band_min", "band_max", "extra_juice"]].isna().any().any():
        raise ValueError(f"{JUICE_FILE} has non-numeric band_min, band_max, or extra_juice values")

    return juice_df


def find_extra_juice(juice_df: pd.DataFrame, total_line: float, side: str):
    band = juice_df[
        (juice_df["band_min"] <= total_line)
        & (total_line <= juice_df["band_max"])
        & (juice_df["side"] == side)
    ]

    if len(band) != 1:
        return None

    return float(band.iloc[0]["extra_juice"])


def process_file(path: Path, juice_df: pd.DataFrame) -> tuple[int, int, int]:
    df = pd.read_csv(path)
    validate_columns(path, df, REQUIRED_INPUT_COLUMNS)

    for col in [
        "total",
        "total_projected_goals",
        "over_prob_total",
        "under_prob_total",
        "over_fair_decimal_total",
        "under_fair_decimal_total",
        "dk_total_over_american",
        "dk_total_under_american",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in [
        "over_juiced_decimal_total",
        "under_juiced_decimal_total",
        "over_juiced_prob_total",
        "under_juiced_prob_total",
        "over_normalized_prob_total",
        "under_normalized_prob_total",
    ]:
        df[col] = pd.NA

    applied = 0
    skipped_bad = 0
    skipped_noband = 0

    for idx, row in df.iterrows():
        try:
            total_line = float(row["total"])
            over_fair = float(row["over_fair_decimal_total"])
            under_fair = float(row["under_fair_decimal_total"])
        except Exception:
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_numeric_parse")
            continue

        if (
            not math.isfinite(total_line)
            or not math.isfinite(over_fair)
            or not math.isfinite(under_fair)
            or over_fair <= 1
            or under_fair <= 1
        ):
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_total_values")
            continue

        over_extra = find_extra_juice(juice_df, total_line, "over")
        under_extra = find_extra_juice(juice_df, total_line, "under")

        if over_extra is None or under_extra is None:
            skipped_noband += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=no_config_band total={total_line}")
            continue

        over_juiced_decimal = over_fair * (1 - over_extra)
        under_juiced_decimal = under_fair * (1 - under_extra)

        if (
            not math.isfinite(over_juiced_decimal)
            or not math.isfinite(under_juiced_decimal)
            or over_juiced_decimal <= 1
            or under_juiced_decimal <= 1
        ):
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_juiced_decimal")
            continue

        over_juiced_prob = 1 / over_juiced_decimal
        under_juiced_prob = 1 / under_juiced_decimal
        prob_total = over_juiced_prob + under_juiced_prob

        if not math.isfinite(prob_total) or prob_total <= 0:
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_probability_total")
            continue

        df.at[idx, "over_juiced_decimal_total"] = over_juiced_decimal
        df.at[idx, "under_juiced_decimal_total"] = under_juiced_decimal
        df.at[idx, "over_juiced_prob_total"] = over_juiced_prob
        df.at[idx, "under_juiced_prob_total"] = under_juiced_prob
        df.at[idx, "over_normalized_prob_total"] = over_juiced_prob / prob_total
        df.at[idx, "under_normalized_prob_total"] = under_juiced_prob / prob_total

        applied += 1

    out_path = OUTPUT_DIR / path.name
    df = df[OUTPUT_COLUMNS]
    df.to_csv(out_path, index=False)

    log(
        f"WROTE {out_path} rows={len(df)} applied={applied} "
        f"skipped_bad={skipped_bad} skipped_noband={skipped_noband}"
    )

    return applied, skipped_bad, skipped_noband


def main() -> None:
    reset_log()

    try:
        wipe_outputs()

        log(f"INPUT_DIR: {INPUT_DIR}")
        log(f"OUTPUT_DIR: {OUTPUT_DIR}")
        log(f"JUICE_FILE: {JUICE_FILE}")

        juice_df = load_config()
        input_files = sorted(INPUT_DIR.glob("*_NHL_total.csv"))

        log(f"Input files found: {len(input_files)}")

        if not input_files:
            raise FileNotFoundError(f"No total input files found in {INPUT_DIR}")

        files_written = 0
        total_applied = 0
        total_skipped_bad = 0
        total_skipped_noband = 0

        for path in input_files:
            log(f"Processing input: {path}")
            applied, skipped_bad, skipped_noband = process_file(path, juice_df)

            files_written += 1
            total_applied += applied
            total_skipped_bad += skipped_bad
            total_skipped_noband += skipped_noband

        log("--- SUMMARY ---")
        log(f"Files processed: {len(input_files)}")
        log(f"Files written: {files_written}")
        log(f"Rows applied: {total_applied}")
        log(f"Rows skipped bad: {total_skipped_bad}")
        log(f"Rows skipped no band: {total_skipped_noband}")
        log("STATUS: SUCCESS")

        print("apply_total_juice complete.")

    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
