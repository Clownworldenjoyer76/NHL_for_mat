#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/02_juice/apply_moneyline_juice.py

import math
import sys
import traceback
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd


BASE_DIR = Path("docs/win/hockey/nhl")

INPUT_DIR = BASE_DIR / "01_merge" / "01_merguiced"
OUTPUT_DIR = BASE_DIR / "02_juice"
JUICE_FILE = BASE_DIR / "config" / "juice" / "nhl_moneyline_juice.csv"

ERROR_DIR = BASE_DIR / "errors" / "02_juice"
LOG_FILE = ERROR_DIR / "apply_moneyline_juice.txt"

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
    "away_prob_moneyline",
    "home_prob_moneyline",
    "away_fair_decimal_moneyline",
    "home_fair_decimal_moneyline",
    "away_dk_moneyline_american",
    "home_dk_moneyline_american",
    "away_dk_moneyline_decimal",
    "home_dk_moneyline_decimal",
]

REQUIRED_CONFIG_COLUMNS = [
    "band",
    "band_min",
    "band_max",
    "fav_ud",
    "venue",
    "extra_juice",
]

OUTPUT_COLUMNS = REQUIRED_INPUT_COLUMNS + [
    "away_juiced_decimal_moneyline",
    "home_juiced_decimal_moneyline",
    "away_juiced_prob_moneyline",
    "home_juiced_prob_moneyline",
    "away_normalized_prob_moneyline",
    "home_normalized_prob_moneyline",
]


def now() -> str:
    return datetime.now(UTC).isoformat()


def reset_log() -> None:
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== apply_moneyline_juice RUN {now()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{now()} | {msg}\n")


def wipe_outputs() -> int:
    removed = 0

    for path in OUTPUT_DIR.glob("*moneyline*.csv"):
        path.unlink()
        removed += 1

    log(f"Wiped moneyline output CSVs: {removed}")
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
    juice_df["fav_ud"] = juice_df["fav_ud"].astype(str).str.strip()
    juice_df["venue"] = juice_df["venue"].astype(str).str.strip()

    if juice_df[["band_min", "band_max", "extra_juice"]].isna().any().any():
        raise ValueError(f"{JUICE_FILE} has non-numeric band_min, band_max, or extra_juice values")

    return juice_df


def find_extra_juice(juice_df: pd.DataFrame, american: float, fav_ud: str, venue: str):
    band = juice_df[
        (juice_df["band_min"] <= american)
        & (american <= juice_df["band_max"])
        & (juice_df["fav_ud"] == fav_ud)
        & (juice_df["venue"] == venue)
    ]

    if len(band) != 1:
        return None

    return float(band.iloc[0]["extra_juice"])


def process_file(path: Path, juice_df: pd.DataFrame) -> tuple[int, int, int]:
    df = pd.read_csv(path)
    validate_columns(path, df, REQUIRED_INPUT_COLUMNS)

    for col in [
        "away_prob_moneyline",
        "home_prob_moneyline",
        "away_fair_decimal_moneyline",
        "home_fair_decimal_moneyline",
        "away_dk_moneyline_american",
        "home_dk_moneyline_american",
        "away_dk_moneyline_decimal",
        "home_dk_moneyline_decimal",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in [
        "away_juiced_decimal_moneyline",
        "home_juiced_decimal_moneyline",
        "away_juiced_prob_moneyline",
        "home_juiced_prob_moneyline",
        "away_normalized_prob_moneyline",
        "home_normalized_prob_moneyline",
    ]:
        df[col] = pd.NA

    applied = 0
    skipped_bad = 0
    skipped_noband = 0

    for idx, row in df.iterrows():
        try:
            away_american = float(row["away_dk_moneyline_american"])
            home_american = float(row["home_dk_moneyline_american"])
            away_fair = float(row["away_fair_decimal_moneyline"])
            home_fair = float(row["home_fair_decimal_moneyline"])
        except Exception:
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_numeric_parse")
            continue

        if (
            not math.isfinite(away_american)
            or not math.isfinite(home_american)
            or not math.isfinite(away_fair)
            or not math.isfinite(home_fair)
            or away_fair <= 1
            or home_fair <= 1
        ):
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_moneyline_values")
            continue

        away_fav_ud = "favorite" if away_american < 0 else "underdog"
        home_fav_ud = "favorite" if home_american < 0 else "underdog"

        away_extra = find_extra_juice(juice_df, away_american, away_fav_ud, "away")
        home_extra = find_extra_juice(juice_df, home_american, home_fav_ud, "home")

        if away_extra is None or home_extra is None:
            skipped_noband += 1
            log(
                f"ROW SKIP: {path.name} idx={idx} reason=no_config_band "
                f"away_american={away_american} home_american={home_american}"
            )
            continue

        away_juiced_decimal = away_fair * (1 - away_extra)
        home_juiced_decimal = home_fair * (1 - home_extra)

        if (
            not math.isfinite(away_juiced_decimal)
            or not math.isfinite(home_juiced_decimal)
            or away_juiced_decimal <= 1
            or home_juiced_decimal <= 1
        ):
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_juiced_decimal")
            continue

        away_juiced_prob = 1 / away_juiced_decimal
        home_juiced_prob = 1 / home_juiced_decimal
        prob_total = away_juiced_prob + home_juiced_prob

        if not math.isfinite(prob_total) or prob_total <= 0:
            skipped_bad += 1
            log(f"ROW SKIP: {path.name} idx={idx} reason=bad_probability_total")
            continue

        df.at[idx, "away_juiced_decimal_moneyline"] = away_juiced_decimal
        df.at[idx, "home_juiced_decimal_moneyline"] = home_juiced_decimal
        df.at[idx, "away_juiced_prob_moneyline"] = away_juiced_prob
        df.at[idx, "home_juiced_prob_moneyline"] = home_juiced_prob
        df.at[idx, "away_normalized_prob_moneyline"] = away_juiced_prob / prob_total
        df.at[idx, "home_normalized_prob_moneyline"] = home_juiced_prob / prob_total

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
        input_files = sorted(INPUT_DIR.glob("*_NHL_moneyline.csv"))

        log(f"Input files found: {len(input_files)}")

        if not input_files:
            raise FileNotFoundError(f"No moneyline input files found in {INPUT_DIR}")

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

        print("apply_moneyline_juice complete.")

    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
