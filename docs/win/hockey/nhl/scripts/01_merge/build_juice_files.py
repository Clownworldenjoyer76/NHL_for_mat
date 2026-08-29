#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/01_merge/build_juice_files.py

import math
import sys
import traceback
from pathlib import Path
from datetime import datetime, UTC

import pandas as pd
from scipy.stats import poisson, skellam


BASE_DIR = Path("docs/win/hockey/nhl")

INPUT_DIR = BASE_DIR / "01_merge"
OUTPUT_DIR = INPUT_DIR / "01_merguiced"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = BASE_DIR / "errors" / "01_merge"
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "build_juice_files.txt"


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


MERGED_REQUIRED_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
    *FATIGUE_FEATURE_COLUMNS,
    "away_prob_moneyline",
    "home_prob_moneyline",
    "away_projected_goals",
    "home_projected_goals",
    "total_projected_goals",
    "away_puck_line",
    "home_puck_line",
    "total",
    "away_dk_moneyline_american",
    "home_dk_moneyline_american",
    "away_dk_moneyline_decimal",
    "home_dk_moneyline_decimal",
    "away_dk_puck_line_american",
    "home_dk_puck_line_american",
    "away_dk_puck_line_decimal",
    "home_dk_puck_line_decimal",
    "dk_total_over_american",
    "dk_total_under_american",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]


MONEYLINE_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
    *FATIGUE_FEATURE_COLUMNS,
    "away_prob_moneyline",
    "home_prob_moneyline",
    "away_fair_decimal_moneyline",
    "home_fair_decimal_moneyline",
    "away_dk_moneyline_american",
    "home_dk_moneyline_american",
    "away_dk_moneyline_decimal",
    "home_dk_moneyline_decimal",
]


PUCK_LINE_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
    *FATIGUE_FEATURE_COLUMNS,
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


TOTAL_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
    *FATIGUE_FEATURE_COLUMNS,
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


with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== build_juice_files RUN {datetime.now(UTC).isoformat()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(UTC).isoformat()} | {msg}\n")


def wipe_output_dir() -> None:
    removed = 0

    for path in OUTPUT_DIR.glob("*.csv"):
        path.unlink()
        removed += 1

    log(f"Wiped pre-juice CSV outputs: {removed}")


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def fair_decimal(prob):
    if pd.isna(prob) or prob <= 0:
        return None

    return 1 / prob


def calculate_home_puck_probability(home_line, home_projected_goals, away_projected_goals):
    if (
        pd.isna(home_line)
        or pd.isna(home_projected_goals)
        or pd.isna(away_projected_goals)
        or home_projected_goals <= 0
        or away_projected_goals <= 0
    ):
        return None

    threshold = math.floor(-home_line)
    probability = 1 - skellam.cdf(threshold, home_projected_goals, away_projected_goals)

    if pd.isna(probability):
        return None

    return min(max(probability, 0.01), 0.99)


def calculate_away_puck_probability(away_line, away_projected_goals, home_projected_goals):
    if (
        pd.isna(away_line)
        or pd.isna(away_projected_goals)
        or pd.isna(home_projected_goals)
        or away_projected_goals <= 0
        or home_projected_goals <= 0
    ):
        return None

    threshold = math.floor(-away_line)
    probability = 1 - skellam.cdf(threshold, away_projected_goals, home_projected_goals)

    if pd.isna(probability):
        return None

    return min(max(probability, 0.01), 0.99)


def calculate_total_probabilities(total_line, total_projected_goals):
    if (
        pd.isna(total_line)
        or pd.isna(total_projected_goals)
        or total_projected_goals <= 0
    ):
        return None, None

    total_line = float(total_line)

    if total_line.is_integer():
        push_total = int(total_line)

        under_prob = poisson.cdf(push_total - 1, total_projected_goals)
        over_prob = 1 - poisson.cdf(push_total, total_projected_goals)

        no_push_prob = under_prob + over_prob

        if pd.isna(no_push_prob) or no_push_prob <= 0:
            return None, None

        under_prob = under_prob / no_push_prob
        over_prob = over_prob / no_push_prob
    else:
        cutoff = math.floor(total_line)

        under_prob = poisson.cdf(cutoff, total_projected_goals)
        over_prob = 1 - under_prob

    if pd.isna(over_prob) or pd.isna(under_prob):
        return None, None

    over_prob = min(max(over_prob, 0.01), 0.99)
    under_prob = min(max(under_prob, 0.01), 0.99)

    return over_prob, under_prob


def validate_schema(path: Path, df: pd.DataFrame) -> list[str]:
    return [col for col in MERGED_REQUIRED_COLUMNS if col not in df.columns]


def build_moneyline(df: pd.DataFrame, output_path: Path) -> int:
    moneyline = df.copy()

    moneyline["away_fair_decimal_moneyline"] = moneyline["away_prob_moneyline"].apply(fair_decimal)
    moneyline["home_fair_decimal_moneyline"] = moneyline["home_prob_moneyline"].apply(fair_decimal)

    moneyline = moneyline[MONEYLINE_COLUMNS]
    moneyline.to_csv(output_path, index=False)

    log(f"WROTE {output_path} ({len(moneyline)} rows)")
    return len(moneyline)


def build_puck_line(df: pd.DataFrame, output_path: Path) -> int:
    puck_line = df.copy()

    away_probs = []
    home_probs = []
    away_fair_decimals = []
    home_fair_decimals = []

    for idx, row in puck_line.iterrows():
        home_prob = calculate_home_puck_probability(
            row["home_puck_line"],
            row["home_projected_goals"],
            row["away_projected_goals"],
        )
        away_prob = calculate_away_puck_probability(
            row["away_puck_line"],
            row["away_projected_goals"],
            row["home_projected_goals"],
        )

        if home_prob is None or away_prob is None:
            log(
                f"ROW ISSUE: puck-line probability unavailable "
                f"idx={idx} game_id={row.get('game_id', '')} "
                f"home_line={row.get('home_puck_line', '')} "
                f"away_line={row.get('away_puck_line', '')}"
            )

        home_probs.append(home_prob)
        away_probs.append(away_prob)
        home_fair_decimals.append(fair_decimal(home_prob) if home_prob is not None else None)
        away_fair_decimals.append(fair_decimal(away_prob) if away_prob is not None else None)

    puck_line["away_prob_puck_line"] = away_probs
    puck_line["home_prob_puck_line"] = home_probs
    puck_line["away_fair_decimal_puck_line"] = away_fair_decimals
    puck_line["home_fair_decimal_puck_line"] = home_fair_decimals

    puck_line = puck_line[PUCK_LINE_COLUMNS]
    puck_line.to_csv(output_path, index=False)

    log(f"WROTE {output_path} ({len(puck_line)} rows)")
    return len(puck_line)


def build_total(df: pd.DataFrame, output_path: Path) -> int:
    total = df.copy()

    over_probs = []
    under_probs = []
    over_fair_decimals = []
    under_fair_decimals = []

    for idx, row in total.iterrows():
        over_prob, under_prob = calculate_total_probabilities(
            row["total"],
            row["total_projected_goals"],
        )

        if over_prob is None or under_prob is None:
            log(
                f"ROW ISSUE: total probability unavailable "
                f"idx={idx} game_id={row.get('game_id', '')} "
                f"total={row.get('total', '')} "
                f"total_projected_goals={row.get('total_projected_goals', '')}"
            )

        over_probs.append(over_prob)
        under_probs.append(under_prob)
        over_fair_decimals.append(fair_decimal(over_prob) if over_prob is not None else None)
        under_fair_decimals.append(fair_decimal(under_prob) if under_prob is not None else None)

    total["over_prob_total"] = over_probs
    total["under_prob_total"] = under_probs
    total["over_fair_decimal_total"] = over_fair_decimals
    total["under_fair_decimal_total"] = under_fair_decimals

    total = total[TOTAL_COLUMNS]
    total.to_csv(output_path, index=False)

    log(f"WROTE {output_path} ({len(total)} rows)")
    return len(total)


def process_file(path: Path) -> list[tuple[str, int]]:
    files_written = []

    df = pd.read_csv(path)

    if df.empty:
        log(f"EMPTY: {path} — skipping")
        return files_written

    missing_columns = validate_schema(path, df)

    if missing_columns:
        log(f"SCHEMA ERROR: {path} missing columns: {missing_columns}")
        raise ValueError(f"{path} missing required columns: {missing_columns}")

    numeric_columns = [
        *FATIGUE_FEATURE_COLUMNS,
        "away_prob_moneyline",
        "home_prob_moneyline",
        "away_projected_goals",
        "home_projected_goals",
        "total_projected_goals",
        "away_puck_line",
        "home_puck_line",
        "total",
        "away_dk_moneyline_american",
        "home_dk_moneyline_american",
        "away_dk_moneyline_decimal",
        "home_dk_moneyline_decimal",
        "away_dk_puck_line_american",
        "home_dk_puck_line_american",
        "away_dk_puck_line_decimal",
        "home_dk_puck_line_decimal",
        "dk_total_over_american",
        "dk_total_under_american",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
    ]

    for col in numeric_columns:
        df[col] = to_numeric(df[col])

    slate_date = path.name.replace("_NHL_merged.csv", "")

    moneyline_path = OUTPUT_DIR / f"{slate_date}_NHL_moneyline.csv"
    puck_line_path = OUTPUT_DIR / f"{slate_date}_NHL_puck_line.csv"
    total_path = OUTPUT_DIR / f"{slate_date}_NHL_total.csv"

    moneyline_count = build_moneyline(df, moneyline_path)
    files_written.append((str(moneyline_path), moneyline_count))

    puck_line_count = build_puck_line(df, puck_line_path)
    files_written.append((str(puck_line_path), puck_line_count))

    total_count = build_total(df, total_path)
    files_written.append((str(total_path), total_count))

    return files_written


def main() -> None:
    files_written = []
    files_processed = 0

    try:
        wipe_output_dir()

        input_files = sorted(INPUT_DIR.glob("*_NHL_merged.csv"))

        log(f"Input files found: {len(input_files)}")

        if not input_files:
            raise FileNotFoundError(f"No merged input files found in {INPUT_DIR}")

        for path in input_files:
            log(f"Processing merged input: {path}")
            written = process_file(path)
            files_written.extend(written)
            files_processed += 1

        log("--- SUMMARY ---")
        log(f"Input files processed: {files_processed}")
        log(f"Files written: {len(files_written)}")
        for path, count in files_written:
            log(f"  FILE: {path} ({count} rows)")
        log("STATUS: SUCCESS")

    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
