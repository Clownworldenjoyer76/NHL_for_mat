import re
import traceback
from pathlib import Path
from datetime import datetime

import pandas as pd


BASE_DIR = Path("docs/win/hockey/nhl")

INPUT_DIR = BASE_DIR / "00_intake" / "predictions" / "scraper"
OUTPUT_DIR = BASE_DIR / "00_intake" / "predictions"
SPORTSBOOK_DIR = BASE_DIR / "00_intake" / "sportsbook"

MAP_PATH = BASE_DIR / "config" / "mapping" / "team_map_nhl.csv"
NO_MAP_PATH = BASE_DIR / "config" / "mapping" / "no_map_nhl_pred.csv"

ERROR_DIR = BASE_DIR / "errors" / "00_intake"
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "transform_hockey.txt"

OUTPUT_COLUMNS = [
    "sport",
    "league",
    "game_id",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_prob_moneyline",
    "away_prob_moneyline",
    "away_projected_goals",
    "home_projected_goals",
    "total_projected_goals",
]


with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== transform_hockey RUN {datetime.now().isoformat()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {msg}\n")


def strip_record(name: str) -> str:
    return re.sub(r"\s*\(\d+[-–]\d+[-–]?\d*\)\s*$", "", str(name)).strip()


def hardcoded_normalize_team(name: str) -> str:
    name = str(name).strip().lower()

    replacements = {
        "st. louis": "st louis",
        "ny rangers": "new york rangers",
        "ny islanders": "new york islanders",
        "nj devils": "new jersey devils",
        "la kings": "los angeles kings",
    }

    for old_value, new_value in replacements.items():
        name = name.replace(old_value, new_value)

    return name


def detect_mapping_columns(df: pd.DataFrame) -> tuple[str, str]:
    columns = list(df.columns)
    lower_map = {col.lower().strip(): col for col in columns}

    source_candidates = [
        "source_team",
        "raw_team",
        "raw_name",
        "from",
        "alias",
        "team",
        "input",
        "input_team",
        "odds_team",
        "prediction_team",
        "dratings_team",
    ]

    target_candidates = [
        "normalized_team",
        "canonical_team",
        "standard_team",
        "mapped_team",
        "to",
        "team_normalized",
        "nhl_team",
        "output",
        "output_team",
    ]

    source_col = None
    target_col = None

    for candidate in source_candidates:
        if candidate in lower_map:
            source_col = lower_map[candidate]
            break

    for candidate in target_candidates:
        if candidate in lower_map:
            target_col = lower_map[candidate]
            break

    if source_col is None or target_col is None:
        if len(columns) >= 2:
            source_col = columns[0]
            target_col = columns[1]
        else:
            raise ValueError(
                f"Could not detect mapping columns in {MAP_PATH}. Columns found: {columns}"
            )

    return source_col, target_col


def load_team_map() -> dict:
    if not MAP_PATH.exists():
        raise FileNotFoundError(f"Missing team mapping file: {MAP_PATH}")

    df = pd.read_csv(MAP_PATH)

    if df.empty:
        raise ValueError(f"Team mapping file is empty: {MAP_PATH}")

    source_col, target_col = detect_mapping_columns(df)

    mapping = {}

    for _, row in df.iterrows():
        raw_value = str(row.get(source_col, "")).strip()
        mapped_value = str(row.get(target_col, "")).strip()

        if raw_value and mapped_value and raw_value.lower() != "nan" and mapped_value.lower() != "nan":
            raw_key = hardcoded_normalize_team(strip_record(raw_value))
            mapped_clean = hardcoded_normalize_team(strip_record(mapped_value))
            mapping[raw_key] = mapped_clean

    log(f"Loaded team map: {MAP_PATH}")
    log(f"Team map source column: {source_col}")
    log(f"Team map target column: {target_col}")
    log(f"Team map rows loaded: {len(mapping)}")

    return mapping


def normalize_team(name: str, team_map: dict, no_map_records: list, source_file: str) -> str:
    stripped = strip_record(name)
    base_norm = hardcoded_normalize_team(stripped)

    if base_norm in team_map:
        return team_map[base_norm]

    no_map_records.append(
        {
            "source_file": source_file,
            "raw_team": name,
            "stripped_team": stripped,
            "normalized_attempt": base_norm,
        }
    )

    return base_norm


def parse_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(str(date_str).strip(), "%m/%d/%Y %I:%M %p")
        return dt.strftime("%Y_%m_%d")
    except Exception:
        return str(date_str).strip().replace("/", "_").replace(" ", "_")


def parse_time(date_str: str) -> str:
    parts = str(date_str).strip().split(" ")
    if len(parts) >= 2:
        return " ".join(parts[1:])
    return ""


def parse_probability(value) -> str:
    try:
        parsed = float(str(value).replace("%", "").strip()) / 100
        return f"{parsed:.6f}"
    except Exception:
        return ""


def parse_float(value):
    try:
        return float(str(value).strip())
    except Exception:
        return ""


def load_sportsbook_for_date(date_val: str, team_map: dict, no_map_records: list) -> pd.DataFrame:
    sportsbook_path = SPORTSBOOK_DIR / f"NHL_{date_val}.csv"

    if not sportsbook_path.exists():
        log(f"WARNING: sportsbook missing for game_id match: {sportsbook_path}")
        return pd.DataFrame()

    sportsbook = pd.read_csv(sportsbook_path)

    required_columns = ["game_id", "game_date", "home_team", "away_team"]
    missing_columns = [col for col in required_columns if col not in sportsbook.columns]

    if missing_columns:
        log(
            f"WARNING: sportsbook file missing required columns for game_id match: "
            f"{sportsbook_path} | missing={missing_columns}"
        )
        return pd.DataFrame()

    sportsbook["home_team_norm"] = sportsbook["home_team"].apply(
        lambda value: normalize_team(value, team_map, no_map_records, str(sportsbook_path))
    )
    sportsbook["away_team_norm"] = sportsbook["away_team"].apply(
        lambda value: normalize_team(value, team_map, no_map_records, str(sportsbook_path))
    )
    sportsbook["game_date_norm"] = sportsbook["game_date"].astype(str).str.strip()

    log(f"Loaded sportsbook file for game_id match: {sportsbook_path} ({len(sportsbook)} rows)")

    return sportsbook


def get_game_id(
    sportsbook: pd.DataFrame,
    date_val: str,
    home_team: str,
    away_team: str,
) -> str:
    if sportsbook.empty:
        return ""

    matches = sportsbook[
        (sportsbook["game_date_norm"] == date_val)
        & (sportsbook["home_team_norm"] == home_team)
        & (sportsbook["away_team_norm"] == away_team)
    ]

    if matches.empty:
        log(f"NO GAME_ID MATCH: {away_team} @ {home_team} on {date_val}")
        return ""

    if len(matches) > 1:
        log(f"WARNING: MULTIPLE GAME_ID MATCHES: {away_team} @ {home_team} on {date_val}")
        return ""

    return str(matches.iloc[0]["game_id"]).strip()


def write_no_map_file(no_map_records: list) -> None:
    NO_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not no_map_records:
        pd.DataFrame(
            columns=[
                "source_file",
                "raw_team",
                "stripped_team",
                "normalized_attempt",
            ]
        ).to_csv(NO_MAP_PATH, index=False)
        log(f"WROTE no-map file with 0 rows: {NO_MAP_PATH}")
        return

    no_map_df = pd.DataFrame(no_map_records).drop_duplicates()
    no_map_df.to_csv(NO_MAP_PATH, index=False)
    log(f"WROTE no-map file: {NO_MAP_PATH} ({len(no_map_df)} rows)")


def transform_prediction_file(
    input_path: Path,
    team_map: dict,
    no_map_records: list,
    files_written: list,
) -> None:
    log(f"Processing prediction input: {input_path}")

    df = pd.read_csv(input_path)

    if df.empty:
        log(f"WARNING: input file empty: {input_path}")
        return

    required_columns = [
        "date_time",
        "team1",
        "team2",
        "team1_win_pct",
        "team2_win_pct",
        "proj_score_1",
        "proj_score_2",
        "score1",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        log(f"WARNING: skipping {input_path}; missing columns: {missing_columns}")
        return

    df["team1_clean"] = df["team1"].apply(
        lambda value: normalize_team(value, team_map, no_map_records, str(input_path))
    )
    df["team2_clean"] = df["team2"].apply(
        lambda value: normalize_team(value, team_map, no_map_records, str(input_path))
    )

    df["game_date"] = df["date_time"].apply(parse_date)
    df["game_time"] = df["date_time"].apply(parse_time)

    upcoming = df[
        df["score1"].isna()
        | (df["score1"].astype(str).str.strip() == "")
    ].copy()

    if upcoming.empty:
        log(f"No upcoming NHL games found in {input_path}.")
        return

    for date_val, group in upcoming.groupby("game_date"):
        sportsbook = load_sportsbook_for_date(date_val, team_map, no_map_records)
        output_rows = []

        for _, row in group.iterrows():
            away_team = row["team1_clean"]
            home_team = row["team2_clean"]

            away_projected_goals = parse_float(row["proj_score_1"])
            home_projected_goals = parse_float(row["proj_score_2"])

            if away_projected_goals != "" and home_projected_goals != "":
                total_projected_goals = round(away_projected_goals + home_projected_goals, 2)
            else:
                total_projected_goals = ""

            game_id = get_game_id(
                sportsbook=sportsbook,
                date_val=date_val,
                home_team=home_team,
                away_team=away_team,
            )

            output_rows.append(
                {
                    "sport": "hockey",
                    "league": "nhl",
                    "game_id": game_id,
                    "game_date": date_val,
                    "game_time": row["game_time"],
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_prob_moneyline": parse_probability(row["team2_win_pct"]),
                    "away_prob_moneyline": parse_probability(row["team1_win_pct"]),
                    "away_projected_goals": away_projected_goals,
                    "home_projected_goals": home_projected_goals,
                    "total_projected_goals": total_projected_goals,
                }
            )

        output = pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"hockey_{date_val}.csv"
        output.to_csv(output_path, index=False)

        files_written.append((str(output_path), len(output)))
        log(f"WROTE prediction output: {output_path} ({len(output)} rows)")


def main():
    files_written = []

    try:
        log(f"Input directory: {INPUT_DIR}")
        log(f"Output directory: {OUTPUT_DIR}")
        log(f"Sportsbook directory: {SPORTSBOOK_DIR}")
        log(f"Mapping file: {MAP_PATH}")
        log(f"No-map file: {NO_MAP_PATH}")

        team_map = load_team_map()
        no_map_records = []

        input_files = sorted(INPUT_DIR.glob("*_nhl_predictions.csv"))

        if not input_files:
            log(f"WARNING: no prediction input files found in {INPUT_DIR}")

        for input_path in input_files:
            transform_prediction_file(
                input_path=input_path,
                team_map=team_map,
                no_map_records=no_map_records,
                files_written=files_written,
            )

        write_no_map_file(no_map_records)

        log("--- SUMMARY ---")
        log(f"Input files processed: {len(input_files)}")
        log(f"Files written: {len(files_written)}")
        for path, count in files_written:
            log(f"  FILE: {path} ({count} rows)")
        log(f"No-map records: {len(no_map_records)}")
        log("STATUS: SUCCESS")

    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        raise

    print("\nDone.")


if __name__ == "__main__":
    main()
