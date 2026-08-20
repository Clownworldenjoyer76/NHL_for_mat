#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/01_merge/merge_intake.py

import csv
import traceback
from pathlib import Path
from datetime import datetime, UTC


BASE_DIR = Path("docs/win/hockey/nhl")

GAMES_DIR = BASE_DIR / "00_intake" / "games"
SPORTSBOOK_DIR = BASE_DIR / "00_intake" / "sportsbook"
PREDICTIONS_DIR = BASE_DIR / "00_intake" / "predictions"

MERGE_DIR = BASE_DIR / "01_merge"
AUDIT_DIR = MERGE_DIR / "audit"

ERROR_DIR = BASE_DIR / "errors" / "01_merge"
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "merge_intake.txt"

MERGE_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)


MERGED_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
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

AUDIT_COLUMNS = [
    "game_date",
    "game_id",
    "away_team",
    "home_team",
    "source_present_games",
    "source_present_sportsbook",
    "source_present_predictions",
    "status",
]

REJECTION_COLUMNS = [
    "reason",
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "away_team",
    "home_team",
]

REQUIRED_GAMES_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
]

REQUIRED_SPORTSBOOK_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
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

REQUIRED_PREDICTION_COLUMNS = [
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
    f.write(f"=== merge_intake RUN {datetime.now(UTC).isoformat()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(UTC).isoformat()} | {msg}\n")


def fail(message: str) -> None:
    log(f"FATAL: {message}")
    log("STATUS: FAILED")
    raise SystemExit(message)


def wipe_merge_outputs() -> None:
    removed = 0

    for path in MERGE_DIR.glob("*.csv"):
        path.unlink()
        removed += 1

    log(f"Wiped merge CSV outputs: {removed}")


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    return fieldnames, rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    log(f"WROTE {path} ({len(rows)} rows)")


def validate_required_columns(path: Path, fieldnames: list[str], required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in fieldnames]

    if missing:
        fail(f"{path} missing required columns: {missing}")


def row_date(row: dict[str, str]) -> str:
    return str(row.get("game_date", "")).strip()


def row_game_id(row: dict[str, str]) -> str:
    return str(row.get("game_id", "")).strip()


def parse_game_date(value: str):
    s = str(value).strip().replace("-", "_")

    try:
        return datetime.strptime(s, "%Y_%m_%d").date()
    except Exception:
        return None


def is_future_game_date(value: str) -> bool:
    parsed = parse_game_date(value)

    if parsed is None:
        return False

    return parsed > datetime.now(UTC).date()


def validate_and_filter_game_ids(
    path: Path,
    rows: list[dict[str, str]],
    source_name: str,
) -> list[dict[str, str]]:
    seen = {}
    missing_rows = []
    duplicate_ids = set()
    filtered_rows = []
    skipped_future_missing_game_id = 0

    for idx, row in enumerate(rows, start=2):
        game_id = row_game_id(row)

        if not game_id:
            game_date = row_date(row)

            if is_future_game_date(game_date):
                skipped_future_missing_game_id += 1
                log(
                    f"SKIPPED FUTURE ROW WITH MISSING game_id | "
                    f"source={source_name} | file={path} | row={idx} | game_date={game_date} | "
                    f"away_team={row.get('away_team', '')} | home_team={row.get('home_team', '')}"
                )
                continue

            missing_rows.append(idx)
            continue

        if game_id in seen:
            duplicate_ids.add(game_id)
        else:
            seen[game_id] = idx

        filtered_rows.append(row)

    if missing_rows:
        fail(f"{source_name} file has missing game_id values: {path} rows={missing_rows}")

    if duplicate_ids:
        fail(f"{source_name} file has duplicate game_id values: {path} duplicate_game_ids={sorted(duplicate_ids)}")

    if skipped_future_missing_game_id:
        log(
            f"{source_name} file skipped future rows with missing game_id: "
            f"{path} count={skipped_future_missing_game_id}"
        )

    return filtered_rows


def load_source_rows(
    source_name: str,
    directory: Path,
    pattern: str,
    required_columns: list[str],
) -> list[dict[str, str]]:
    all_rows = []

    if source_name == "sportsbook":
        files = sorted(
            p
            for p in directory.glob("*.csv")
            if p.name.lower().startswith("nhl_")
        )
    else:
        files = sorted(directory.glob(pattern))

    log(f"{source_name} files found: {len(files)}")

    if not files:
        fail(f"No {source_name} files found in {directory} matching {pattern}")

    for path in files:
        fieldnames, rows = load_csv(path)
        validate_required_columns(path, fieldnames, required_columns)

        rows = validate_and_filter_game_ids(path, rows, source_name)

        for row in rows:
            row["_source_file"] = str(path)
            all_rows.append(row)

        log(f"Loaded {source_name} file: {path} ({len(rows)} usable rows)")

    return all_rows


def rows_by_date_game_id(rows: list[dict[str, str]], source_name: str) -> dict[str, dict[str, dict[str, str]]]:
    grouped = {}

    seen = {}
    duplicate_keys = set()
    missing_date_rows = []

    for idx, row in enumerate(rows, start=1):
        game_date = row_date(row)
        game_id = row_game_id(row)

        if not game_date:
            missing_date_rows.append((row.get("_source_file", ""), idx, game_id))
            continue

        if not game_id:
            fail(
                f"{source_name} row reached grouping with blank game_id: "
                f"source_file={row.get('_source_file', '')} row={idx} game_date={game_date}"
            )

        key = (game_date, game_id)

        if key in seen:
            duplicate_keys.add(key)
        else:
            seen[key] = row.get("_source_file", "")

        grouped.setdefault(game_date, {})[game_id] = row

    if missing_date_rows:
        fail(f"{source_name} rows have missing game_date values: {missing_date_rows}")

    if duplicate_keys:
        fail(f"{source_name} has duplicate game_date/game_id keys: {sorted(duplicate_keys)}")

    return grouped


def rejection_from_row(reason: str, row: dict[str, str]) -> dict[str, str]:
    return {
        "reason": reason,
        "game_id": str(row.get("game_id", "")).strip(),
        "sport": row.get("sport", ""),
        "league": row.get("league", ""),
        "game_date": row.get("game_date", ""),
        "game_time": row.get("game_time", ""),
        "away_team": row.get("away_team", ""),
        "home_team": row.get("home_team", ""),
    }


def process_date(
    date_val: str,
    games_map: dict[str, dict[str, str]],
    sportsbook_map: dict[str, dict[str, str]],
    predictions_map: dict[str, dict[str, str]],
) -> tuple[int, int, int, bool]:
    merged_path = MERGE_DIR / f"{date_val}_NHL_merged.csv"
    audit_path = AUDIT_DIR / f"{date_val}_NHL_merge_audit.csv"
    rejected_sportsbook_path = AUDIT_DIR / f"{date_val}_NHL_rejected_sportsbook.csv"
    rejected_predictions_path = AUDIT_DIR / f"{date_val}_NHL_rejected_predictions.csv"

    log(f"Processing game_date: {date_val}")
    log(f"Games rows for date: {len(games_map)}")
    log(f"Sportsbook rows for date: {len(sportsbook_map)}")
    log(f"Prediction rows for date: {len(predictions_map)}")

    date_has_failure = False

    audit_rows = []
    rejected_sportsbook = []
    rejected_predictions = []
    merged_rows = []

    for game_id, row in sportsbook_map.items():
        if game_id not in games_map:
            date_has_failure = True
            rejected_sportsbook.append(rejection_from_row("sportsbook_row_not_found_in_games", row))

    for game_id, row in predictions_map.items():
        if game_id not in games_map:
            date_has_failure = True
            rejected_predictions.append(rejection_from_row("prediction_row_not_found_in_games", row))

    for game_id, game in games_map.items():
        has_sportsbook = game_id in sportsbook_map
        has_prediction = game_id in predictions_map

        if has_sportsbook and has_prediction:
            status = "matched"
        elif not has_sportsbook and not has_prediction:
            status = "missing_sportsbook_and_prediction"
            date_has_failure = True
        elif not has_sportsbook:
            status = "missing_sportsbook"
            date_has_failure = True
        else:
            status = "missing_prediction"
            date_has_failure = True

        audit_rows.append(
            {
                "game_date": game.get("game_date", date_val),
                "game_id": game_id,
                "away_team": game.get("away_team", ""),
                "home_team": game.get("home_team", ""),
                "source_present_games": "1",
                "source_present_sportsbook": "1" if has_sportsbook else "0",
                "source_present_predictions": "1" if has_prediction else "0",
                "status": status,
            }
        )

        if status != "matched":
            continue

        sportsbook = sportsbook_map[game_id]
        prediction = predictions_map[game_id]

        merged_rows.append(
            {
                "sport": game.get("sport", "hockey"),
                "league": game.get("league", "nhl"),
                "game_date": game.get("game_date", date_val),
                "game_time": game.get("game_time", ""),
                "game_id": game_id,
                "away_team": game.get("away_team", ""),
                "home_team": game.get("home_team", ""),
                "away_prob_moneyline": prediction.get("away_prob_moneyline", ""),
                "home_prob_moneyline": prediction.get("home_prob_moneyline", ""),
                "away_projected_goals": prediction.get("away_projected_goals", ""),
                "home_projected_goals": prediction.get("home_projected_goals", ""),
                "total_projected_goals": prediction.get("total_projected_goals", ""),
                "away_puck_line": sportsbook.get("away_puck_line", ""),
                "home_puck_line": sportsbook.get("home_puck_line", ""),
                "total": sportsbook.get("total", ""),
                "away_dk_moneyline_american": sportsbook.get("away_dk_moneyline_american", ""),
                "home_dk_moneyline_american": sportsbook.get("home_dk_moneyline_american", ""),
                "away_dk_moneyline_decimal": sportsbook.get("away_dk_moneyline_decimal", ""),
                "home_dk_moneyline_decimal": sportsbook.get("home_dk_moneyline_decimal", ""),
                "away_dk_puck_line_american": sportsbook.get("away_dk_puck_line_american", ""),
                "home_dk_puck_line_american": sportsbook.get("home_dk_puck_line_american", ""),
                "away_dk_puck_line_decimal": sportsbook.get("away_dk_puck_line_decimal", ""),
                "home_dk_puck_line_decimal": sportsbook.get("home_dk_puck_line_decimal", ""),
                "dk_total_over_american": sportsbook.get("dk_total_over_american", ""),
                "dk_total_under_american": sportsbook.get("dk_total_under_american", ""),
                "dk_total_over_decimal": sportsbook.get("dk_total_over_decimal", ""),
                "dk_total_under_decimal": sportsbook.get("dk_total_under_decimal", ""),
            }
        )

    write_csv(audit_path, AUDIT_COLUMNS, audit_rows)
    write_csv(rejected_sportsbook_path, REJECTION_COLUMNS, rejected_sportsbook)
    write_csv(rejected_predictions_path, REJECTION_COLUMNS, rejected_predictions)

    if merged_rows:
        write_csv(merged_path, MERGED_COLUMNS, merged_rows)
    else:
        log(f"No merged rows written for {date_val}")

    log(
        f"Date summary {date_val}: "
        f"games={len(games_map)} sportsbook={len(sportsbook_map)} predictions={len(predictions_map)} "
        f"merged={len(merged_rows)} rejected_sportsbook={len(rejected_sportsbook)} "
        f"rejected_predictions={len(rejected_predictions)} audit_failures="
        f"{len([r for r in audit_rows if r['status'] != 'matched'])}"
    )

    return len(merged_rows), len(rejected_sportsbook), len(rejected_predictions), date_has_failure


def main() -> None:
    total_merged = 0
    total_rejected_sportsbook = 0
    total_rejected_predictions = 0
    dates_failed = 0

    try:
        wipe_merge_outputs()

        games_rows = load_source_rows("games", GAMES_DIR, "*_nhl_games.csv", REQUIRED_GAMES_COLUMNS)
        sportsbook_rows = load_source_rows("sportsbook", SPORTSBOOK_DIR, "NHL_*.csv", REQUIRED_SPORTSBOOK_COLUMNS)
        prediction_rows = load_source_rows("predictions", PREDICTIONS_DIR, "hockey_*.csv", REQUIRED_PREDICTION_COLUMNS)

        games_by_date = rows_by_date_game_id(games_rows, "games")
        sportsbook_by_date = rows_by_date_game_id(sportsbook_rows, "sportsbook")
        predictions_by_date = rows_by_date_game_id(prediction_rows, "predictions")

        dates = sorted(predictions_by_date.keys())

        log(f"Dates found from prediction row game_date values: {len(dates)}")

        if not dates:
            fail("No Stage 01 prediction rows found.")

        for date_val in dates:
            merged_count, rejected_sportsbook_count, rejected_predictions_count, date_has_failure = process_date(
                date_val,
                games_by_date.get(date_val, {}),
                sportsbook_by_date.get(date_val, {}),
                predictions_by_date.get(date_val, {}),
            )

            total_merged += merged_count
            total_rejected_sportsbook += rejected_sportsbook_count
            total_rejected_predictions += rejected_predictions_count

            if date_has_failure:
                dates_failed += 1

        log("--- SUMMARY ---")
        log(f"Dates processed: {len(dates)}")
        log(f"Dates with failures: {dates_failed}")
        log(f"Rows merged: {total_merged}")
        log(f"Rejected sportsbook rows: {total_rejected_sportsbook}")
        log(f"Rejected prediction rows: {total_rejected_predictions}")

        if dates_failed > 0:
            fail(f"Stage 01 merge audit failed for {dates_failed} date(s). See audit/rejection CSVs.")

        log("STATUS: SUCCESS")

    except SystemExit:
        raise
    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        raise


if __name__ == "__main__":
    main()
