#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/01_merge/merge_intake.py

import csv
import re
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

GAME_ID_RE = re.compile(r"^\d{10}$")
GAME_DATE_RE = re.compile(r"^\d{4}_\d{2}_\d{2}$")


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
    f.write(
        f"=== merge_intake RUN "
        f"{datetime.now(UTC).isoformat()} ===\n"
    )


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"{datetime.now(UTC).isoformat()} | {msg}\n"
        )


def fail(message: str) -> None:
    log(f"FATAL: {message}")
    log("STATUS: FAILED")
    raise SystemExit(message)


def wipe_merge_outputs() -> None:
    removed_merge = 0
    removed_audit = 0

    for path in MERGE_DIR.glob("*.csv"):
        path.unlink()
        removed_merge += 1

    for path in AUDIT_DIR.glob("*.csv"):
        path.unlink()
        removed_audit += 1

    log(
        f"Wiped merge CSV outputs: {removed_merge}"
    )
    log(
        f"Wiped merge audit CSV outputs: {removed_audit}"
    )


def load_csv(
    path: Path,
) -> tuple[list[str], list[dict[str, str]]]:
    with open(
        path,
        newline="",
        encoding="utf-8-sig",
    ) as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    return fieldnames, rows


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)

    log(
        f"WROTE {path} ({len(rows)} rows)"
    )


def validate_required_columns(
    path: Path,
    fieldnames: list[str],
    required_columns: list[str],
) -> None:
    missing = [
        col
        for col in required_columns
        if col not in fieldnames
    ]

    if missing:
        fail(
            f"{path} missing required columns: "
            f"{missing}"
        )


def row_date(
    row: dict[str, str],
) -> str:
    return str(
        row.get("game_date", "")
    ).strip()


def row_game_id(
    row: dict[str, str],
) -> str:
    return str(
        row.get("game_id", "")
    ).strip()


def parse_game_date(
    value: str,
):
    value = str(value).strip()

    if not GAME_DATE_RE.fullmatch(value):
        return None

    try:
        return datetime.strptime(
            value,
            "%Y_%m_%d",
        ).date()

    except ValueError:
        return None


def validate_source_rows(
    path: Path,
    rows: list[dict[str, str]],
    source_name: str,
) -> list[dict[str, str]]:
    seen_game_ids: set[str] = set()

    for row_number, row in enumerate(
        rows,
        start=2,
    ):
        game_id = row_game_id(row)
        game_date = row_date(row)

        if not game_id:
            fail(
                f"{source_name} file has blank "
                f"game_id: {path} row={row_number}"
            )

        if not GAME_ID_RE.fullmatch(game_id):
            fail(
                f"{source_name} file has "
                f"non-canonical game_id: "
                f"{path} row={row_number} "
                f"game_id={game_id!r}"
            )

        if game_id in seen_game_ids:
            fail(
                f"{source_name} file has duplicate "
                f"game_id: {path} "
                f"game_id={game_id}"
            )

        seen_game_ids.add(game_id)

        if parse_game_date(game_date) is None:
            fail(
                f"{source_name} file has invalid "
                f"game_date: {path} "
                f"row={row_number} "
                f"game_date={game_date!r}"
            )

        home_team = str(
            row.get("home_team", "")
        ).strip()

        away_team = str(
            row.get("away_team", "")
        ).strip()

        if not home_team or not away_team:
            fail(
                f"{source_name} file has blank "
                f"team identity: {path} "
                f"row={row_number} "
                f"game_id={game_id}"
            )

    return rows


def load_source_rows(
    source_name: str,
    directory: Path,
    pattern: str,
    required_columns: list[str],
) -> list[dict[str, str]]:
    all_rows: list[dict[str, str]] = []

    if source_name == "sportsbook":
        files = sorted(
            path
            for path in directory.glob("*.csv")
            if path.name.lower().startswith("nhl_")
        )
    else:
        files = sorted(
            directory.glob(pattern)
        )

    log(
        f"{source_name} files found: "
        f"{len(files)}"
    )

    if not files:
        fail(
            f"No {source_name} files found in "
            f"{directory} matching {pattern}"
        )

    for path in files:
        fieldnames, rows = load_csv(path)

        validate_required_columns(
            path,
            fieldnames,
            required_columns,
        )

        rows = validate_source_rows(
            path,
            rows,
            source_name,
        )

        for row in rows:
            row["_source_file"] = str(path)
            all_rows.append(row)

        log(
            f"Loaded {source_name} file: "
            f"{path} ({len(rows)} usable rows)"
        )

    return all_rows


def rows_by_date_game_id(
    rows: list[dict[str, str]],
    source_name: str,
) -> dict[
    str,
    dict[str, dict[str, str]],
]:
    grouped: dict[
        str,
        dict[str, dict[str, str]],
    ] = {}

    seen_game_ids: dict[str, str] = {}

    for row_number, row in enumerate(
        rows,
        start=1,
    ):
        game_date = row_date(row)
        game_id = row_game_id(row)

        if not game_date:
            fail(
                f"{source_name} row has blank "
                f"game_date: "
                f"source_file="
                f"{row.get('_source_file', '')} "
                f"row={row_number} "
                f"game_id={game_id}"
            )

        if not game_id:
            fail(
                f"{source_name} row reached "
                f"grouping with blank game_id: "
                f"source_file="
                f"{row.get('_source_file', '')} "
                f"row={row_number} "
                f"game_date={game_date}"
            )

        if game_id in seen_game_ids:
            fail(
                f"{source_name} has duplicate "
                f"game_id across daily files: "
                f"game_id={game_id} "
                f"first_file="
                f"{seen_game_ids[game_id]} "
                f"duplicate_file="
                f"{row.get('_source_file', '')}"
            )

        seen_game_ids[game_id] = row.get(
            "_source_file",
            "",
        )

        grouped.setdefault(
            game_date,
            {},
        )[game_id] = row

    return grouped


def rows_by_game_id(
    rows: list[dict[str, str]],
) -> dict[str, dict[str, str]]:
    return {
        row_game_id(row): row
        for row in rows
    }


def validate_source_identity_against_games(
    source_name: str,
    source_rows: list[dict[str, str]],
    games_by_id: dict[str, dict[str, str]],
) -> None:
    identity_fields = [
        "sport",
        "league",
        "game_date",
        "game_time",
        "home_team",
        "away_team",
    ]

    problems: list[str] = []

    for row in source_rows:
        game_id = row_game_id(row)
        source_file = row.get(
            "_source_file",
            "",
        )

        game = games_by_id.get(game_id)

        if game is None:
            problems.append(
                f"orphan {source_name} row | "
                f"game_id={game_id} | "
                f"file={source_file}"
            )

            continue

        for field in identity_fields:
            source_value = str(
                row.get(field, "")
            ).strip()

            game_value = str(
                game.get(field, "")
            ).strip()

            if source_value != game_value:
                problems.append(
                    f"{source_name} identity mismatch | "
                    f"game_id={game_id} | "
                    f"field={field} | "
                    f"source={source_value!r} | "
                    f"games={game_value!r} | "
                    f"file={source_file}"
                )

    if problems:
        preview = problems[:25]

        if len(problems) > 25:
            preview.append(
                f"... plus "
                f"{len(problems) - 25} more"
            )

        fail(
            "\n".join(preview)
        )


def rejection_from_row(
    reason: str,
    row: dict[str, str],
) -> dict[str, str]:
    return {
        "reason": reason,
        "game_id": str(
            row.get("game_id", "")
        ).strip(),
        "sport": row.get(
            "sport",
            "",
        ),
        "league": row.get(
            "league",
            "",
        ),
        "game_date": row.get(
            "game_date",
            "",
        ),
        "game_time": row.get(
            "game_time",
            "",
        ),
        "away_team": row.get(
            "away_team",
            "",
        ),
        "home_team": row.get(
            "home_team",
            "",
        ),
    }


def process_date(
    date_val: str,
    games_map: dict[str, dict[str, str]],
    sportsbook_map: dict[str, dict[str, str]],
    predictions_map: dict[str, dict[str, str]],
) -> tuple[
    int,
    int,
    int,
    int,
    bool,
]:
    merged_path = (
        MERGE_DIR
        / f"{date_val}_NHL_merged.csv"
    )

    audit_path = (
        AUDIT_DIR
        / f"{date_val}_NHL_merge_audit.csv"
    )

    rejected_sportsbook_path = (
        AUDIT_DIR
        / f"{date_val}_NHL_rejected_sportsbook.csv"
    )

    rejected_predictions_path = (
        AUDIT_DIR
        / f"{date_val}_NHL_rejected_predictions.csv"
    )

    log(
        f"Processing game_date: {date_val}"
    )

    log(
        f"Games rows for date: "
        f"{len(games_map)}"
    )

    log(
        f"Sportsbook rows for date: "
        f"{len(sportsbook_map)}"
    )

    log(
        f"Prediction rows for date: "
        f"{len(predictions_map)}"
    )

    hard_failure = False

    audit_rows: list[
        dict[str, str]
    ] = []

    rejected_sportsbook: list[
        dict[str, str]
    ] = []

    rejected_predictions: list[
        dict[str, str]
    ] = []

    merged_rows: list[
        dict[str, str]
    ] = []

    missing_source_games = 0

    for game_id, row in sportsbook_map.items():
        if game_id not in games_map:
            hard_failure = True

            rejected_sportsbook.append(
                rejection_from_row(
                    "sportsbook_row_not_found_in_games",
                    row,
                )
            )

    for game_id, row in predictions_map.items():
        if game_id not in games_map:
            hard_failure = True

            rejected_predictions.append(
                rejection_from_row(
                    "prediction_row_not_found_in_games",
                    row,
                )
            )

    for game_id, game in games_map.items():
        has_sportsbook = (
            game_id in sportsbook_map
        )

        has_prediction = (
            game_id in predictions_map
        )

        if (
            has_sportsbook
            and has_prediction
        ):
            status = "matched"

        elif (
            not has_sportsbook
            and not has_prediction
        ):
            status = (
                "missing_sportsbook_and_prediction"
            )

            missing_source_games += 1

        elif not has_sportsbook:
            status = "missing_sportsbook"
            missing_source_games += 1

        else:
            status = "missing_prediction"
            missing_source_games += 1

        audit_rows.append(
            {
                "game_date": game.get(
                    "game_date",
                    date_val,
                ),
                "game_id": game_id,
                "away_team": game.get(
                    "away_team",
                    "",
                ),
                "home_team": game.get(
                    "home_team",
                    "",
                ),
                "source_present_games": "1",
                "source_present_sportsbook": (
                    "1"
                    if has_sportsbook
                    else "0"
                ),
                "source_present_predictions": (
                    "1"
                    if has_prediction
                    else "0"
                ),
                "status": status,
            }
        )

        if status != "matched":
            continue

        sportsbook = sportsbook_map[
            game_id
        ]

        prediction = predictions_map[
            game_id
        ]

        merged_rows.append(
            {
                "sport": game.get(
                    "sport",
                    "hockey",
                ),
                "league": game.get(
                    "league",
                    "nhl",
                ),
                "game_date": game.get(
                    "game_date",
                    date_val,
                ),
                "game_time": game.get(
                    "game_time",
                    "",
                ),
                "game_id": game_id,
                "away_team": game.get(
                    "away_team",
                    "",
                ),
                "home_team": game.get(
                    "home_team",
                    "",
                ),
                "away_prob_moneyline": (
                    prediction.get(
                        "away_prob_moneyline",
                        "",
                    )
                ),
                "home_prob_moneyline": (
                    prediction.get(
                        "home_prob_moneyline",
                        "",
                    )
                ),
                "away_projected_goals": (
                    prediction.get(
                        "away_projected_goals",
                        "",
                    )
                ),
                "home_projected_goals": (
                    prediction.get(
                        "home_projected_goals",
                        "",
                    )
                ),
                "total_projected_goals": (
                    prediction.get(
                        "total_projected_goals",
                        "",
                    )
                ),
                "away_puck_line": (
                    sportsbook.get(
                        "away_puck_line",
                        "",
                    )
                ),
                "home_puck_line": (
                    sportsbook.get(
                        "home_puck_line",
                        "",
                    )
                ),
                "total": sportsbook.get(
                    "total",
                    "",
                ),
                "away_dk_moneyline_american": (
                    sportsbook.get(
                        "away_dk_moneyline_american",
                        "",
                    )
                ),
                "home_dk_moneyline_american": (
                    sportsbook.get(
                        "home_dk_moneyline_american",
                        "",
                    )
                ),
                "away_dk_moneyline_decimal": (
                    sportsbook.get(
                        "away_dk_moneyline_decimal",
                        "",
                    )
                ),
                "home_dk_moneyline_decimal": (
                    sportsbook.get(
                        "home_dk_moneyline_decimal",
                        "",
                    )
                ),
                "away_dk_puck_line_american": (
                    sportsbook.get(
                        "away_dk_puck_line_american",
                        "",
                    )
                ),
                "home_dk_puck_line_american": (
                    sportsbook.get(
                        "home_dk_puck_line_american",
                        "",
                    )
                ),
                "away_dk_puck_line_decimal": (
                    sportsbook.get(
                        "away_dk_puck_line_decimal",
                        "",
                    )
                ),
                "home_dk_puck_line_decimal": (
                    sportsbook.get(
                        "home_dk_puck_line_decimal",
                        "",
                    )
                ),
                "dk_total_over_american": (
                    sportsbook.get(
                        "dk_total_over_american",
                        "",
                    )
                ),
                "dk_total_under_american": (
                    sportsbook.get(
                        "dk_total_under_american",
                        "",
                    )
                ),
                "dk_total_over_decimal": (
                    sportsbook.get(
                        "dk_total_over_decimal",
                        "",
                    )
                ),
                "dk_total_under_decimal": (
                    sportsbook.get(
                        "dk_total_under_decimal",
                        "",
                    )
                ),
            }
        )

    write_csv(
        audit_path,
        AUDIT_COLUMNS,
        audit_rows,
    )

    write_csv(
        rejected_sportsbook_path,
        REJECTION_COLUMNS,
        rejected_sportsbook,
    )

    write_csv(
        rejected_predictions_path,
        REJECTION_COLUMNS,
        rejected_predictions,
    )

    if merged_rows:
        write_csv(
            merged_path,
            MERGED_COLUMNS,
            merged_rows,
        )
    else:
        log(
            f"No merged rows written for "
            f"{date_val}"
        )

    log(
        f"Date summary {date_val}: "
        f"games={len(games_map)} "
        f"sportsbook={len(sportsbook_map)} "
        f"predictions={len(predictions_map)} "
        f"merged={len(merged_rows)} "
        f"missing_source_games="
        f"{missing_source_games} "
        f"rejected_sportsbook="
        f"{len(rejected_sportsbook)} "
        f"rejected_predictions="
        f"{len(rejected_predictions)} "
        f"hard_failure={hard_failure}"
    )

    return (
        len(merged_rows),
        len(rejected_sportsbook),
        len(rejected_predictions),
        missing_source_games,
        hard_failure,
    )


def main() -> None:
    total_merged = 0
    total_rejected_sportsbook = 0
    total_rejected_predictions = 0
    total_missing_source_games = 0
    dates_with_missing_sources = 0
    dates_with_hard_failures = 0

    try:
        games_rows = load_source_rows(
            "games",
            GAMES_DIR,
            "*_nhl_games.csv",
            REQUIRED_GAMES_COLUMNS,
        )

        sportsbook_rows = load_source_rows(
            "sportsbook",
            SPORTSBOOK_DIR,
            "NHL_*.csv",
            REQUIRED_SPORTSBOOK_COLUMNS,
        )

        prediction_rows = load_source_rows(
            "predictions",
            PREDICTIONS_DIR,
            "hockey_*.csv",
            REQUIRED_PREDICTION_COLUMNS,
        )

        games_by_date = (
            rows_by_date_game_id(
                games_rows,
                "games",
            )
        )

        sportsbook_by_date = (
            rows_by_date_game_id(
                sportsbook_rows,
                "sportsbook",
            )
        )

        predictions_by_date = (
            rows_by_date_game_id(
                prediction_rows,
                "predictions",
            )
        )

        games_by_id = rows_by_game_id(
            games_rows
        )

        validate_source_identity_against_games(
            "sportsbook",
            sportsbook_rows,
            games_by_id,
        )

        validate_source_identity_against_games(
            "predictions",
            prediction_rows,
            games_by_id,
        )

        dates = sorted(
            games_by_date.keys()
        )

        log(
            f"Dates found from canonical "
            f"games rows: {len(dates)}"
        )

        if not dates:
            fail(
                "No Stage 00 games rows found."
            )

        extra_sportsbook_dates = sorted(
            set(sportsbook_by_date)
            - set(games_by_date)
        )

        if extra_sportsbook_dates:
            fail(
                "Sportsbook contains dates not "
                "present in canonical games: "
                f"{extra_sportsbook_dates}"
            )

        extra_prediction_dates = sorted(
            set(predictions_by_date)
            - set(games_by_date)
        )

        if extra_prediction_dates:
            fail(
                "Predictions contain dates not "
                "present in canonical games: "
                f"{extra_prediction_dates}"
            )

        wipe_merge_outputs()

        for date_val in dates:
            (
                merged_count,
                rejected_sportsbook_count,
                rejected_predictions_count,
                missing_source_count,
                hard_failure,
            ) = process_date(
                date_val,
                games_by_date.get(
                    date_val,
                    {},
                ),
                sportsbook_by_date.get(
                    date_val,
                    {},
                ),
                predictions_by_date.get(
                    date_val,
                    {},
                ),
            )

            total_merged += merged_count

            total_rejected_sportsbook += (
                rejected_sportsbook_count
            )

            total_rejected_predictions += (
                rejected_predictions_count
            )

            total_missing_source_games += (
                missing_source_count
            )

            if missing_source_count > 0:
                dates_with_missing_sources += 1

            if hard_failure:
                dates_with_hard_failures += 1

        log("--- SUMMARY ---")
        log(
            f"Dates processed: {len(dates)}"
        )
        log(
            "Dates with missing source games: "
            f"{dates_with_missing_sources}"
        )
        log(
            "Games missing prediction and/or "
            "sportsbook source: "
            f"{total_missing_source_games}"
        )
        log(
            "Dates with hard failures: "
            f"{dates_with_hard_failures}"
        )
        log(
            f"Rows merged: {total_merged}"
        )
        log(
            "Rejected sportsbook rows: "
            f"{total_rejected_sportsbook}"
        )
        log(
            "Rejected prediction rows: "
            f"{total_rejected_predictions}"
        )

        if dates_with_hard_failures > 0:
            fail(
                "Stage 01 merge failed for "
                f"{dates_with_hard_failures} "
                "date(s) due to invalid/orphan "
                "source rows. See audit and "
                "rejection CSVs."
            )

        log("STATUS: SUCCESS")

        print("STAGE 01 MERGE PASSED")
        print(
            f"dates_processed={len(dates)}"
        )
        print(
            f"rows_merged={total_merged}"
        )
        print(
            "games_missing_sources="
            f"{total_missing_source_games}"
        )
        print(
            "dates_with_missing_sources="
            f"{dates_with_missing_sources}"
        )
        print(
            "rejected_sportsbook_rows="
            f"{total_rejected_sportsbook}"
        )
        print(
            "rejected_prediction_rows="
            f"{total_rejected_predictions}"
        )

    except SystemExit:
        raise

    except Exception as exc:
        log(
            f"FATAL ERROR: {exc}\n"
            f"{traceback.format_exc()}"
        )
        log("STATUS: FAILED")
        raise


if __name__ == "__main__":
    main()