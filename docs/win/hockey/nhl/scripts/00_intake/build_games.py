# docs/win/hockey/nhl/scripts/00_intake/build_games.py

from __future__ import annotations

import csv
import sys
import traceback
from datetime import datetime
from pathlib import Path


SPORTBOOK_DIR = Path("docs/win/hockey/nhl/00_intake/sportsbook")
GAMES_DIR = Path("docs/win/hockey/nhl/00_intake/games")
LOG_PATH = Path("docs/win/hockey/nhl/errors/00_intake/build_games.txt")

INPUT_SUFFIX = ".csv"
OUTPUT_SUFFIX = "_nhl_games.csv"

REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
]

OUTPUT_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
]


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def write_log(lines: list[str]) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_log(lines: list[str]) -> None:
    print("\n".join(lines), file=sys.stderr)


def fail(lines: list[str], message: str) -> None:
    lines.append(f"ERROR: {message}")
    lines.append(f"Finished: {now_stamp()}")
    write_log(lines)
    print_log(lines)
    raise SystemExit(1)


def is_valid_input_file(path: Path) -> bool:
    name = path.name

    if not name.endswith(INPUT_SUFFIX):
        return False

    return name.startswith("NHL_") or name.startswith("nhl")


def extract_date_from_input_name(path: Path) -> str:
    name = path.name

    if not name.endswith(INPUT_SUFFIX):
        raise ValueError(f"Invalid sportsbook filename format: {name}")

    if name.startswith("NHL_"):
        date_value = name[len("NHL_") : -len(INPUT_SUFFIX)]
    elif name.startswith("nhl"):
        date_value = name[len("nhl") : -len(INPUT_SUFFIX)].lstrip("_-")
    else:
        raise ValueError(f"Invalid sportsbook filename format: {name}")

    if not date_value:
        raise ValueError(f"Missing date in sportsbook filename: {name}")

    return date_value


def read_sportsbook_file(path: Path, log_lines: list[str]) -> tuple[str, list[dict[str, str]]]:
    file_date = extract_date_from_input_name(path)

    with path.open("r", newline="", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)

        if reader.fieldnames is None:
            fail(log_lines, f"{path} has no header row.")

        missing_columns = [col for col in REQUIRED_COLUMNS if col not in reader.fieldnames]
        if missing_columns:
            fail(
                log_lines,
                f"{path} missing required columns: {', '.join(missing_columns)}",
            )

        rows: list[dict[str, str]] = []
        game_dates_seen: set[str] = set()

        for row_number, row in enumerate(reader, start=2):
            output_row = {col: (row.get(col) or "").strip() for col in OUTPUT_COLUMNS}

            missing_values = [col for col in OUTPUT_COLUMNS if output_row[col] == ""]
            if missing_values:
                fail(
                    log_lines,
                    f"{path} row {row_number} missing values for: {', '.join(missing_values)}",
                )

            game_dates_seen.add(output_row["game_date"])
            rows.append(output_row)

    if not rows:
        fail(log_lines, f"{path} contains no game rows.")

    if len(game_dates_seen) != 1:
        fail(
            log_lines,
            f"{path} contains multiple game_date values: {', '.join(sorted(game_dates_seen))}",
        )

    game_date = next(iter(game_dates_seen))

    if game_date != file_date:
        fail(
            log_lines,
            f"{path} filename date {file_date} does not match game_date column {game_date}.",
        )

    return game_date, rows


def write_games_file(game_date: str, rows: list[dict[str, str]]) -> Path:
    GAMES_DIR.mkdir(parents=True, exist_ok=True)

    output_path = GAMES_DIR / f"{game_date}{OUTPUT_SUFFIX}"

    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def remove_stale_games_files(valid_dates: set[str], log_lines: list[str]) -> None:
    GAMES_DIR.mkdir(parents=True, exist_ok=True)

    for path in sorted(GAMES_DIR.glob(f"*{OUTPUT_SUFFIX}")):
        output_date = path.name[: -len(OUTPUT_SUFFIX)]

        if output_date not in valid_dates:
            path.unlink()
            log_lines.append(f"Removed stale games file: {path}")


def main() -> None:
    log_lines = [
        "NHL build_games.py summary",
        f"Started: {now_stamp()}",
        f"Input directory: {SPORTBOOK_DIR}",
        f"Output directory: {GAMES_DIR}",
        f"Log path: {LOG_PATH}",
        "",
    ]

    if not SPORTBOOK_DIR.exists():
        fail(log_lines, f"Sportsbook directory does not exist: {SPORTBOOK_DIR}")

    sportsbook_files = sorted(
        path for path in SPORTBOOK_DIR.iterdir()
        if path.is_file() and is_valid_input_file(path)
    )

    if not sportsbook_files:
        fail(log_lines, f"No sportsbook files found in {SPORTBOOK_DIR}")

    valid_dates: set[str] = set()
    written_files: list[Path] = []
    total_rows = 0

    for sportsbook_path in sportsbook_files:
        log_lines.append(f"Processing sportsbook file: {sportsbook_path}")

        game_date, rows = read_sportsbook_file(sportsbook_path, log_lines)

        valid_dates.add(game_date)
        total_rows += len(rows)

        output_path = write_games_file(game_date, rows)
        written_files.append(output_path)

        log_lines.append(f"Date: {game_date}")
        log_lines.append(f"Rows written: {len(rows)}")
        log_lines.append(f"Output file: {output_path}")
        log_lines.append("")

    remove_stale_games_files(valid_dates, log_lines)

    log_lines.extend(
        [
            "Completed successfully.",
            f"Sportsbook files processed: {len(sportsbook_files)}",
            f"Games files written: {len(written_files)}",
            f"Total rows written: {total_rows}",
            f"Finished: {now_stamp()}",
        ]
    )

    write_log(log_lines)
    print_log(log_lines)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        lines = [
            "NHL build_games.py summary",
            f"Started: {now_stamp()}",
            f"Input directory: {SPORTBOOK_DIR}",
            f"Output directory: {GAMES_DIR}",
            f"Log path: {LOG_PATH}",
            "",
            f"ERROR: Unhandled exception: {exc}",
            "",
            "TRACEBACK:",
            traceback.format_exc(),
            f"Finished: {now_stamp()}",
        ]
        write_log(lines)
        print_log(lines)
        raise SystemExit(1)
