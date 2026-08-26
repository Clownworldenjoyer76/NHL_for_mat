#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import shutil
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path


BASE_DIR = Path("docs/win/hockey/nhl")
SEASON_NAME = "2025_2026"

SEASON_DIR = BASE_DIR / "season_master" / SEASON_NAME
AUDIT_DIR = SEASON_DIR / "audit"

GAMES_MASTER = SEASON_DIR / "games.csv"
PREDICTIONS_MASTER = SEASON_DIR / "predictions.csv"
SPORTSBOOK_MASTER = SEASON_DIR / "sportsbook.csv"

PREDICTIONS_DIR = BASE_DIR / "00_intake" / "predictions"
SPORTSBOOK_DIR = BASE_DIR / "00_intake" / "sportsbook"
RECONCILED_DIR = BASE_DIR / "00_intake" / "reconciled"
GAMES_DIR = BASE_DIR / "00_intake" / "games"

GAME_ID_RE = re.compile(r"^\d{10}$")
DATE_RE = re.compile(r"^\d{4}_\d{2}_\d{2}$")

IDENTITY_COLUMNS = [
    "game_id",
    "sportsbook_event_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
]

PREDICTION_COLUMNS = [
    "game_id",
    "sport",
    "league",
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

SPORTSBOOK_COLUMNS = [
    "game_id",
    "sportsbook_event_id",
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

SUMMARY_COLUMNS = [
    "metric",
    "value",
]


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing required season master: {path}"
        )

    with path.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = [dict(row) for row in reader]

    return fieldnames, rows


def write_csv(
    path: Path,
    columns: list[str],
    rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    column: str(
                        row.get(column, "")
                    ).strip()
                    for column in columns
                }
            )


def require_columns(
    path: Path,
    fieldnames: list[str],
    required: list[str],
) -> None:
    missing = [
        column
        for column in required
        if column not in fieldnames
    ]

    if missing:
        raise ValueError(
            f"{path} missing required columns: {missing}"
        )


def duplicate_values(
    rows: list[dict[str, str]],
    field: str,
    *,
    ignore_blank: bool = False,
) -> list[str]:
    values = [
        str(row.get(field, "")).strip()
        for row in rows
    ]

    if ignore_blank:
        values = [
            value
            for value in values
            if value
        ]

    counts = Counter(values)

    return sorted(
        value
        for value, count in counts.items()
        if count > 1
    )


def validate_game_ids(
    path: Path,
    rows: list[dict[str, str]],
) -> None:
    for row_number, row in enumerate(
        rows,
        start=2,
    ):
        game_id = str(
            row.get("game_id", "")
        ).strip()

        if not game_id:
            raise ValueError(
                f"{path} row {row_number} has blank game_id"
            )

        if not GAME_ID_RE.fullmatch(game_id):
            raise ValueError(
                f"{path} row {row_number} has "
                f"non-canonical game_id={game_id!r}"
            )

    duplicates = duplicate_values(
        rows,
        "game_id",
    )

    if duplicates:
        raise ValueError(
            f"{path} has duplicate game_id values: "
            f"{duplicates}"
        )


def validate_game_dates(
    path: Path,
    rows: list[dict[str, str]],
) -> None:
    for row_number, row in enumerate(
        rows,
        start=2,
    ):
        game_date = str(
            row.get("game_date", "")
        ).strip()

        if not DATE_RE.fullmatch(game_date):
            raise ValueError(
                f"{path} row {row_number} has "
                f"invalid game_date={game_date!r}"
            )


def validate_identity_against_games(
    source_path: Path,
    rows: list[dict[str, str]],
    games_by_id: dict[str, dict[str, str]],
) -> None:
    compare_columns = [
        "sport",
        "league",
        "game_date",
        "game_time",
        "home_team",
        "away_team",
    ]

    for row_number, row in enumerate(
        rows,
        start=2,
    ):
        game_id = str(
            row.get("game_id", "")
        ).strip()

        if game_id not in games_by_id:
            raise ValueError(
                f"{source_path} row {row_number} "
                f"game_id={game_id} does not exist "
                f"in {GAMES_MASTER}"
            )

        game = games_by_id[game_id]

        for column in compare_columns:
            source_value = str(
                row.get(column, "")
            ).strip()

            game_value = str(
                game.get(column, "")
            ).strip()

            if source_value != game_value:
                raise ValueError(
                    f"{source_path} row {row_number} "
                    f"game_id={game_id} "
                    f"{column} mismatch: "
                    f"{source_value!r} != {game_value!r}"
                )


def validate_provider_ids(
    games: list[dict[str, str]],
    sportsbook: list[dict[str, str]],
) -> None:
    duplicate_provider_ids = duplicate_values(
        sportsbook,
        "sportsbook_event_id",
        ignore_blank=True,
    )

    if duplicate_provider_ids:
        raise ValueError(
            "sportsbook.csv has duplicate "
            "sportsbook_event_id values: "
            f"{duplicate_provider_ids}"
        )

    nhl_ids = {
        str(row["game_id"]).strip()
        for row in games
    }

    sportsbook_by_game = {
        str(row["game_id"]).strip(): row
        for row in sportsbook
    }

    for row_number, row in enumerate(
        sportsbook,
        start=2,
    ):
        game_id = str(
            row.get("game_id", "")
        ).strip()

        provider_id = str(
            row.get(
                "sportsbook_event_id",
                "",
            )
        ).strip()

        if (
            provider_id
            and provider_id in nhl_ids
        ):
            raise ValueError(
                f"{SPORTSBOOK_MASTER} row "
                f"{row_number} uses NHL game_id "
                f"{provider_id} as "
                "sportsbook_event_id"
            )

    for row_number, game in enumerate(
        games,
        start=2,
    ):
        game_id = str(
            game["game_id"]
        ).strip()

        game_provider = str(
            game.get(
                "sportsbook_event_id",
                "",
            )
        ).strip()

        sportsbook_row = sportsbook_by_game.get(
            game_id
        )

        sportsbook_provider = (
            str(
                sportsbook_row.get(
                    "sportsbook_event_id",
                    "",
                )
            ).strip()
            if sportsbook_row
            else ""
        )

        if game_provider != sportsbook_provider:
            raise ValueError(
                f"{GAMES_MASTER} row {row_number} "
                f"game_id={game_id} has "
                "sportsbook_event_id mismatch: "
                f"{game_provider!r} != "
                f"{sportsbook_provider!r}"
            )


def load_and_validate_masters():
    game_fields, games = read_csv(
        GAMES_MASTER
    )

    prediction_fields, predictions = read_csv(
        PREDICTIONS_MASTER
    )

    sportsbook_fields, sportsbook = read_csv(
        SPORTSBOOK_MASTER
    )

    require_columns(
        GAMES_MASTER,
        game_fields,
        IDENTITY_COLUMNS,
    )

    require_columns(
        PREDICTIONS_MASTER,
        prediction_fields,
        PREDICTION_COLUMNS,
    )

    require_columns(
        SPORTSBOOK_MASTER,
        sportsbook_fields,
        SPORTSBOOK_COLUMNS,
    )

    if not games:
        raise ValueError(
            f"{GAMES_MASTER} contains no rows"
        )

    validate_game_ids(
        GAMES_MASTER,
        games,
    )

    validate_game_ids(
        PREDICTIONS_MASTER,
        predictions,
    )

    validate_game_ids(
        SPORTSBOOK_MASTER,
        sportsbook,
    )

    validate_game_dates(
        GAMES_MASTER,
        games,
    )

    validate_game_dates(
        PREDICTIONS_MASTER,
        predictions,
    )

    validate_game_dates(
        SPORTSBOOK_MASTER,
        sportsbook,
    )

    games_by_id = {
        str(row["game_id"]).strip(): row
        for row in games
    }

    validate_identity_against_games(
        PREDICTIONS_MASTER,
        predictions,
        games_by_id,
    )

    validate_identity_against_games(
        SPORTSBOOK_MASTER,
        sportsbook,
        games_by_id,
    )

    validate_provider_ids(
        games,
        sportsbook,
    )

    return (
        games,
        predictions,
        sportsbook,
    )


def group_by_date(
    rows: list[dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    grouped: dict[
        str,
        list[dict[str, str]],
    ] = defaultdict(list)

    for row in rows:
        grouped[
            str(
                row["game_date"]
            ).strip()
        ].append(row)

    for date_rows in grouped.values():
        date_rows.sort(
            key=lambda row: (
                str(
                    row.get(
                        "game_time",
                        "",
                    )
                ),
                str(row["game_id"]),
            )
        )

    return dict(grouped)


def identity_row(
    game: dict[str, str],
) -> dict[str, str]:
    return {
        column: str(
            game.get(column, "")
        ).strip()
        for column in IDENTITY_COLUMNS
    }


def validate_daily_rows(
    date_value: str,
    rows: list[dict[str, str]],
    *,
    require_all_games: bool,
    expected_game_ids: set[str],
) -> None:
    seen: set[str] = set()

    for row in rows:
        game_id = str(
            row.get(
                "game_id",
                "",
            )
        ).strip()

        game_date = str(
            row.get(
                "game_date",
                "",
            )
        ).strip()

        if not GAME_ID_RE.fullmatch(
            game_id
        ):
            raise ValueError(
                f"Daily rebuild produced invalid "
                f"game_id={game_id!r}"
            )

        if game_date != date_value:
            raise ValueError(
                "Daily rebuild crossed game dates: "
                f"expected={date_value} "
                f"actual={game_date}"
            )

        if game_id in seen:
            raise ValueError(
                "Daily rebuild produced duplicate "
                f"game_id={game_id}"
            )

        seen.add(game_id)

    if (
        require_all_games
        and seen != expected_game_ids
    ):
        raise ValueError(
            f"Daily identity rows for {date_value} "
            "do not exactly match games master"
        )


def stage_daily_files(
    staging_root: Path,
    games: list[dict[str, str]],
    predictions: list[dict[str, str]],
    sportsbook: list[dict[str, str]],
) -> tuple[
    list[str],
    int,
    int,
    int,
    int,
]:
    games_by_date = group_by_date(
        games
    )

    predictions_by_date = group_by_date(
        predictions
    )

    sportsbook_by_date = group_by_date(
        sportsbook
    )

    dates = sorted(
        games_by_date
    )

    prediction_count = 0
    sportsbook_count = 0
    reconciled_count = 0
    games_count = 0

    for date_value in dates:
        game_rows = games_by_date[
            date_value
        ]

        prediction_rows = predictions_by_date.get(
            date_value,
            [],
        )

        sportsbook_rows = sportsbook_by_date.get(
            date_value,
            [],
        )

        expected_game_ids = {
            row["game_id"]
            for row in game_rows
        }

        validate_daily_rows(
            date_value,
            game_rows,
            require_all_games=True,
            expected_game_ids=expected_game_ids,
        )

        validate_daily_rows(
            date_value,
            prediction_rows,
            require_all_games=False,
            expected_game_ids=expected_game_ids,
        )

        validate_daily_rows(
            date_value,
            sportsbook_rows,
            require_all_games=False,
            expected_game_ids=expected_game_ids,
        )

        if not {
            row["game_id"]
            for row in prediction_rows
        }.issubset(
            expected_game_ids
        ):
            raise ValueError(
                f"Prediction rows for {date_value} "
                "contain game IDs outside games master"
            )

        if not {
            row["game_id"]
            for row in sportsbook_rows
        }.issubset(
            expected_game_ids
        ):
            raise ValueError(
                f"Sportsbook rows for {date_value} "
                "contain game IDs outside games master"
            )

        reconciled_rows = [
            identity_row(row)
            for row in game_rows
        ]

        daily_game_rows = [
            identity_row(row)
            for row in game_rows
        ]

        validate_daily_rows(
            date_value,
            reconciled_rows,
            require_all_games=True,
            expected_game_ids=expected_game_ids,
        )

        validate_daily_rows(
            date_value,
            daily_game_rows,
            require_all_games=True,
            expected_game_ids=expected_game_ids,
        )

        write_csv(
            staging_root
            / "predictions"
            / f"hockey_{date_value}.csv",
            PREDICTION_COLUMNS,
            prediction_rows,
        )

        write_csv(
            staging_root
            / "sportsbook"
            / f"NHL_{date_value}.csv",
            SPORTSBOOK_COLUMNS,
            sportsbook_rows,
        )

        write_csv(
            staging_root
            / "reconciled"
            / f"NHL_{date_value}.csv",
            IDENTITY_COLUMNS,
            reconciled_rows,
        )

        write_csv(
            staging_root
            / "games"
            / f"{date_value}_nhl_games.csv",
            IDENTITY_COLUMNS,
            daily_game_rows,
        )

        prediction_count += len(
            prediction_rows
        )

        sportsbook_count += len(
            sportsbook_rows
        )

        reconciled_count += len(
            reconciled_rows
        )

        games_count += len(
            daily_game_rows
        )

    return (
        dates,
        prediction_count,
        sportsbook_count,
        reconciled_count,
        games_count,
    )


def remove_existing_date_file(
    directory: Path,
    pattern: re.Pattern[str],
) -> None:
    if not directory.exists():
        return

    for path in directory.iterdir():
        if (
            path.is_file()
            and pattern.fullmatch(
                path.name
            )
        ):
            path.unlink()


def install_staged_files(
    staging_root: Path,
    dates: list[str],
) -> None:
    PREDICTIONS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    SPORTSBOOK_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    RECONCILED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    GAMES_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    for date_value in dates:
        remove_existing_date_file(
            PREDICTIONS_DIR,
            re.compile(
                rf"(?i)hockey_"
                rf"{re.escape(date_value)}"
                r"\.csv"
            ),
        )

        remove_existing_date_file(
            SPORTSBOOK_DIR,
            re.compile(
                rf"(?i)nhl_"
                rf"{re.escape(date_value)}"
                r"\.csv"
            ),
        )

        remove_existing_date_file(
            RECONCILED_DIR,
            re.compile(
                rf"(?i)nhl_"
                rf"{re.escape(date_value)}"
                r"\.csv"
            ),
        )

        remove_existing_date_file(
            GAMES_DIR,
            re.compile(
                rf"{re.escape(date_value)}"
                r"_nhl_games\.csv",
                re.IGNORECASE,
            ),
        )

        shutil.move(
            str(
                staging_root
                / "predictions"
                / f"hockey_{date_value}.csv"
            ),
            str(
                PREDICTIONS_DIR
                / f"hockey_{date_value}.csv"
            ),
        )

        shutil.move(
            str(
                staging_root
                / "sportsbook"
                / f"NHL_{date_value}.csv"
            ),
            str(
                SPORTSBOOK_DIR
                / f"NHL_{date_value}.csv"
            ),
        )

        shutil.move(
            str(
                staging_root
                / "reconciled"
                / f"NHL_{date_value}.csv"
            ),
            str(
                RECONCILED_DIR
                / f"NHL_{date_value}.csv"
            ),
        )

        shutil.move(
            str(
                staging_root
                / "games"
                / f"{date_value}_nhl_games.csv"
            ),
            str(
                GAMES_DIR
                / f"{date_value}_nhl_games.csv"
            ),
        )


def write_summary(
    *,
    dates: list[str],
    games: list[dict[str, str]],
    predictions: list[dict[str, str]],
    sportsbook: list[dict[str, str]],
    reconciled_count: int,
    games_count: int,
) -> None:
    missing_provider = sum(
        not str(
            row.get(
                "sportsbook_event_id",
                "",
            )
        ).strip()
        for row in sportsbook
    )

    prediction_ids = {
        row["game_id"]
        for row in predictions
    }

    sportsbook_ids = {
        row["game_id"]
        for row in sportsbook
    }

    games_missing_prediction = sum(
        row["game_id"]
        not in prediction_ids
        for row in games
    )

    games_missing_sportsbook = sum(
        row["game_id"]
        not in sportsbook_ids
        for row in games
    )

    summary = [
        {
            "metric": "dates_written",
            "value": str(len(dates)),
        },
        {
            "metric": "games_rows",
            "value": str(len(games)),
        },
        {
            "metric": "prediction_rows",
            "value": str(len(predictions)),
        },
        {
            "metric": "sportsbook_rows",
            "value": str(len(sportsbook)),
        },
        {
            "metric": "reconciled_rows",
            "value": str(reconciled_count),
        },
        {
            "metric": "daily_games_rows",
            "value": str(games_count),
        },
        {
            "metric": "missing_sportsbook_event_id",
            "value": str(missing_provider),
        },
        {
            "metric": "games_missing_prediction",
            "value": str(games_missing_prediction),
        },
        {
            "metric": "games_missing_sportsbook",
            "value": str(games_missing_sportsbook),
        },
    ]

    write_csv(
        AUDIT_DIR
        / "daily_rebuild_summary.csv",
        SUMMARY_COLUMNS,
        summary,
    )


def main() -> int:
    (
        games,
        predictions,
        sportsbook,
    ) = load_and_validate_masters()

    staging_root = Path(
        tempfile.mkdtemp(
            prefix=".daily_rebuild_",
            dir=SEASON_DIR,
        )
    )

    try:
        (
            dates,
            prediction_count,
            sportsbook_count,
            reconciled_count,
            games_count,
        ) = stage_daily_files(
            staging_root,
            games,
            predictions,
            sportsbook,
        )

        if prediction_count != len(
            predictions
        ):
            raise ValueError(
                "Prediction row count changed "
                "during daily split"
            )

        if sportsbook_count != len(
            sportsbook
        ):
            raise ValueError(
                "Sportsbook row count changed "
                "during daily split"
            )

        if reconciled_count != len(
            games
        ):
            raise ValueError(
                "Reconciled row count does not "
                "equal games master"
            )

        if games_count != len(games):
            raise ValueError(
                "Daily games row count does not "
                "equal games master"
            )

        install_staged_files(
            staging_root,
            dates,
        )

        write_summary(
            dates=dates,
            games=games,
            predictions=predictions,
            sportsbook=sportsbook,
            reconciled_count=reconciled_count,
            games_count=games_count,
        )

    finally:
        if staging_root.exists():
            shutil.rmtree(
                staging_root,
                ignore_errors=True,
            )

    print(
        "DAILY STAGE 00 REBUILD PASSED"
    )
    print(
        f"dates_written={len(dates)}"
    )
    print(
        f"games_rows={len(games)}"
    )
    print(
        f"prediction_rows={len(predictions)}"
    )
    print(
        f"sportsbook_rows={len(sportsbook)}"
    )
    print(
        f"reconciled_rows={reconciled_count}"
    )
    print(
        f"daily_games_rows={games_count}"
    )
    print(
        "output_predictions="
        f"{PREDICTIONS_DIR}"
    )
    print(
        "output_sportsbook="
        f"{SPORTSBOOK_DIR}"
    )
    print(
        "output_reconciled="
        f"{RECONCILED_DIR}"
    )
    print(
        f"output_games={GAMES_DIR}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)
