#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/tests/test_game_id_reconciliation.py

from __future__ import annotations

import csv
import shutil
import subprocess
from pathlib import Path


BASE_DIR = Path("docs/win/hockey/nhl")

FIXTURE_ROOT = (
    BASE_DIR
    / "test_fixture"
    / "game_id_reconciliation"
)

FIXTURE_ODDS_DIR = FIXTURE_ROOT / "odds"
FIXTURE_PREDICTIONS_DIR = FIXTURE_ROOT / "predictions"
FIXTURE_SPORTSBOOK_DIR = FIXTURE_ROOT / "sportsbook"

ODDS_DIR = BASE_DIR / "odds"
SCRAPER_DIR = (
    BASE_DIR
    / "00_intake"
    / "predictions"
    / "scraper"
)
SPORTSBOOK_DIR = (
    BASE_DIR
    / "00_intake"
    / "sportsbook"
)
PREDICTIONS_DIR = (
    BASE_DIR
    / "00_intake"
    / "predictions"
)
SCHEDULE_DIR = (
    BASE_DIR
    / "00_intake"
    / "nhl_schedule"
)
RECONCILED_DIR = (
    BASE_DIR
    / "00_intake"
    / "reconciled"
)
GAMES_DIR = (
    BASE_DIR
    / "00_intake"
    / "games"
)

TEST_OUTPUT_ROOT = (
    BASE_DIR
    / "test_output"
    / "game_id_reconciliation"
)

SCRIPT_DIR = (
    BASE_DIR
    / "scripts"
    / "00_intake"
)

EXPECTED_OFFICIAL_GAME_ID = "2025020004"
EXPECTED_PROVIDER_ID = "fixture_sb_20251008_001"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        return list(csv.DictReader(handle))


def run_script(name: str) -> None:
    subprocess.run(
        [
            "python",
            str(SCRIPT_DIR / name),
        ],
        check=True,
    )


def remove_root_files(
    directory: Path,
    pattern: str,
) -> None:
    directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    for path in directory.glob(pattern):
        if path.is_file():
            path.unlink()


def recreate_directory(
    directory: Path,
) -> None:
    if directory.exists():
        shutil.rmtree(directory)

    directory.mkdir(
        parents=True,
        exist_ok=True,
    )


def fixture_dates() -> list[str]:
    odds_dates = {
        path.stem
        for path in FIXTURE_ODDS_DIR.glob("*.json")
    }

    prediction_dates = {
        path.name[
            : -len("_nhl_predictions.csv")
        ]
        for path in FIXTURE_PREDICTIONS_DIR.glob(
            "*_nhl_predictions.csv"
        )
    }

    sportsbook_dates = {
        path.stem[len("NHL_") :]
        for path in FIXTURE_SPORTSBOOK_DIR.glob(
            "NHL_*.csv"
        )
    }

    common = sorted(
        odds_dates
        & prediction_dates
        & sportsbook_dates
    )

    if len(common) != 1:
        raise RuntimeError(
            "Fixture tree must contain exactly one "
            "common test date across odds, predictions, "
            f"and sportsbook. Found: {common}"
        )

    return common


def prepare_fixture_inputs(
    test_date: str,
) -> None:
    remove_root_files(
        ODDS_DIR,
        "*.json",
    )

    remove_root_files(
        SCRAPER_DIR,
        "*_nhl_predictions.csv",
    )

    remove_root_files(
        SPORTSBOOK_DIR,
        "*.csv",
    )

    remove_root_files(
        PREDICTIONS_DIR,
        "hockey_*.csv",
    )

    recreate_directory(
        SCHEDULE_DIR
    )

    recreate_directory(
        RECONCILED_DIR
    )

    (
        RECONCILED_DIR
        / "audit"
    ).mkdir(
        parents=True,
        exist_ok=True,
    )

    recreate_directory(
        GAMES_DIR
    )

    shutil.copy2(
        FIXTURE_ODDS_DIR
        / f"{test_date}.json",
        ODDS_DIR
        / f"{test_date}.json",
    )

    shutil.copy2(
        FIXTURE_PREDICTIONS_DIR
        / f"{test_date}_nhl_predictions.csv",
        SCRAPER_DIR
        / f"{test_date}_nhl_predictions.csv",
    )

    shutil.copy2(
        FIXTURE_SPORTSBOOK_DIR
        / f"NHL_{test_date}.csv",
        SPORTSBOOK_DIR
        / f"NHL_{test_date}.csv",
    )


def validate_transformed_sportsbook(
    test_date: str,
) -> None:
    generated_path = (
        SPORTSBOOK_DIR
        / f"NHL_{test_date}.csv"
    )

    expected_path = (
        FIXTURE_SPORTSBOOK_DIR
        / f"NHL_{test_date}.csv"
    )

    if not generated_path.exists():
        raise RuntimeError(
            f"Sportsbook output missing: {generated_path}"
        )

    generated = read_csv(
        generated_path
    )

    expected = read_csv(
        expected_path
    )

    if generated != expected:
        raise RuntimeError(
            "Generated sportsbook output does not match "
            f"fixture sportsbook input: {expected_path}"
        )

    if len(generated) != 1:
        raise RuntimeError(
            "Fixture sportsbook must produce exactly "
            f"one row. Found: {len(generated)}"
        )

    row = generated[0]

    if "game_id" in row:
        raise RuntimeError(
            "Pre-reconciliation sportsbook output "
            "contains game_id."
        )

    if (
        row.get(
            "sportsbook_event_id",
            ""
        ).strip()
        != EXPECTED_PROVIDER_ID
    ):
        raise RuntimeError(
            "Unexpected sportsbook_event_id."
        )


def validate_transformed_predictions(
    test_date: str,
) -> None:
    path = (
        PREDICTIONS_DIR
        / f"hockey_{test_date}.csv"
    )

    if not path.exists():
        raise RuntimeError(
            f"Prediction output missing: {path}"
        )

    rows = read_csv(path)

    if len(rows) != 1:
        raise RuntimeError(
            "Fixture prediction must produce exactly "
            f"one row. Found: {len(rows)}"
        )

    if rows[0].get(
        "game_id",
        "",
    ).strip():
        raise RuntimeError(
            "Prediction received game_id before "
            "reconciliation."
        )


def validate_schedule(
    test_date: str,
) -> None:
    path = (
        SCHEDULE_DIR
        / f"NHL_{test_date}.csv"
    )

    if not path.exists():
        raise RuntimeError(
            f"Official NHL schedule missing: {path}"
        )

    rows = read_csv(path)

    if not rows:
        raise RuntimeError(
            "Official NHL schedule contains zero rows."
        )

    official_ids = {
        row.get(
            "game_id",
            "",
        ).strip()
        for row in rows
    }

    if EXPECTED_OFFICIAL_GAME_ID not in official_ids:
        raise RuntimeError(
            "Expected official NHL game_id "
            f"{EXPECTED_OFFICIAL_GAME_ID} "
            "not found in schedule."
        )

    bad_ids = sorted(
        game_id
        for game_id in official_ids
        if (
            len(game_id) != 10
            or not game_id.isdigit()
        )
    )

    if bad_ids:
        raise RuntimeError(
            f"Invalid official NHL game IDs: {bad_ids}"
        )


def validate_reconciliation(
    test_date: str,
) -> None:
    sportsbook_path = (
        SPORTSBOOK_DIR
        / f"NHL_{test_date}.csv"
    )

    prediction_path = (
        PREDICTIONS_DIR
        / f"hockey_{test_date}.csv"
    )

    reconciled_path = (
        RECONCILED_DIR
        / f"NHL_{test_date}.csv"
    )

    audit_path = (
        RECONCILED_DIR
        / "audit"
        / f"NHL_{test_date}_reconciliation.csv"
    )

    for path in (
        sportsbook_path,
        prediction_path,
        reconciled_path,
        audit_path,
    ):
        if not path.exists():
            raise RuntimeError(
                f"Expected reconciliation output missing: {path}"
            )

    sportsbook_rows = read_csv(
        sportsbook_path
    )

    prediction_rows = read_csv(
        prediction_path
    )

    reconciled_rows = read_csv(
        reconciled_path
    )

    audit_rows = read_csv(
        audit_path
    )

    if len(reconciled_rows) != 1:
        raise RuntimeError(
            "Expected exactly one reconciled game. "
            f"Found: {len(reconciled_rows)}"
        )

    row = reconciled_rows[0]

    if (
        row.get(
            "game_id",
            "",
        ).strip()
        != EXPECTED_OFFICIAL_GAME_ID
    ):
        raise RuntimeError(
            "Reconciled official game_id does not "
            "match expected NHL ID."
        )

    if (
        row.get(
            "sportsbook_event_id",
            "",
        ).strip()
        != EXPECTED_PROVIDER_ID
    ):
        raise RuntimeError(
            "Reconciled sportsbook_event_id does not "
            "match expected provider ID."
        )

    if (
        row["game_id"]
        == row["sportsbook_event_id"]
    ):
        raise RuntimeError(
            "Official NHL game_id equals "
            "sportsbook_event_id."
        )

    if (
        row.get(
            "home_team",
            "",
        ).strip()
        != "Toronto Maple Leafs"
    ):
        raise RuntimeError(
            "Home team was not corrected to "
            "Toronto Maple Leafs."
        )

    if (
        row.get(
            "away_team",
            "",
        ).strip()
        != "Montreal Canadiens"
    ):
        raise RuntimeError(
            "Montreal/Montréal normalization failed."
        )

    for source_name, rows in (
        (
            "sportsbook",
            sportsbook_rows,
        ),
        (
            "prediction",
            prediction_rows,
        ),
    ):
        if len(rows) != 1:
            raise RuntimeError(
                f"{source_name} reconciled output "
                "must contain one row."
            )

        source_row = rows[0]

        if (
            source_row.get(
                "game_id",
                "",
            ).strip()
            != EXPECTED_OFFICIAL_GAME_ID
        ):
            raise RuntimeError(
                f"{source_name} does not contain "
                "the official NHL game_id."
            )

    bad_audit = [
        audit
        for audit in audit_rows
        if audit.get(
            "status",
            "",
        ).strip()
        != "reconciled"
    ]

    if bad_audit:
        raise RuntimeError(
            "Reconciliation audit contains failures: "
            f"{bad_audit}"
        )

    corrections = [
        audit
        for audit in audit_rows
        if audit.get(
            "orientation_corrected",
            "",
        ).strip()
        == "yes"
    ]

    if not corrections:
        raise RuntimeError(
            "Fixture did not exercise reversed "
            "home/away correction."
        )


def validate_games(
    test_date: str,
) -> None:
    path = (
        GAMES_DIR
        / f"{test_date}_nhl_games.csv"
    )

    if not path.exists():
        raise RuntimeError(
            f"Games output missing: {path}"
        )

    rows = read_csv(path)

    if len(rows) != 1:
        raise RuntimeError(
            "Games output must contain exactly "
            f"one fixture row. Found: {len(rows)}"
        )

    row = rows[0]

    if (
        row.get(
            "game_id",
            "",
        ).strip()
        != EXPECTED_OFFICIAL_GAME_ID
    ):
        raise RuntimeError(
            "Games output official game_id mismatch."
        )

    if (
        row.get(
            "sportsbook_event_id",
            "",
        ).strip()
        != EXPECTED_PROVIDER_ID
    ):
        raise RuntimeError(
            "Games output sportsbook_event_id mismatch."
        )


def save_test_output(
    test_date: str,
) -> Path:
    output_dir = (
        TEST_OUTPUT_ROOT
        / test_date
    )

    if output_dir.exists():
        shutil.rmtree(
            output_dir
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    logs_dir = (
        output_dir
        / "logs"
    )

    logs_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    copies = {
        (
            SCHEDULE_DIR
            / f"NHL_{test_date}.csv"
        ): (
            output_dir
            / "nhl_schedule.csv"
        ),
        (
            SPORTSBOOK_DIR
            / f"NHL_{test_date}.csv"
        ): (
            output_dir
            / "sportsbook_reconciled.csv"
        ),
        (
            PREDICTIONS_DIR
            / f"hockey_{test_date}.csv"
        ): (
            output_dir
            / "predictions_reconciled.csv"
        ),
        (
            RECONCILED_DIR
            / f"NHL_{test_date}.csv"
        ): (
            output_dir
            / "reconciled_games.csv"
        ),
        (
            RECONCILED_DIR
            / "audit"
            / f"NHL_{test_date}_reconciliation.csv"
        ): (
            output_dir
            / "reconciliation_audit.csv"
        ),
        (
            GAMES_DIR
            / f"{test_date}_nhl_games.csv"
        ): (
            output_dir
            / "games.csv"
        ),
    }

    for source, target in copies.items():
        shutil.copy2(
            source,
            target,
        )

    error_dir = (
        BASE_DIR
        / "errors"
        / "00_intake"
    )

    for name in (
        "transform_hockey_odds.txt",
        "transform_hockey.txt",
        "pull_nhl_schedule.txt",
        "reconcile_game_ids.txt",
        "build_games.txt",
    ):
        source = error_dir / name

        if source.exists():
            shutil.copy2(
                source,
                logs_dir / name,
            )

    summary = [
        "NHL GAME ID RECONCILIATION TEST",
        f"TEST_DATE={test_date}",
        "STATUS=PASSED",
        "",
        (
            "official_nhl_game_id="
            f"{EXPECTED_OFFICIAL_GAME_ID}"
        ),
        (
            "sportsbook_event_id="
            f"{EXPECTED_PROVIDER_ID}"
        ),
        "fixture_source=game_id_reconciliation",
    ]

    (
        output_dir
        / "summary.txt"
    ).write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8",
    )

    return output_dir


def main() -> None:
    dates = fixture_dates()
    test_date = dates[0]

    print(
        f"Fixture test date: {test_date}"
    )

    prepare_fixture_inputs(
        test_date
    )

    run_script(
        "transform_hockey_odds.py"
    )

    validate_transformed_sportsbook(
        test_date
    )

    run_script(
        "transform_hockey.py"
    )

    validate_transformed_predictions(
        test_date
    )

    run_script(
        "pull_nhl_schedule.py"
    )

    validate_schedule(
        test_date
    )

    run_script(
        "reconcile_game_ids.py"
    )

    validate_reconciliation(
        test_date
    )

    run_script(
        "build_games.py"
    )

    validate_games(
        test_date
    )

    output_dir = save_test_output(
        test_date
    )

    print(
        "NHL GAME ID RECONCILIATION TEST PASSED"
    )

    print(
        f"test_date={test_date}"
    )

    print(
        "official_game_id="
        f"{EXPECTED_OFFICIAL_GAME_ID}"
    )

    print(
        "sportsbook_event_id="
        f"{EXPECTED_PROVIDER_ID}"
    )

    print(
        f"output_dir={output_dir}"
    )


if __name__ == "__main__":
    main()
