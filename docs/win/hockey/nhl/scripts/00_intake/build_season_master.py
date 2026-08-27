#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

BASE_DIR = Path("docs/win/hockey/nhl")

SEASON_CODE = "20252026"
SEASON_NAME = "2025_2026"

SEASON_DIR = BASE_DIR / "season_master" / SEASON_NAME
AUDIT_DIR = SEASON_DIR / "audit"

SCHEDULE_DIR = BASE_DIR / "00_intake" / "nhl_schedule"
PREDICTIONS_DIR = BASE_DIR / "00_intake" / "predictions"
SPORTSBOOK_DIR = BASE_DIR / "00_intake" / "sportsbook"

TEAM_MAP_PATH = (
    BASE_DIR
    / "config"
    / "mapping"
    / "team_map_nhl.csv"
)

GAMES_PATH = SEASON_DIR / "games.csv"
PREDICTIONS_PATH = SEASON_DIR / "predictions.csv"
SPORTSBOOK_PATH = SEASON_DIR / "sportsbook.csv"

GAME_ID_RE = re.compile(r"^\d{10}$")
DATE_RE = re.compile(r"^\d{4}_\d{2}_\d{2}$")

GAME_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "sportsbook_event_id",
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

RECON_AUDIT_COLUMNS = [
    "source",
    "source_file",
    "source_row",
    "source_game_id",
    "sportsbook_event_id",
    "game_date",
    "source_home_team",
    "source_away_team",
    "official_game_id",
    "official_home_team",
    "official_away_team",
    "orientation_corrected",
    "status",
]

MISSING_SOURCE_COLUMNS = [
    "game_id",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "missing_source",
]

VALIDATION_COLUMNS = [
    "check",
    "count",
    "status",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        return [
            dict(row)
            for row in csv.DictReader(handle)
        ]


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
                    column: row.get(column, "")
                    for column in columns
                }
            )


def normalize_text(value: str) -> str:
    text = unicodedata.normalize(
        "NFKD",
        str(value).strip(),
    )

    text = "".join(
        char
        for char in text
        if not unicodedata.combining(char)
    )

    text = text.lower().replace(
        "&",
        " and ",
    )

    text = re.sub(
        r"[^a-z0-9]+",
        " ",
        text,
    )

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def load_team_map() -> dict:
    if not TEAM_MAP_PATH.exists():
        raise FileNotFoundError(
            f"Missing team map: {TEAM_MAP_PATH}"
        )

    by_source: dict[
        str,
        dict[str, dict[str, str]],
    ] = {}
    by_id: dict[str, dict[str, str]] = {}
    by_abbrev: dict[str, dict[str, str]] = {}

    for row_number, row in enumerate(
        read_csv(TEAM_MAP_PATH),
        start=2,
    ):
        if (
            str(row.get("league", ""))
            .strip()
            .lower()
            != "nhl"
        ):
            continue

        source = str(
            row.get("source", "")
        ).strip().lower()

        alias = str(
            row.get("alias", "")
        ).strip()

        canonical = str(
            row.get("canonical_team", "")
        ).strip()

        team_id = str(
            row.get("nhl_team_id", "")
        ).strip()

        abbrev = str(
            row.get("nhl_abbrev", "")
        ).strip().upper()

        if (
            not source
            or not alias
            or not canonical
        ):
            continue

        if source not in {
            "dratings",
            "sportsbook",
            "official_nhl",
            "shared",
        }:
            raise ValueError(
                f"{TEAM_MAP_PATH} row {row_number} "
                f"has unsupported source={source!r}"
            )

        if canonical != "TBD":
            if (
                not team_id
                or not team_id.isdigit()
            ):
                raise ValueError(
                    f"{TEAM_MAP_PATH} row {row_number} "
                    "has invalid nhl_team_id="
                    f"{team_id!r}"
                )

            if not re.fullmatch(
                r"[A-Z]{3}",
                abbrev,
            ):
                raise ValueError(
                    f"{TEAM_MAP_PATH} row {row_number} "
                    "has invalid nhl_abbrev="
                    f"{abbrev!r}"
                )

        identity = {
            "canonical_team": canonical,
            "nhl_team_id": team_id,
            "nhl_abbrev": abbrev,
        }

        if team_id:
            prior_id = by_id.get(team_id)

            if (
                prior_id is not None
                and prior_id != identity
            ):
                raise ValueError(
                    f"{TEAM_MAP_PATH} has conflicting "
                    "identity for nhl_team_id="
                    f"{team_id}: "
                    f"{prior_id} != {identity}"
                )

            by_id[team_id] = identity

        if abbrev:
            prior_abbrev = by_abbrev.get(abbrev)

            if (
                prior_abbrev is not None
                and prior_abbrev != identity
            ):
                raise ValueError(
                    f"{TEAM_MAP_PATH} has conflicting "
                    "identity for nhl_abbrev="
                    f"{abbrev}: "
                    f"{prior_abbrev} != {identity}"
                )

            by_abbrev[abbrev] = identity

        source_map = by_source.setdefault(
            source,
            {},
        )

        key = normalize_text(alias)
        prior_alias = source_map.get(key)

        if (
            prior_alias is not None
            and prior_alias != identity
        ):
            raise ValueError(
                f"{TEAM_MAP_PATH} has conflicting "
                f"mapping for source={source} "
                f"alias={alias!r}: "
                f"{prior_alias} != {identity}"
            )

        source_map[key] = identity

    for required_source in (
        "official_nhl",
        "dratings",
        "sportsbook",
    ):
        if not by_source.get(required_source):
            raise ValueError(
                "No NHL mappings loaded for "
                f"source={required_source} "
                f"from {TEAM_MAP_PATH}"
            )

    if len(by_id) != 32:
        raise ValueError(
            "Expected 32 stable NHL team IDs "
            f"in {TEAM_MAP_PATH}; "
            f"found {len(by_id)}"
        )

    return {
        "by_source": by_source,
        "by_id": by_id,
        "by_abbrev": by_abbrev,
    }


def resolve_team_identity(
    value: str,
    source: str,
    team_map: dict,
) -> dict[str, str] | None:
    raw = str(value).strip()

    if not raw:
        return None

    key = normalize_text(raw)

    for candidate_source in (
        source,
        "shared",
        "official_nhl",
    ):
        identity = (
            team_map["by_source"]
            .get(candidate_source, {})
            .get(key)
        )

        if identity is not None:
            return identity

    return None


def canonical_team(
    value: str,
    source: str,
    team_map: dict,
) -> str:
    raw = str(value).strip()

    identity = resolve_team_identity(
        raw,
        source,
        team_map,
    )

    if identity is None:
        return raw

    return identity["canonical_team"]


def team_key(
    value: str,
    source: str,
    team_map: dict,
) -> str:
    identity = resolve_team_identity(
        value,
        source,
        team_map,
    )

    if identity is None:
        return ""

    return identity["nhl_team_id"]


def matchup_key(
    game_date: str,
    home_team: str,
    away_team: str,
    source: str,
    team_map: dict,
) -> tuple[str, str, str] | None:
    home_id = team_key(
        home_team,
        source,
        team_map,
    )

    away_id = team_key(
        away_team,
        source,
        team_map,
    )

    if (
        not home_id
        or not away_id
    ):
        return None

    teams = sorted(
        [
            home_id,
            away_id,
        ]
    )

    return (
        str(game_date).strip(),
        teams[0],
        teams[1],
    )


def official_schedule_identity(
    source: dict[str, str],
    side: str,
    team_map: dict,
    path: Path,
) -> dict[str, str]:
    team_id = str(
        source.get(
            f"{side}_team_id",
            "",
        )
    ).strip()

    abbrev = str(
        source.get(
            f"{side}_team_abbrev",
            "",
        )
    ).strip().upper()

    name = str(
        source.get(
            f"{side}_team",
            "",
        )
    ).strip()

    if (
        not team_id
        or not abbrev
        or not name
    ):
        raise ValueError(
            f"Incomplete official {side} team "
            f"identity in {path}"
        )

    identity_by_id = team_map["by_id"].get(team_id)
    identity_by_abbrev = team_map["by_abbrev"].get(abbrev)
    identity_by_name = resolve_team_identity(
        name,
        "official_nhl",
        team_map,
    )

    if (
        identity_by_id is None
        or identity_by_abbrev is None
        or identity_by_name is None
    ):
        raise ValueError(
            f"Unmapped official {side} team "
            f"identity in {path}: "
            f"id={team_id!r} "
            f"abbrev={abbrev!r} "
            f"name={name!r}"
        )

    if not (
        identity_by_id
        == identity_by_abbrev
        == identity_by_name
    ):
        raise ValueError(
            f"Official {side} team identity "
            f"disagrees across id/abbrev/name "
            f"in {path}"
        )

    return identity_by_id

def swap_home_away_fields(
    row: dict[str, str],
) -> dict[str, str]:
    swapped = dict(row)

    processed: set[str] = set()

    for field in row:
        if (
            not field.startswith("home_")
            or field in processed
        ):
            continue

        suffix = field[len("home_"):]
        away_field = f"away_{suffix}"

        if away_field not in row:
            continue

        swapped[field] = row.get(
            away_field,
            "",
        )

        swapped[away_field] = row.get(
            field,
            "",
        )

        processed.add(field)
        processed.add(away_field)

    return swapped


def source_date_from_name(
    path: Path,
    prefix: str,
) -> str:
    match = re.fullmatch(
        rf"(?i){re.escape(prefix)}"
        r"(\d{4}_\d{2}_\d{2})",
        path.stem,
    )

    if not match:
        return ""

    return match.group(1)


def get_provider_id(
    row: dict[str, str],
    official_game_id: str,
) -> tuple[str, str]:
    explicit = str(
        row.get(
            "sportsbook_event_id",
            "",
        )
    ).strip()

    if explicit:
        if explicit == official_game_id:
            raise ValueError(
                "sportsbook_event_id equals "
                f"official NHL game_id={official_game_id}"
            )

        return (
            explicit,
            "explicit",
        )

    legacy = str(
        row.get(
            "game_id",
            "",
        )
    ).strip()

    if (
        legacy.isdigit()
        and legacy != official_game_id
        and not GAME_ID_RE.fullmatch(legacy)
    ):
        return (
            legacy,
            "recovered_numeric_legacy_game_id",
        )

    return (
        "",
        "missing",
    )


def build_games(
    team_map: dict[str, str],
) -> tuple[
    list[dict[str, str]],
    dict[
        tuple[str, str, str],
        dict[str, str],
    ],
]:
    rows: list[dict[str, str]] = []

    index: dict[
        tuple[str, str, str],
        dict[str, str],
    ] = {}

    seen_ids: set[str] = set()

    schedule_files = sorted(
        SCHEDULE_DIR.glob(
            "NHL_*.csv"
        )
    )

    if not schedule_files:
        raise FileNotFoundError(
            "No official schedule files found in "
            f"{SCHEDULE_DIR}"
        )

    for path in schedule_files:
        for source in read_csv(path):
            if (
                str(
                    source.get(
                        "season",
                        "",
                    )
                ).strip()
                != SEASON_CODE
            ):
                continue

            if (
                str(
                    source.get(
                        "game_type",
                        "",
                    )
                ).strip()
                not in {"2", "3"}
            ):
                continue

            game_id = str(
                source.get(
                    "game_id",
                    "",
                )
            ).strip()

            game_date = str(
                source.get(
                    "game_date",
                    "",
                )
            ).strip()

            home_identity = official_schedule_identity(
                source,
                "home",
                team_map,
                path,
            )

            away_identity = official_schedule_identity(
                source,
                "away",
                team_map,
                path,
            )

            home_team = home_identity[
                "canonical_team"
            ]

            away_team = away_identity[
                "canonical_team"
            ]

            home_team_id = home_identity[
                "nhl_team_id"
            ]

            away_team_id = away_identity[
                "nhl_team_id"
            ]

            if not GAME_ID_RE.fullmatch(
                game_id
            ):
                raise ValueError(
                    "Invalid official NHL "
                    f"game_id={game_id!r} "
                    f"in {path}"
                )

            if not DATE_RE.fullmatch(
                game_date
            ):
                raise ValueError(
                    "Invalid game_date="
                    f"{game_date!r} "
                    f"for game_id={game_id}"
                )

            if (
                not home_team
                or not away_team
            ):
                raise ValueError(
                    "Blank official team for "
                    f"game_id={game_id}"
                )

            if game_id in seen_ids:
                raise ValueError(
                    "Duplicate official NHL "
                    f"game_id={game_id}"
                )

            team_ids = sorted(
                [
                    home_team_id,
                    away_team_id,
                ]
            )

            key = (
                game_date,
                team_ids[0],
                team_ids[1],
            )

            if key in index:
                raise ValueError(
                    "Non-unique official "
                    f"matchup={key}"
                )

            row = {
                "game_id": game_id,
                "sport": "hockey",
                "league": "nhl",
                "game_date": game_date,
                "game_time": str(
                    source.get(
                        "game_time",
                        "",
                    )
                ).strip(),
                "home_team": home_team,
                "away_team": away_team,
                "sportsbook_event_id": "",
            }

            rows.append(row)
            index[key] = row
            seen_ids.add(game_id)

    if not rows:
        raise ValueError(
            "No season="
            f"{SEASON_CODE} games found "
            f"in {SCHEDULE_DIR}"
        )

    rows.sort(
        key=lambda row: (
            row["game_date"],
            row["game_time"],
            row["game_id"],
        )
    )

    return (
        rows,
        index,
    )


def build_audit_row(
    *,
    source_name: str,
    source_file: Path,
    source_row: int,
    source: dict[str, str],
    official: dict[str, str] | None,
    sportsbook_event_id: str,
    corrected: bool,
    status: str,
) -> dict[str, str]:
    return {
        "source": source_name,
        "source_file": str(source_file),
        "source_row": str(source_row),
        "source_game_id": str(
            source.get(
                "game_id",
                "",
            )
        ).strip(),
        "sportsbook_event_id": sportsbook_event_id,
        "game_date": str(
            source.get(
                "game_date",
                "",
            )
        ).strip(),
        "source_home_team": str(
            source.get(
                "home_team",
                "",
            )
        ).strip(),
        "source_away_team": str(
            source.get(
                "away_team",
                "",
            )
        ).strip(),
        "official_game_id": (
            official["game_id"]
            if official
            else ""
        ),
        "official_home_team": (
            official["home_team"]
            if official
            else ""
        ),
        "official_away_team": (
            official["away_team"]
            if official
            else ""
        ),
        "orientation_corrected": (
            "yes"
            if corrected
            else "no"
        ),
        "status": status,
    }


def reconcile_predictions(
    games_index: dict[
        tuple[str, str, str],
        dict[str, str],
    ],
    season_dates: set[str],
    team_map: dict[str, str],
) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
]:
    output: list[dict[str, str]] = []
    audit: list[dict[str, str]] = []
    unresolved: list[dict[str, str]] = []

    seen_game_ids: set[str] = set()

    files = sorted(
        PREDICTIONS_DIR.glob(
            "hockey_*.csv"
        )
    )

    files = [
        path
        for path in files
        if source_date_from_name(
            path,
            "hockey_",
        )
        in season_dates
    ]

    if not files:
        raise FileNotFoundError(
            "No season prediction files "
            f"found in {PREDICTIONS_DIR}"
        )

    for path in files:
        for row_number, source in enumerate(
            read_csv(path),
            start=2,
        ):
            game_date = str(
                source.get(
                    "game_date",
                    "",
                )
            ).strip()

            key = matchup_key(
                game_date,
                source.get(
                    "home_team",
                    "",
                ),
                source.get(
                    "away_team",
                    "",
                ),
                "dratings",
                team_map,
            )

            official = games_index.get(
                key
            )

            if official is None:
                row = build_audit_row(
                    source_name="prediction",
                    source_file=path,
                    source_row=row_number,
                    source=source,
                    official=None,
                    sportsbook_event_id="",
                    corrected=False,
                    status="unresolved",
                )

                audit.append(row)
                unresolved.append(row)
                continue

            source_home = team_key(
                source.get(
                    "home_team",
                    "",
                ),
                "dratings",
                team_map,
            )

            source_away = team_key(
                source.get(
                    "away_team",
                    "",
                ),
                "dratings",
                team_map,
            )

            official_home = team_key(
                official["home_team"],
                "official_nhl",
                team_map,
            )

            official_away = team_key(
                official["away_team"],
                "official_nhl",
                team_map,
            )

            normalized = dict(source)
            corrected = False
            status = "matched"

            if (
                source_home == official_home
                and source_away == official_away
            ):
                pass

            elif (
                source_home == official_away
                and source_away == official_home
            ):
                normalized = (
                    swap_home_away_fields(
                        source
                    )
                )

                corrected = True
                status = "matched_reversed"

            else:
                row = build_audit_row(
                    source_name="prediction",
                    source_file=path,
                    source_row=row_number,
                    source=source,
                    official=official,
                    sportsbook_event_id="",
                    corrected=False,
                    status="orientation_failure",
                )

                audit.append(row)
                unresolved.append(row)
                continue

            game_id = official["game_id"]

            if game_id in seen_game_ids:
                raise ValueError(
                    "Duplicate prediction "
                    "mapping to official "
                    f"game_id={game_id}"
                )

            seen_game_ids.add(
                game_id
            )

            output.append(
                {
                    "game_id": game_id,
                    "sport": official["sport"],
                    "league": official["league"],
                    "game_date": official["game_date"],
                    "game_time": official["game_time"],
                    "home_team": official["home_team"],
                    "away_team": official["away_team"],
                    "home_prob_moneyline": str(
                        normalized.get(
                            "home_prob_moneyline",
                            "",
                        )
                    ).strip(),
                    "away_prob_moneyline": str(
                        normalized.get(
                            "away_prob_moneyline",
                            "",
                        )
                    ).strip(),
                    "away_projected_goals": str(
                        normalized.get(
                            "away_projected_goals",
                            "",
                        )
                    ).strip(),
                    "home_projected_goals": str(
                        normalized.get(
                            "home_projected_goals",
                            "",
                        )
                    ).strip(),
                    "total_projected_goals": str(
                        normalized.get(
                            "total_projected_goals",
                            "",
                        )
                    ).strip(),
                }
            )

            audit.append(
                build_audit_row(
                    source_name="prediction",
                    source_file=path,
                    source_row=row_number,
                    source=source,
                    official=official,
                    sportsbook_event_id="",
                    corrected=corrected,
                    status=status,
                )
            )

    output.sort(
        key=lambda row: (
            row["game_date"],
            row["game_time"],
            row["game_id"],
        )
    )

    return (
        output,
        audit,
        unresolved,
    )


def reconcile_sportsbook(
    games_index: dict[
        tuple[str, str, str],
        dict[str, str],
    ],
    season_dates: set[str],
    team_map: dict[str, str],
) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    dict[str, str],
]:
    output: list[dict[str, str]] = []
    audit: list[dict[str, str]] = []
    unresolved: list[dict[str, str]] = []

    provider_by_game: dict[str, str] = {}

    seen_game_ids: set[str] = set()
    seen_provider_ids: set[str] = set()

    files = sorted(
        SPORTSBOOK_DIR.glob(
            "*.csv"
        )
    )

    files = [
        path
        for path in files
        if source_date_from_name(
            path,
            "nhl_",
        )
        in season_dates
    ]

    if not files:
        raise FileNotFoundError(
            "No season sportsbook files "
            f"found in {SPORTSBOOK_DIR}"
        )

    for path in files:
        for row_number, source in enumerate(
            read_csv(path),
            start=2,
        ):
            game_date = str(
                source.get(
                    "game_date",
                    "",
                )
            ).strip()

            key = matchup_key(
                game_date,
                source.get(
                    "home_team",
                    "",
                ),
                source.get(
                    "away_team",
                    "",
                ),
                "sportsbook",
                team_map,
            )

            official = games_index.get(
                key
            )

            if official is None:
                row = build_audit_row(
                    source_name="sportsbook",
                    source_file=path,
                    source_row=row_number,
                    source=source,
                    official=None,
                    sportsbook_event_id=str(
                        source.get(
                            "sportsbook_event_id",
                            "",
                        )
                    ).strip(),
                    corrected=False,
                    status="unresolved",
                )

                audit.append(row)
                unresolved.append(row)
                continue

            source_home = team_key(
                source.get(
                    "home_team",
                    "",
                ),
                "sportsbook",
                team_map,
            )

            source_away = team_key(
                source.get(
                    "away_team",
                    "",
                ),
                "sportsbook",
                team_map,
            )

            official_home = team_key(
                official["home_team"],
                "official_nhl",
                team_map,
            )

            official_away = team_key(
                official["away_team"],
                "official_nhl",
                team_map,
            )

            normalized = dict(source)
            corrected = False
            status = "matched"

            if (
                source_home == official_home
                and source_away == official_away
            ):
                pass

            elif (
                source_home == official_away
                and source_away == official_home
            ):
                normalized = (
                    swap_home_away_fields(
                        source
                    )
                )

                corrected = True
                status = "matched_reversed"

            else:
                row = build_audit_row(
                    source_name="sportsbook",
                    source_file=path,
                    source_row=row_number,
                    source=source,
                    official=official,
                    sportsbook_event_id="",
                    corrected=False,
                    status="orientation_failure",
                )

                audit.append(row)
                unresolved.append(row)
                continue

            game_id = official["game_id"]

            if game_id in seen_game_ids:
                raise ValueError(
                    "Duplicate sportsbook "
                    "mapping to official "
                    f"game_id={game_id}"
                )

            seen_game_ids.add(
                game_id
            )

            (
                provider_id,
                provider_status,
            ) = get_provider_id(
                source,
                game_id,
            )

            if provider_id:
                if (
                    provider_id
                    in seen_provider_ids
                ):
                    raise ValueError(
                        "Duplicate "
                        "sportsbook_event_id="
                        f"{provider_id}"
                    )

                seen_provider_ids.add(
                    provider_id
                )

                provider_by_game[
                    game_id
                ] = provider_id

            row = {
                "game_id": game_id,
                "sportsbook_event_id": provider_id,
                "sport": official["sport"],
                "league": official["league"],
                "game_date": official["game_date"],
                "game_time": official["game_time"],
                "home_team": official["home_team"],
                "away_team": official["away_team"],
            }

            for column in SPORTSBOOK_COLUMNS[8:]:
                row[column] = str(
                    normalized.get(
                        column,
                        "",
                    )
                ).strip()

            output.append(row)

            audit.append(
                build_audit_row(
                    source_name="sportsbook",
                    source_file=path,
                    source_row=row_number,
                    source=source,
                    official=official,
                    sportsbook_event_id=provider_id,
                    corrected=corrected,
                    status=(
                        f"{status};"
                        f"provider_id={provider_status}"
                    ),
                )
            )

    output.sort(
        key=lambda row: (
            row["game_date"],
            row["game_time"],
            row["game_id"],
        )
    )

    return (
        output,
        audit,
        unresolved,
        provider_by_game,
    )


def build_missing_source_audit(
    games: list[dict[str, str]],
    predictions: list[dict[str, str]],
    sportsbook: list[dict[str, str]],
) -> list[dict[str, str]]:
    prediction_ids = {
        row["game_id"]
        for row in predictions
    }

    sportsbook_ids = {
        row["game_id"]
        for row in sportsbook
    }

    rows: list[dict[str, str]] = []

    for game in games:
        missing: list[str] = []

        if (
            game["game_id"]
            not in prediction_ids
        ):
            missing.append(
                "prediction"
            )

        if (
            game["game_id"]
            not in sportsbook_ids
        ):
            missing.append(
                "sportsbook"
            )

        if not missing:
            continue

        rows.append(
            {
                "game_id": game["game_id"],
                "game_date": game["game_date"],
                "game_time": game["game_time"],
                "home_team": game["home_team"],
                "away_team": game["away_team"],
                "missing_source": ";".join(
                    missing
                ),
            }
        )

    return rows


def validate(
    games: list[dict[str, str]],
    predictions: list[dict[str, str]],
    sportsbook: list[dict[str, str]],
) -> list[dict[str, str]]:
    game_by_id = {
        row["game_id"]: row
        for row in games
    }

    provider_ids = {
        row["sportsbook_event_id"]
        for row in sportsbook
        if row.get(
            "sportsbook_event_id"
        )
    }

    def duplicate_count(
        rows: list[dict[str, str]],
        field: str,
        ignore_blank: bool = False,
    ) -> int:
        values = [
            str(
                row.get(
                    field,
                    "",
                )
            ).strip()
            for row in rows
        ]

        if ignore_blank:
            values = [
                value
                for value in values
                if value
            ]

        return sum(
            1
            for count
            in Counter(values).values()
            if count > 1
        )

    checks: list[
        tuple[str, int]
    ] = [
        (
            "games_blank_game_id",
            sum(
                not str(
                    row.get(
                        "game_id",
                        "",
                    )
                ).strip()
                for row in games
            ),
        ),
        (
            "predictions_blank_game_id",
            sum(
                not str(
                    row.get(
                        "game_id",
                        "",
                    )
                ).strip()
                for row in predictions
            ),
        ),
        (
            "sportsbook_blank_game_id",
            sum(
                not str(
                    row.get(
                        "game_id",
                        "",
                    )
                ).strip()
                for row in sportsbook
            ),
        ),
        (
            "games_invalid_game_id",
            sum(
                not GAME_ID_RE.fullmatch(
                    str(
                        row.get(
                            "game_id",
                            "",
                        )
                    ).strip()
                )
                for row in games
            ),
        ),
        (
            "predictions_invalid_game_id",
            sum(
                not GAME_ID_RE.fullmatch(
                    str(
                        row.get(
                            "game_id",
                            "",
                        )
                    ).strip()
                )
                for row in predictions
            ),
        ),
        (
            "sportsbook_invalid_game_id",
            sum(
                not GAME_ID_RE.fullmatch(
                    str(
                        row.get(
                            "game_id",
                            "",
                        )
                    ).strip()
                )
                for row in sportsbook
            ),
        ),
        (
            "games_duplicate_game_id",
            duplicate_count(
                games,
                "game_id",
            ),
        ),
        (
            "predictions_duplicate_game_id",
            duplicate_count(
                predictions,
                "game_id",
            ),
        ),
        (
            "sportsbook_duplicate_game_id",
            duplicate_count(
                sportsbook,
                "game_id",
            ),
        ),
        (
            "prediction_game_id_missing_from_games",
            sum(
                row["game_id"]
                not in game_by_id
                for row in predictions
            ),
        ),
        (
            "sportsbook_game_id_missing_from_games",
            sum(
                row["game_id"]
                not in game_by_id
                for row in sportsbook
            ),
        ),
        (
            "sportsbook_event_id_used_as_game_id",
            sum(
                row.get(
                    "game_id",
                    "",
                )
                in provider_ids
                for row in (
                    games
                    + predictions
                    + sportsbook
                )
            ),
        ),
        (
            "prediction_team_mismatch",
            sum(
                row["game_id"]
                in game_by_id
                and (
                    row["home_team"]
                    != game_by_id[
                        row["game_id"]
                    ]["home_team"]
                    or
                    row["away_team"]
                    != game_by_id[
                        row["game_id"]
                    ]["away_team"]
                )
                for row in predictions
            ),
        ),
        (
            "sportsbook_team_mismatch",
            sum(
                row["game_id"]
                in game_by_id
                and (
                    row["home_team"]
                    != game_by_id[
                        row["game_id"]
                    ]["home_team"]
                    or
                    row["away_team"]
                    != game_by_id[
                        row["game_id"]
                    ]["away_team"]
                )
                for row in sportsbook
            ),
        ),
        (
            "prediction_game_date_mismatch",
            sum(
                row["game_id"]
                in game_by_id
                and row["game_date"]
                != game_by_id[
                    row["game_id"]
                ]["game_date"]
                for row in predictions
            ),
        ),
        (
            "sportsbook_game_date_mismatch",
            sum(
                row["game_id"]
                in game_by_id
                and row["game_date"]
                != game_by_id[
                    row["game_id"]
                ]["game_date"]
                for row in sportsbook
            ),
        ),
        (
            "sportsbook_duplicate_sportsbook_event_id",
            duplicate_count(
                sportsbook,
                "sportsbook_event_id",
                ignore_blank=True,
            ),
        ),
    ]

    return [
        {
            "check": name,
            "count": str(count),
            "status": (
                "PASS"
                if count == 0
                else "FAIL"
            ),
        }
        for name, count in checks
    ]


def main() -> int:
    SEASON_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    AUDIT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    team_map = load_team_map()

    print(
        "stable_nhl_team_ids="
        f"{len(team_map['by_id'])}"
    )

    (
        games,
        games_index,
    ) = build_games(
        team_map
    )

    season_dates = {
        row["game_date"]
        for row in games
    }

    (
        predictions,
        prediction_audit,
        prediction_unresolved,
    ) = reconcile_predictions(
        games_index,
        season_dates,
        team_map,
    )

    (
        sportsbook,
        sportsbook_audit,
        sportsbook_unresolved,
        provider_by_game,
    ) = reconcile_sportsbook(
        games_index,
        season_dates,
        team_map,
    )

    for game in games:
        game[
            "sportsbook_event_id"
        ] = provider_by_game.get(
            game["game_id"],
            "",
        )

    write_csv(
        AUDIT_DIR
        / "predictions_reconciliation.csv",
        RECON_AUDIT_COLUMNS,
        prediction_audit,
    )

    write_csv(
        AUDIT_DIR
        / "sportsbook_reconciliation.csv",
        RECON_AUDIT_COLUMNS,
        sportsbook_audit,
    )

    write_csv(
        AUDIT_DIR
        / "unresolved_predictions.csv",
        RECON_AUDIT_COLUMNS,
        prediction_unresolved,
    )

    write_csv(
        AUDIT_DIR
        / "unresolved_sportsbook.csv",
        RECON_AUDIT_COLUMNS,
        sportsbook_unresolved,
    )

    missing_source_games = (
        build_missing_source_audit(
            games,
            predictions,
            sportsbook,
        )
    )

    write_csv(
        AUDIT_DIR
        / "missing_source_games.csv",
        MISSING_SOURCE_COLUMNS,
        missing_source_games,
    )

    if (
        prediction_unresolved
        or sportsbook_unresolved
    ):
        raise ValueError(
            "Season-master reconciliation "
            "failed: "
            "unresolved_predictions="
            f"{len(prediction_unresolved)} "
            "unresolved_sportsbook="
            f"{len(sportsbook_unresolved)}. "
            f"See {AUDIT_DIR}."
        )

    validation_rows = validate(
        games,
        predictions,
        sportsbook,
    )

    write_csv(
        AUDIT_DIR
        / "validation.csv",
        VALIDATION_COLUMNS,
        validation_rows,
    )

    failed = [
        row
        for row in validation_rows
        if row["status"] == "FAIL"
    ]

    if failed:
        details = ", ".join(
            f"{row['check']}="
            f"{row['count']}"
            for row in failed
        )

        raise ValueError(
            "Season-master validation "
            f"failed: {details}. "
            "See "
            f"{AUDIT_DIR / 'validation.csv'}"
        )

    write_csv(
        GAMES_PATH,
        GAME_COLUMNS,
        games,
    )

    write_csv(
        PREDICTIONS_PATH,
        PREDICTION_COLUMNS,
        predictions,
    )

    write_csv(
        SPORTSBOOK_PATH,
        SPORTSBOOK_COLUMNS,
        sportsbook,
    )

    missing_provider = sum(
        not row.get(
            "sportsbook_event_id"
        )
        for row in sportsbook
    )

    reversed_predictions = sum(
        row[
            "orientation_corrected"
        ] == "yes"
        for row in prediction_audit
    )

    reversed_sportsbook = sum(
        row[
            "orientation_corrected"
        ] == "yes"
        for row in sportsbook_audit
    )

    print(
        "SEASON MASTER BUILD PASSED"
    )

    print(
        f"games_rows={len(games)}"
    )

    print(
        "prediction_rows="
        f"{len(predictions)}"
    )

    print(
        "sportsbook_rows="
        f"{len(sportsbook)}"
    )

    print(
        "reversed_predictions_fixed="
        f"{reversed_predictions}"
    )

    print(
        "reversed_sportsbook_fixed="
        f"{reversed_sportsbook}"
    )

    print(
        "missing_sportsbook_event_id="
        f"{missing_provider}"
    )

    print(
        "games_missing_prediction_or_"
        "sportsbook_source="
        f"{len(missing_source_games)}"
    )

    print(
        f"audit_dir={AUDIT_DIR}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(
            main()
        )
    except Exception as exc:
        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1)
