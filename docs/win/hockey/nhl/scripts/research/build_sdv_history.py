#!/usr/bin/env python3
"""
Build the local SportsDataverse NHL historical research store.

The generated Parquet data is intentionally local-only and Git-ignored:

    docs/win/hockey/nhl/research/sdv_history/

Examples:

    python docs/win/hockey/nhl/scripts/research/build_sdv_history.py --season 2024

    python docs/win/hockey/nhl/scripts/research/build_sdv_history.py \
        --start-season 2019 --end-season 2024

    python docs/win/hockey/nhl/scripts/research/build_sdv_history.py \
        --season 2024 --datasets play_by_play,schedules,team_boxscores

Season convention
-----------------
The command-line season is the NHL/project **start year**. For example,
``--season 2024`` means the 2024-25 NHL season and official game IDs begin
with ``2024``. SportsDataverse labels NHL seasons by the **ending year**, so
the builder translates that request to SDV season ``2025`` internally.

Leakage policy
--------------
The Parquet tables produced here are historical source facts, not target-game
pregame feature rows. Many tables contain information that is only known after
a game finishes. They must never be joined directly to the same target game as
pregame features.

For postgame/game-derived history, the conservative production-safe rule is:

    source_game_date < target_game_date

This deliberately excludes all target-date games from a target game's feature
history. Timestamped observations, when used by downstream feature builders,
must additionally satisfy:

    observed_at_utc < pregame_cutoff_utc

The per-game cutoff table written under ``pregame_index/`` uses the repository's
official NHL ``start_time_utc`` when available. If it is unavailable, the
cutoff falls back to the start of the target game date in America/New_York,
which is conservative and leakage-safe.

Historical SportsDataverse schedule score fields are preserved as source data,
but are not validated grading truth. In particular, schedule scores through
2023 must not be used as grading ground truth without independent validation;
use play-by-play GOAL events or another validated official NHL score endpoint.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import sportsdataverse.nhl as nhl


PINNED_SDV_VERSION = "0.0.75"

REPO_ROOT = Path(__file__).resolve().parents[6]
BASE_DIR = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"
OUTPUT_DIR = BASE_DIR / "research" / "sdv_history"
OFFICIAL_SCHEDULE_DIR = BASE_DIR / "00_intake" / "nhl_schedule"
GITIGNORE_PATH = REPO_ROOT / ".gitignore"
GITIGNORE_ENTRY = "docs/win/hockey/nhl/research/sdv_history/"

NY = ZoneInfo("America/New_York")
UTC = timezone.utc


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    loader_name: str
    min_season: int
    game_keyed: bool


DATASET_SPECS: tuple[DatasetSpec, ...] = (
    DatasetSpec("schedules", "load_nhl_schedule", 2010, True),
    DatasetSpec("play_by_play", "load_nhl_pbp", 2010, True),
    DatasetSpec("team_boxscores", "load_nhl_team_boxscores", 2010, True),
    DatasetSpec("player_boxscores", "load_nhl_player_boxscores", 2010, True),
    DatasetSpec("rosters", "load_nhl_rosters", 2010, False),
    DatasetSpec("game_info", "load_nhl_game_info", 2024, True),
    DatasetSpec("game_rosters", "load_nhl_game_rosters", 2024, True),
    DatasetSpec("goalie_boxscores", "load_nhl_goalie_boxscores", 2024, True),
    DatasetSpec("skater_boxscores", "load_nhl_skater_boxscores", 2024, True),
    DatasetSpec("linescores", "load_nhl_linescore", 2024, True),
    DatasetSpec("penalties", "load_nhl_penalties", 2024, True),
    DatasetSpec("scoring", "load_nhl_scoring", 2024, True),
    DatasetSpec("scratches", "load_nhl_scratches", 2024, True),
    DatasetSpec("shifts", "load_nhl_shifts", 2025, True),
    DatasetSpec("shots_by_period", "load_nhl_shots_by_period", 2025, True),
)

SPEC_BY_NAME = {spec.name: spec for spec in DATASET_SPECS}

PBP_REQUIRED_RESEARCH_COLUMNS = {
    "game_id",
    "game_date",
    "event_type",
    "event_team_type",
    "xg",
    "x",
    "y",
    "x_fixed",
    "y_fixed",
    "shot_distance",
    "shot_angle",
    "strength_state",
    "strength_code",
    "strength",
    "home_skaters",
    "away_skaters",
    "event_goalie_id",
    "home_goalie_id",
    "away_goalie_id",
    "home_on_1_id",
    "home_on_2_id",
    "home_on_3_id",
    "home_on_4_id",
    "home_on_5_id",
    "home_on_6_id",
    "home_on_7_id",
    "away_on_1_id",
    "away_on_2_id",
    "away_on_3_id",
    "away_on_4_id",
    "away_on_5_id",
    "away_on_6_id",
    "away_on_7_id",
}

LEAKAGE_POLICY = {
    "status": "REQUIRED_ENFORCED",
    "game_id": "Official 10-digit NHL game ID only; provider IDs remain separate.",
    "raw_table_role": (
        "Historical source facts. These tables may contain postgame information "
        "and are not target-game pregame feature rows."
    ),
    "historical_postgame_source_rule": "source_game_date < target_game_date",
    "same_day_postgame_sources": "excluded",
    "timestamped_observation_rule": "observed_at_utc < pregame_cutoff_utc",
    "pregame_cutoff_preference": "official NHL start_time_utc",
    "pregame_cutoff_fallback": "conservative target-date start in America/New_York",
    "target_game_postgame_data": "never eligible for that target game's pregame features",
    "schedule_score_guardrail": (
        "SportsDataverse schedule score fields through 2023 are not grading ground "
        "truth without independent validation. Use play-by-play GOAL events or "
        "another validated official NHL score endpoint."
    ),
    "lineup_goalie_guardrail": (
        "Final-game participation, scratches, or later confirmations must not be "
        "backfilled as target-game pregame evidence; existing T-60 production rules "
        "remain authoritative."
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build local, Git-ignored SportsDataverse NHL historical Parquet data."
        )
    )
    parser.add_argument(
        "--season",
        type=int,
        action="append",
        help=(
            "NHL season start year, e.g. --season 2024 means 2024-25. "
            "SportsDataverse labels NHL seasons by ending year, so the builder "
            "internally requests SDV season 2025."
        ),
    )
    parser.add_argument("--start-season", type=int)
    parser.add_argument("--end-season", type=int)
    parser.add_argument(
        "--datasets",
        default="all",
        help=(
            "Comma-separated dataset names or 'all'. Schedules are always loaded "
            "because they provide canonical game dates and the leakage-safe game index."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing season Parquet files instead of skipping them.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Return a non-zero exit code when an eligible requested loader is empty "
            "or unpublished."
        ),
    )
    return parser.parse_args()


def requested_seasons(args: argparse.Namespace) -> list[int]:
    seasons: list[int] = []

    if args.season:
        seasons.extend(args.season)

    if args.start_season is not None or args.end_season is not None:
        if args.start_season is None or args.end_season is None:
            raise SystemExit("--start-season and --end-season must be used together")
        if args.end_season < args.start_season:
            raise SystemExit("--end-season cannot be less than --start-season")
        seasons.extend(range(args.start_season, args.end_season + 1))

    seasons = sorted(set(seasons))
    if not seasons:
        raise SystemExit(
            "Specify --season or --start-season/--end-season; the builder will not "
            "materialize multiple seasons implicitly."
        )

    bad = [season for season in seasons if season < 2009]
    if bad:
        raise SystemExit(
            "SportsDataverse core NHL historical loaders begin with SDV season 2010 "
            f"(the 2009-10 NHL season); invalid NHL start year(s): {bad}"
        )

    return seasons


def requested_datasets(args: argparse.Namespace) -> set[str]:
    if str(args.datasets).strip().lower() == "all":
        return set(SPEC_BY_NAME)

    names = {
        value.strip()
        for value in str(args.datasets).split(",")
        if value.strip()
    }
    unknown = sorted(names - set(SPEC_BY_NAME))
    if unknown:
        raise SystemExit(f"Unknown dataset names: {unknown}")

    names.add("schedules")
    return names


def installed_sdv_version() -> str:
    try:
        return version("sportsdataverse")
    except PackageNotFoundError as exc:
        raise SystemExit(
            "sportsdataverse is not installed; install the repository requirements first"
        ) from exc


def enforce_sdv_pin() -> str:
    installed = installed_sdv_version()
    if installed != PINNED_SDV_VERSION:
        raise SystemExit(
            "Refusing to build with SportsDataverse version "
            f"{installed!r}; repository pin is {PINNED_SDV_VERSION!r}."
        )
    return installed


def enforce_gitignored_output() -> None:
    if not GITIGNORE_PATH.exists():
        raise SystemExit(f"Missing repository .gitignore: {GITIGNORE_PATH}")

    lines = {
        line.strip()
        for line in GITIGNORE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    if GITIGNORE_ENTRY not in lines:
        raise SystemExit(
            "Refusing to materialize historical data until this exact .gitignore "
            f"entry exists: {GITIGNORE_ENTRY}"
        )


def canonical_game_id_expr(column: str = "game_id") -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Int64, strict=False)
        .cast(pl.String)
        .alias(column)
    )


def canonicalize_game_id(df: pl.DataFrame, *, season: int, dataset: str) -> pl.DataFrame:
    if "game_id" not in df.columns:
        raise RuntimeError(f"{dataset}: expected game_id column is missing")

    original_non_null = int(
        df.select(pl.col("game_id").is_not_null().sum()).item()
    )
    out = df.with_columns(canonical_game_id_expr())
    canonical_non_null = int(
        out.select(pl.col("game_id").is_not_null().sum()).item()
    )

    if canonical_non_null != original_non_null:
        raise RuntimeError(
            f"{dataset}: one or more non-null game_id values could not be converted "
            "to official integer NHL IDs"
        )

    invalid_count = int(
        out.select(
            (~pl.col("game_id").str.contains(r"^\d{10}$"))
            .fill_null(False)
            .sum()
        ).item()
    )
    if invalid_count:
        raise RuntimeError(
            f"{dataset}: found {invalid_count} non-canonical game_id values; "
            "official NHL game_id must be exactly 10 digits"
        )

    wrong_season_count = int(
        out.select(
            (
                pl.col("game_id").is_not_null()
                & (pl.col("game_id").str.slice(0, 4) != str(season))
            ).sum()
        ).item()
    )
    if wrong_season_count:
        raise RuntimeError(
            f"{dataset}: found {wrong_season_count} game_id values whose first four "
            f"digits do not match requested season {season}"
        )

    return out


def normalized_date_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.slice(0, 10)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .dt.strftime("%Y-%m-%d")
    )


def prepare_schedule(schedule: pl.DataFrame, season: int) -> pl.DataFrame:
    schedule = canonicalize_game_id(
        schedule,
        season=season,
        dataset="schedules",
    )
    if "game_date" not in schedule.columns:
        raise RuntimeError("schedules: required game_date column is missing")

    schedule = schedule.with_columns(
        normalized_date_expr("game_date").alias("source_game_date")
    )

    missing_dates = int(
        schedule.select(pl.col("source_game_date").is_null().sum()).item()
    )
    if missing_dates:
        raise RuntimeError(
            f"schedules: {missing_dates} rows have no parseable game_date; "
            "cannot build leakage-safe historical cutoffs"
        )

    duplicate_games = int(
        schedule.group_by("game_id").len().filter(pl.col("len") > 1).height
    )
    if duplicate_games:
        raise RuntimeError(
            f"schedules: found {duplicate_games} duplicate official game_id values"
        )

    return schedule.with_columns(
        pl.lit(season).alias("nhl_season_start_year"),
        pl.lit(sdv_season_year(season)).alias("sdv_season_year"),
        pl.lit(True).alias("pregame_source_eligible"),
    )


def schedule_date_lookup(schedule: pl.DataFrame) -> pl.DataFrame:
    return schedule.select(
        "game_id",
        pl.col("source_game_date").alias("_schedule_source_game_date"),
    )


def attach_source_dates(
    df: pl.DataFrame,
    *,
    spec: DatasetSpec,
    season: int,
    schedule_dates: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    if spec.game_keyed:
        out = canonicalize_game_id(df, season=season, dataset=spec.name)
        out = out.join(schedule_dates, on="game_id", how="left")

        if "game_date" in out.columns:
            own_date = normalized_date_expr("game_date")
            out = out.with_columns(
                pl.coalesce(
                    pl.col("_schedule_source_game_date"),
                    own_date,
                ).alias("source_game_date")
            )
        else:
            out = out.with_columns(
                pl.col("_schedule_source_game_date").alias("source_game_date")
            )

        out = out.drop("_schedule_source_game_date")
        out = out.with_columns(
            pl.col("source_game_date")
            .is_not_null()
            .alias("pregame_source_eligible")
        )
        ineligible = int(
            out.select((~pl.col("pregame_source_eligible")).sum()).item()
        )
        return out, ineligible

    out = df
    if "game_date" in out.columns:
        out = out.with_columns(
            normalized_date_expr("game_date").alias("source_observed_date")
        ).with_columns(
            pl.col("source_observed_date")
            .is_not_null()
            .alias("pregame_source_eligible")
        )
        ineligible = int(
            out.select((~pl.col("pregame_source_eligible")).sum()).item()
        )
    else:
        out = out.with_columns(pl.lit(False).alias("pregame_source_eligible"))
        ineligible = out.height

    return out, ineligible


def validate_pbp_research_columns(df: pl.DataFrame) -> None:
    missing = sorted(PBP_REQUIRED_RESEARCH_COLUMNS - set(df.columns))
    if missing:
        raise RuntimeError(
            "play_by_play is missing required SDV-P5 research columns: "
            + ", ".join(missing)
        )


def parse_utc_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def read_official_start_times() -> dict[str, datetime]:
    lookup: dict[str, datetime] = {}
    if not OFFICIAL_SCHEDULE_DIR.exists():
        return lookup

    for path in sorted(OFFICIAL_SCHEDULE_DIR.glob("*.csv")):
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    raw_game_id = str(row.get("game_id", "")).strip()
                    raw_start = row.get("start_time_utc", "")
                    if not raw_game_id.isdigit() or len(raw_game_id) != 10:
                        continue
                    start = parse_utc_timestamp(raw_start)
                    if start is not None:
                        lookup[raw_game_id] = start
        except (OSError, csv.Error):
            continue

    return lookup


def utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build_pregame_index(
    schedule: pl.DataFrame,
    official_start_times: dict[str, datetime],
) -> pl.DataFrame:
    desired_columns = [
        col
        for col in (
            "game_id",
            "season",
            "nhl_season_start_year",
            "sdv_season_year",
            "game_type",
            "source_game_date",
            "home_team_abbr",
            "away_team_abbr",
        )
        if col in schedule.columns
    ]
    base = schedule.select(desired_columns)

    rows: list[dict[str, Any]] = []
    for row in base.iter_rows(named=True):
        game_id = str(row["game_id"])
        game_date_text = str(row["source_game_date"])
        game_day = date.fromisoformat(game_date_text)

        cutoff = official_start_times.get(game_id)
        if cutoff is not None:
            cutoff_source = "official_nhl_schedule:start_time_utc"
        else:
            cutoff = datetime.combine(game_day, dt_time.min, tzinfo=NY).astimezone(UTC)
            cutoff_source = "conservative_game_date_start_et"

        out_row = dict(row)
        out_row["game_date"] = out_row.pop("source_game_date")
        out_row["pregame_cutoff_utc"] = utc_iso(cutoff)
        out_row["pregame_cutoff_source"] = cutoff_source
        out_row["historical_postgame_source_rule"] = (
            "source_game_date < target_game_date"
        )
        out_row["timestamped_observation_rule"] = (
            "observed_at_utc < pregame_cutoff_utc"
        )
        rows.append(out_row)

    if not rows:
        return pl.DataFrame()

    return pl.DataFrame(rows).sort(["game_date", "game_id"])


def dataset_path(spec: DatasetSpec, season: int) -> Path:
    return OUTPUT_DIR / spec.name / f"season_{season}.parquet"


def pregame_index_path(season: int) -> Path:
    return OUTPUT_DIR / "pregame_index" / f"season_{season}.parquet"


def repo_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    df.write_parquet(tmp, compression="zstd", statistics=True)
    tmp.replace(path)


def write_json_atomic(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_text(
        json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def sdv_season_year(nhl_season_start: int) -> int:
    """Translate project/NHL start-year semantics to SDV ending-year semantics."""
    return nhl_season_start + 1


def load_sdv_frame(spec: DatasetSpec, season: int) -> pl.DataFrame:
    loader = getattr(nhl, spec.loader_name, None)
    if loader is None:
        raise RuntimeError(
            f"SportsDataverse {PINNED_SDV_VERSION} has no {spec.loader_name}"
        )

    frame = loader(seasons=sdv_season_year(season))
    if not isinstance(frame, pl.DataFrame):
        raise TypeError(
            f"{spec.loader_name} returned {type(frame).__name__}; expected polars.DataFrame"
        )
    return frame


def result_entry(
    *,
    spec: DatasetSpec,
    season: int,
    status: str,
    path: Path | None = None,
    rows: int | None = None,
    columns: int | None = None,
    ineligible_rows: int | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "dataset": spec.name,
        "loader": spec.loader_name,
        "season": season,
        "nhl_season_start_year": season,
        "sdv_season_year": sdv_season_year(season),
        "minimum_supported_nhl_start_season": spec.min_season - 1,
        "minimum_supported_sdv_season": spec.min_season,
        "status": status,
    }
    if path is not None:
        entry["path"] = repo_relative(path)
    if rows is not None:
        entry["rows"] = rows
    if columns is not None:
        entry["columns"] = columns
    if ineligible_rows is not None:
        entry["pregame_source_ineligible_rows"] = ineligible_rows
    if message:
        entry["message"] = message
    return entry


def materialize_schedule(
    *,
    season: int,
    overwrite: bool,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    spec = SPEC_BY_NAME["schedules"]
    path = dataset_path(spec, season)

    if path.exists() and not overwrite:
        frame = pl.read_parquet(path)
        frame = prepare_schedule(frame, season)
        return frame, result_entry(
            spec=spec,
            season=season,
            status="existing_validated",
            path=path,
            rows=frame.height,
            columns=frame.width,
            ineligible_rows=0,
        )

    frame = load_sdv_frame(spec, season)
    if frame.is_empty():
        raise RuntimeError(
            "schedules: SportsDataverse returned no schedule rows for NHL season "
            f"{season}-{str(season + 1)[-2:]} (SDV season {sdv_season_year(season)})"
        )

    frame = prepare_schedule(frame, season)
    write_parquet_atomic(frame, path)
    return frame, result_entry(
        spec=spec,
        season=season,
        status="written",
        path=path,
        rows=frame.height,
        columns=frame.width,
        ineligible_rows=0,
    )


def materialize_dataset(
    *,
    spec: DatasetSpec,
    season: int,
    schedule_dates: pl.DataFrame,
    overwrite: bool,
) -> dict[str, Any]:
    path = dataset_path(spec, season)

    if sdv_season_year(season) < spec.min_season:
        return result_entry(
            spec=spec,
            season=season,
            status="unavailable_before_minimum_season",
        )

    if path.exists() and not overwrite:
        return result_entry(
            spec=spec,
            season=season,
            status="skipped_existing",
            path=path,
        )

    frame = load_sdv_frame(spec, season)
    if frame.is_empty():
        return result_entry(
            spec=spec,
            season=season,
            status="empty_or_unpublished",
        )

    if spec.name == "play_by_play":
        validate_pbp_research_columns(frame)

    frame, ineligible_rows = attach_source_dates(
        frame,
        spec=spec,
        season=season,
        schedule_dates=schedule_dates,
    )
    write_parquet_atomic(frame, path)

    return result_entry(
        spec=spec,
        season=season,
        status="written",
        path=path,
        rows=frame.height,
        columns=frame.width,
        ineligible_rows=ineligible_rows,
    )


def main() -> int:
    args = parse_args()
    seasons = requested_seasons(args)
    selected = requested_datasets(args)

    sdv_version = enforce_sdv_pin()
    enforce_gitignored_output()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    write_json_atomic(LEAKAGE_POLICY, OUTPUT_DIR / "leakage_policy.json")
    official_start_times = read_official_start_times()

    run_started = datetime.now(UTC)
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    strict_failures: list[str] = []

    for season in seasons:
        print(f"SEASON {season}")
        try:
            schedule, schedule_result = materialize_schedule(
                season=season,
                overwrite=args.overwrite,
            )
            results.append(schedule_result)
            print(f"  schedules: {schedule_result['status']}")
        except Exception as exc:
            message = f"season {season} schedules: {exc}"
            errors.append(message)
            print(f"ERROR | {message}", file=sys.stderr)
            continue

        schedule_dates = schedule_date_lookup(schedule)
        pregame_index = build_pregame_index(schedule, official_start_times)
        if pregame_index.is_empty():
            message = f"season {season}: failed to build pregame index"
            errors.append(message)
            print(f"ERROR | {message}", file=sys.stderr)
            continue

        index_path = pregame_index_path(season)
        write_parquet_atomic(pregame_index, index_path)
        print(f"  pregame_index: written ({pregame_index.height} games)")

        for spec in DATASET_SPECS:
            if spec.name == "schedules" or spec.name not in selected:
                continue
            try:
                entry = materialize_dataset(
                    spec=spec,
                    season=season,
                    schedule_dates=schedule_dates,
                    overwrite=args.overwrite,
                )
                results.append(entry)
                print(f"  {spec.name}: {entry['status']}")

                if (
                    args.strict
                    and sdv_season_year(season) >= spec.min_season
                    and entry["status"] == "empty_or_unpublished"
                ):
                    strict_failures.append(
                        f"season {season} {spec.name}: empty_or_unpublished"
                    )
            except Exception as exc:
                message = f"season {season} {spec.name}: {exc}"
                errors.append(message)
                results.append(
                    result_entry(
                        spec=spec,
                        season=season,
                        status="error",
                        message=str(exc),
                    )
                )
                print(f"ERROR | {message}", file=sys.stderr)

    run_finished = datetime.now(UTC)
    manifest = {
        "builder": "docs/win/hockey/nhl/scripts/research/build_sdv_history.py",
        "output_root": repo_relative(OUTPUT_DIR),
        "storage_format": "parquet",
        "compression": "zstd",
        "git_tracked": False,
        "required_gitignore_entry": GITIGNORE_ENTRY,
        "sportsdataverse_version": sdv_version,
        "nhl_season_start_years": seasons,
        "sportsdataverse_season_years": [sdv_season_year(season) for season in seasons],
        "datasets_requested": sorted(selected),
        "run_started_utc": utc_iso(run_started),
        "run_finished_utc": utc_iso(run_finished),
        "official_start_time_rows_available": len(official_start_times),
        "pbp_required_research_columns": sorted(PBP_REQUIRED_RESEARCH_COLUMNS),
        "leakage_policy": LEAKAGE_POLICY,
        "results": results,
        "errors": errors,
        "strict_failures": strict_failures,
    }

    stamp = run_finished.strftime("%Y%m%dT%H%M%SZ")
    write_json_atomic(manifest, OUTPUT_DIR / "manifests" / f"run_{stamp}.json")
    write_json_atomic(manifest, OUTPUT_DIR / "manifest_latest.json")

    if errors:
        print(f"FAILED | {len(errors)} loader/build error(s)", file=sys.stderr)
        return 1
    if strict_failures:
        print(
            f"FAILED | {len(strict_failures)} strict empty/unpublished dataset(s)",
            file=sys.stderr,
        )
        return 1

    print(f"DONE | {repo_relative(OUTPUT_DIR)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
