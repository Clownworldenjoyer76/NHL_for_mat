#!/usr/bin/env python3
"""
Single-entry SportsDataverse NHL puller.

Default:
    python docs/win/hockey/nhl/scripts/00_intake/pull_sdv.py

Historical:
    python docs/win/hockey/nhl/scripts/00_intake/pull_sdv.py --season 2024
    python docs/win/hockey/nhl/scripts/00_intake/pull_sdv.py --start-season 2021 --end-season 2024

Optional category subset:
    python docs/win/hockey/nhl/scripts/00_intake/pull_sdv.py --categories schedule,goalie,odds

Current mode never requires a date. It uses the current America/New_York date.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import polars as pl
import sportsdataverse.nhl as nhl


ROOT = Path("docs/win/hockey/nhl/sdv")
CATEGORIES = (
    "schedule",
    "team-strength",
    "goalie",
    "lineup-strength",
    "fatigue",
    "sdv_predictions",
    "odds",
)
NY = ZoneInfo("America/New_York")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--season",
        type=int,
        action="append",
        help="NHL season start year, e.g. --season 2024. Repeat for multiple seasons.",
    )
    parser.add_argument("--start-season", type=int)
    parser.add_argument("--end-season", type=int)
    parser.add_argument(
        "--categories",
        default="all",
        help="Comma-separated category names, or 'all'.",
    )
    return parser.parse_args()


def requested_seasons(args: argparse.Namespace) -> list[int]:
    out: list[int] = []

    if args.season:
        out.extend(args.season)

    if args.start_season is not None or args.end_season is not None:
        if args.start_season is None or args.end_season is None:
            raise SystemExit(
                "--start-season and --end-season must be used together"
            )

        if args.end_season < args.start_season:
            raise SystemExit(
                "--end-season cannot be less than --start-season"
            )

        out.extend(
            range(
                args.start_season,
                args.end_season + 1,
            )
        )

    return sorted(set(out))


def requested_categories(args: argparse.Namespace) -> set[str]:
    if str(args.categories).strip().lower() == "all":
        return set(CATEGORIES)

    values = {
        x.strip()
        for x in str(args.categories).split(",")
        if x.strip()
    }

    unknown = sorted(values - set(CATEGORIES))

    if unknown:
        raise SystemExit(
            f"Unknown categories: {unknown}"
        )

    return values


def season_start_for_day(day: date) -> int:
    return (
        day.year
        if day.month >= 7
        else day.year - 1
    )


def run_stamp() -> str:
    return datetime.now(NY).strftime(
        "%Y_%m_%dT%H%M%S_ET"
    )


def ensure_dirs() -> None:
    ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    for name in CATEGORIES:
        (ROOT / name).mkdir(
            parents=True,
            exist_ok=True,
        )


def is_polars_frame(obj: Any) -> bool:
    return isinstance(
        obj,
        pl.DataFrame,
    )


def is_pandas_frame(obj: Any) -> bool:
    return isinstance(
        obj,
        pd.DataFrame,
    )


def as_pandas(obj: Any) -> pd.DataFrame:
    if is_pandas_frame(obj):
        return obj.copy()

    if is_polars_frame(obj):
        return pd.DataFrame(
            obj.to_dicts()
        )

    raise TypeError(
        f"Not a DataFrame: {type(obj).__name__}"
    )


def csv_safe_value(value: Any) -> Any:
    if isinstance(
        value,
        (dict, list, tuple, set),
    ):
        return json.dumps(
            value,
            default=str,
            ensure_ascii=False,
        )

    return value


def csv_safe_frame(obj: Any) -> pd.DataFrame:
    df = as_pandas(obj)

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].map(
                csv_safe_value
            )

    return df


def _write_json(
    path: Path,
    obj: Any,
) -> None:
    path.write_text(
        json.dumps(
            obj,
            indent=2,
            default=str,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _write_csv(
    path: Path,
    obj: Any,
) -> None:
    csv_safe_frame(obj).to_csv(
        path,
        index=False,
    )


def save_object(
    category: str,
    label: str,
    obj: Any,
    *,
    prefix: str,
    current: bool,
) -> list[Path]:
    out_dir = ROOT / category
    written: list[Path] = []

    if (
        isinstance(obj, dict)
        and obj
        and any(
            is_polars_frame(v)
            or is_pandas_frame(v)
            for v in obj.values()
        )
    ):
        for key, value in obj.items():
            written.extend(
                save_object(
                    category,
                    f"{label}_{key}",
                    value,
                    prefix=prefix,
                    current=current,
                )
            )

        return written

    if (
        is_polars_frame(obj)
        or is_pandas_frame(obj)
    ):
        snap = (
            out_dir
            / f"{prefix}_{label}.csv"
        )

        _write_csv(
            snap,
            obj,
        )

        written.append(snap)

        if current:
            latest = (
                out_dir
                / f"latest_{label}.csv"
            )

            shutil.copyfile(
                snap,
                latest,
            )

            written.append(latest)

        return written

    snap = (
        out_dir
        / f"{prefix}_{label}.json"
    )

    _write_json(
        snap,
        obj,
    )

    written.append(snap)

    if current:
        latest = (
            out_dir
            / f"latest_{label}.json"
        )

        shutil.copyfile(
            snap,
            latest,
        )

        written.append(latest)

    return written


def safe_pull(
    failures: list[str],
    category: str,
    label: str,
    fn,
    *args,
    **kwargs,
):
    try:
        return fn(
            *args,
            **kwargs,
        )

    except Exception as exc:
        failures.append(
            f"{category}/{label}: {exc}"
        )

        print(
            f"WARN | {category}/{label} | {exc}",
            file=sys.stderr,
        )

        return None


def frame_empty(obj: Any) -> bool:
    if obj is None:
        return True

    if is_polars_frame(obj):
        return obj.is_empty()

    if is_pandas_frame(obj):
        return obj.empty

    return False


def clean_war_rows(war: Any) -> Any:
    if is_polars_frame(war):
        if "player_id" not in war.columns:
            return war

        player_id = pl.col(
            "player_id"
        ).cast(
            pl.Int64,
            strict=False,
        )

        return war.filter(
            player_id.is_not_null()
            & (player_id != 0)
        )

    if is_pandas_frame(war):
        if "player_id" not in war.columns:
            return war

        player_id = pd.to_numeric(
            war["player_id"],
            errors="coerce",
        )

        return war.loc[
            player_id.notna()
            & player_id.ne(0)
        ].copy()

    return war


def first_col(
    df: Any,
    names: tuple[str, ...],
) -> str | None:
    cols = set(
        as_pandas(df).columns
    )

    for name in names:
        if name in cols:
            return name

    return None


def filter_exact_date(
    schedule: Any,
    target: date,
):
    if frame_empty(schedule):
        return schedule

    pdf = as_pandas(schedule)

    date_col = first_col(
        pdf,
        (
            "schedule_date",
            "game_date",
            "date",
            "start_date",
        ),
    )

    if date_col is None:
        return schedule

    normalized = pd.to_datetime(
        pdf[date_col],
        errors="coerce",
    ).dt.date

    return pl.from_pandas(
        pdf.loc[
            normalized == target
        ].reset_index(
            drop=True
        )
    )


def schedule_teams(
    schedule: Any,
) -> list[str]:
    if frame_empty(schedule):
        return []

    pdf = as_pandas(schedule)

    home_col = first_col(
        pdf,
        (
            "home_team_abbrev",
            "home_team_abbr",
            "home_abbr",
            "home_team",
        ),
    )

    away_col = first_col(
        pdf,
        (
            "away_team_abbrev",
            "away_team_abbr",
            "away_abbr",
            "away_team",
        ),
    )

    if (
        home_col is None
        or away_col is None
    ):
        return []

    vals = pd.concat(
        [
            pdf[home_col],
            pdf[away_col],
        ],
        ignore_index=True,
    )

    vals = (
        vals
        .dropna()
        .astype(str)
        .str.strip()
    )

    return sorted(
        {
            x
            for x in vals
            if x
        }
    )


def historical_schedule_date_bounds(
    schedule: Any,
) -> tuple[
    date | None,
    date | None,
]:
    if frame_empty(schedule):
        return None, None

    pdf = as_pandas(schedule)

    date_col = first_col(
        pdf,
        (
            "game_date",
            "schedule_date",
            "date",
        ),
    )

    if date_col is None:
        return None, None

    d = pd.to_datetime(
        pdf[date_col],
        errors="coerce",
    ).dropna()

    if d.empty:
        return None, None

    return (
        d.min().date(),
        d.max().date(),
    )


def historical_schedule_game_dates(
    schedule: Any,
) -> list[date]:
    if frame_empty(schedule):
        return []

    pdf = as_pandas(schedule)

    date_col = first_col(
        pdf,
        (
            "game_date",
            "schedule_date",
            "date",
            "start_date",
        ),
    )

    if date_col is None:
        return []

    parsed = pd.to_datetime(
        pdf[date_col],
        errors="coerce",
    ).dropna()

    return sorted(
        {
            value.date()
            for value in parsed
        }
    )


def current_or_historical_prefix(
    *,
    current: bool,
    season: int | None,
) -> str:
    if current:
        return run_stamp()

    assert season is not None

    return f"season_{season}"


def save_pair(
    failures: list[str],
    *,
    category: str,
    label: str,
    parsed_fn,
    raw_fn,
    prefix: str,
    current: bool,
):
    parsed = safe_pull(
        failures,
        category,
        f"{label}_parsed",
        parsed_fn,
    )

    if parsed is not None:
        save_object(
            category,
            label,
            parsed,
            prefix=prefix,
            current=current,
        )

    raw = safe_pull(
        failures,
        category,
        f"{label}_raw",
        raw_fn,
    )

    if raw is not None:
        save_object(
            category,
            f"{label}_raw",
            raw,
            prefix=prefix,
            current=current,
        )

    return parsed


def normalize_loader_schedule_for_ratings(
    schedule: pl.DataFrame,
) -> pl.DataFrame:
    if schedule.is_empty():
        return pl.DataFrame()

    required = {
        "game_id",
        "season",
        "game_date",
        "home_team_abbr",
        "away_team_abbr",
    }

    if not required.issubset(
        set(schedule.columns)
    ):
        return pl.DataFrame()

    work = schedule

    if "game_type" in work.columns:
        work = work.filter(
            pl.col("game_type") == "R"
        )

    return work.select(
        pl.col("game_id"),
        pl.col("season").cast(
            pl.Int64
        ),
        pl.col("game_date")
        .cast(pl.Utf8)
        .str.strptime(
            pl.Date,
            "%Y-%m-%d",
            strict=False,
        )
        .alias("date"),
        pl.col("home_team_abbr")
        .cast(pl.Utf8)
        .alias("home_abbr"),
        pl.col("away_team_abbr")
        .cast(pl.Utf8)
        .alias("away_abbr"),
        pl.lit(False).alias(
            "neutral_site"
        ),
    )


def ratings_from_game_rates(
    game_rates: pl.DataFrame,
) -> pl.DataFrame:
    if game_rates.is_empty():
        return pl.DataFrame()

    const = nhl.get_constants(
        "nhl"
    )

    xg_adj = nhl.adjust_rate_opponent(
        game_rates,
        for_col="xgf",
        against_col="xga",
        hfa=const.hfa,
        avg=const.avg_xgf,
        shrink_k=const.shrink_k,
    )

    goal_adj = nhl.adjust_rate_opponent(
        game_rates,
        for_col="gf",
        against_col="ga",
        hfa=const.hfa,
        avg=(
            const.avg_total_goals
            / 2.0
        ),
        shrink_k=const.shrink_k,
    )

    if (
        xg_adj.is_empty()
        or goal_adj.is_empty()
    ):
        return pl.DataFrame()

    out = (
        xg_adj.join(
            goal_adj.select(
                "team",
                pl.col(
                    "adj_for"
                ).alias(
                    "adj_gf"
                ),
                pl.col(
                    "adj_against"
                ).alias(
                    "adj_ga"
                ),
            ),
            on="team",
            how="left",
        )
        .rename(
            {
                "adj_for": "adj_xgf",
                "adj_against": "adj_xga",
                "adj_net": "adj_xg_net",
            }
        )
    )

    net_mean = out[
        "adj_xg_net"
    ].mean()

    net_std = out[
        "adj_xg_net"
    ].std()

    return out.with_columns(
        pl.col("adj_xgf")
        .rank(
            method="ordinal",
            descending=True,
        )
        .cast(pl.Int64)
        .alias("off_rank"),

        pl.col("adj_xga")
        .rank(
            method="ordinal",
            descending=False,
        )
        .cast(pl.Int64)
        .alias("def_rank"),

        pl.col("adj_xg_net")
        .rank(
            method="ordinal",
            descending=True,
        )
        .cast(pl.Int64)
        .alias("net_rank"),

        (
            (
                (
                    pl.col(
                        "adj_xg_net"
                    )
                    - net_mean
                )
                / net_std
            )
            if net_std not in (
                None,
                0,
            )
            else pl.lit(0.0)
        ).alias(
            "net_z"
        ),
    ).select(
        "season",
        "team",
        "adj_xgf",
        "adj_xga",
        "adj_xg_net",
        "adj_gf",
        "adj_ga",
        "games",
        "off_rank",
        "def_rank",
        "net_rank",
        "net_z",
    )


def prediction_games(
    schedule: Any,
) -> pl.DataFrame:
    if frame_empty(schedule):
        return pl.DataFrame()

    pdf = as_pandas(schedule)

    gid_col = first_col(
        pdf,
        (
            "game_id",
            "id",
            "event_id",
        ),
    )

    home_col = first_col(
        pdf,
        (
            "home_team_abbrev",
            "home_team_abbr",
            "home_abbr",
            "home_team",
        ),
    )

    away_col = first_col(
        pdf,
        (
            "away_team_abbrev",
            "away_team_abbr",
            "away_abbr",
            "away_team",
        ),
    )

    date_col = first_col(
        pdf,
        (
            "schedule_date",
            "game_date",
            "date",
        ),
    )

    if (
        gid_col is None
        or home_col is None
        or away_col is None
    ):
        return pl.DataFrame()

    neutral_col = first_col(
        pdf,
        (
            "neutral_site",
            "neutral",
        ),
    )

    out = pd.DataFrame(
        {
            "game_id": (
                pdf[gid_col]
                .astype(str)
            ),
            "home_team": (
                pdf[home_col]
                .astype(str)
            ),
            "away_team": (
                pdf[away_col]
                .astype(str)
            ),
            "neutral_site": (
                pdf[
                    neutral_col
                ]
                .fillna(False)
                .astype(bool)
                if neutral_col
                else False
            ),
        }
    )

    if date_col:
        out[
            "game_date"
        ] = pd.to_datetime(
            pdf[date_col],
            errors="coerce",
        ).dt.strftime(
            "%Y-%m-%d"
        )

    return pl.from_pandas(out)


def build_fatigue_from_team_schedule(
    team: str,
    team_schedule: Any,
    *,
    target_date: date | None = None,
) -> pl.DataFrame:
    if frame_empty(
        team_schedule
    ):
        return pl.DataFrame()

    pdf = as_pandas(
        team_schedule
    )

    date_col = first_col(
        pdf,
        (
            "game_date",
            "schedule_date",
            "date",
        ),
    )

    if date_col is None:
        return pl.DataFrame()

    dates = sorted(
        {
            x.date()
            for x in pd.to_datetime(
                pdf[date_col],
                errors="coerce",
            ).dropna()
        }
    )

    rows: list[
        dict[str, Any]
    ] = []

    for idx, game_day in enumerate(
        dates
    ):
        prior = dates[:idx]

        previous = (
            prior[-1]
            if prior
            else None
        )

        days_rest = (
            (
                game_day
                - previous
            ).days
            if previous
            else None
        )

        def count_inclusive(
            window_days: int,
        ) -> int:
            return 1 + sum(
                1
                for d in prior
                if (
                    0
                    < (
                        game_day
                        - d
                    ).days
                    < window_days
                )
            )

        games_2 = count_inclusive(
            2
        )

        games_4 = count_inclusive(
            4
        )

        games_6 = count_inclusive(
            6
        )

        games_7 = count_inclusive(
            7
        )

        rows.append(
            {
                "team": team,
                "game_date": (
                    game_day.isoformat()
                ),
                "previous_game_date": (
                    previous.isoformat()
                    if previous
                    else None
                ),
                "days_rest": (
                    days_rest
                ),
                "back_to_back": (
                    games_2 >= 2
                ),
                "games_in_4_days": (
                    games_4
                ),
                "three_in_four": (
                    games_4 >= 3
                ),
                "games_in_6_days": (
                    games_6
                ),
                "four_in_six": (
                    games_6 >= 4
                ),
                "games_in_7_days": (
                    games_7
                ),
            }
        )

    out = pl.DataFrame(rows)

    if (
        target_date is not None
        and not out.is_empty()
    ):
        out = out.filter(
            pl.col(
                "game_date"
            )
            == target_date.isoformat()
        )

    return out


def build_fatigue_from_league_schedule(
    schedule: Any,
) -> pl.DataFrame:
    if frame_empty(schedule):
        return pl.DataFrame()

    pdf = as_pandas(schedule)

    date_col = first_col(
        pdf,
        (
            "game_date",
            "schedule_date",
            "date",
        ),
    )

    home_col = first_col(
        pdf,
        (
            "home_team_abbrev",
            "home_team_abbr",
            "home_abbr",
            "home_team",
        ),
    )

    away_col = first_col(
        pdf,
        (
            "away_team_abbrev",
            "away_team_abbr",
            "away_abbr",
            "away_team",
        ),
    )

    if (
        date_col is None
        or home_col is None
        or away_col is None
    ):
        return pl.DataFrame()

    long_rows = []

    for _, row in pdf.iterrows():
        d = pd.to_datetime(
            row.get(
                date_col
            ),
            errors="coerce",
        )

        if pd.isna(d):
            continue

        long_rows.append(
            (
                str(
                    row.get(
                        home_col,
                        "",
                    )
                ).strip(),
                d.date(),
            )
        )

        long_rows.append(
            (
                str(
                    row.get(
                        away_col,
                        "",
                    )
                ).strip(),
                d.date(),
            )
        )

    by_team: dict[
        str,
        list[date],
    ] = {}

    for team, d in long_rows:
        if team:
            by_team.setdefault(
                team,
                [],
            ).append(d)

    frames = []

    for team, dates in by_team.items():
        fake = pd.DataFrame(
            {
                "game_date": sorted(
                    set(dates)
                )
            }
        )

        frames.append(
            build_fatigue_from_team_schedule(
                team,
                fake,
            )
        )

    return (
        pl.concat(
            frames,
            how="diagonal_relaxed",
        )
        if frames
        else pl.DataFrame()
    )


def historical_predictions(
    schedule: pl.DataFrame,
    pbp: pl.DataFrame,
) -> pl.DataFrame:
    if (
        schedule.is_empty()
        or pbp.is_empty()
    ):
        return pl.DataFrame()

    rating_schedule = (
        normalize_loader_schedule_for_ratings(
            schedule
        )
    )

    if rating_schedule.is_empty():
        return pl.DataFrame()

    game_rates = (
        nhl.team_game_xg_rates(
            pbp,
            rating_schedule,
        )
    )

    if game_rates.is_empty():
        return pl.DataFrame()

    games = prediction_games(
        schedule
    )

    if (
        games.is_empty()
        or "game_date"
        not in games.columns
    ):
        return pl.DataFrame()

    dates = sorted(
        {
            d
            for d in games[
                "game_date"
            ].to_list()
            if (
                isinstance(
                    d,
                    str,
                )
                and d
            )
        }
    )

    outputs = []

    for d_str in dates:
        try:
            d = date.fromisoformat(
                d_str
            )
        except ValueError:
            continue

        prior_rates = (
            game_rates.filter(
                pl.col("date")
                < pl.lit(d)
            )
        )

        ratings = (
            ratings_from_game_rates(
                prior_rates
            )
        )

        if ratings.is_empty():
            continue

        games_day = (
            games.filter(
                pl.col(
                    "game_date"
                )
                == d_str
            )
            .select(
                "game_id",
                "home_team",
                "away_team",
                "neutral_site",
            )
        )

        if games_day.is_empty():
            continue

        pred = (
            nhl.nhl_predict_games(
                games_day,
                ratings,
            )
        )

        if not pred.is_empty():
            outputs.append(
                pred.with_columns(
                    pl.lit(
                        d_str
                    ).alias(
                        "game_date"
                    )
                )
            )

    return (
        pl.concat(
            outputs,
            how="diagonal_relaxed",
        )
        if outputs
        else pl.DataFrame()
    )


def pull_schedule(
    failures: list[str],
    *,
    current: bool,
    target_date: date,
    season: int,
    prefix: str,
):
    if current:
        parsed = safe_pull(
            failures,
            "schedule",
            "nhl_web_schedule",
            nhl.nhl_web_schedule,
            date=target_date.isoformat(),
        )

        raw = safe_pull(
            failures,
            "schedule",
            "nhl_web_schedule_raw",
            nhl.nhl_web_schedule,
            date=target_date.isoformat(),
            return_parsed=False,
        )

        if parsed is not None:
            save_object(
                "schedule",
                "nhl_schedule",
                parsed,
                prefix=prefix,
                current=True,
            )

        if raw is not None:
            save_object(
                "schedule",
                "nhl_schedule_raw",
                raw,
                prefix=prefix,
                current=True,
            )

        slate = (
            filter_exact_date(
                parsed,
                target_date,
            )
            if parsed is not None
            else pl.DataFrame()
        )

        if not frame_empty(
            slate
        ):
            save_object(
                "schedule",
                "nhl_schedule_slate",
                slate,
                prefix=prefix,
                current=True,
            )

        return parsed, slate

    parsed = safe_pull(
        failures,
        "schedule",
        f"load_nhl_schedule_{season}",
        nhl.load_nhl_schedule,
        season,
    )

    if parsed is not None:
        save_object(
            "schedule",
            "nhl_schedule",
            parsed,
            prefix=prefix,
            current=False,
        )

    return parsed, parsed


def pull_team_strength(
    failures: list[str],
    *,
    current: bool,
    target_date: date,
    season: int,
    prefix: str,
    teams: list[str],
):
    ratings = safe_pull(
        failures,
        "team-strength",
        "nhl_team_ratings",
        nhl.nhl_team_ratings,
        season,
        as_of_date=(
            target_date
            if current
            else None
        ),
    )

    if ratings is not None:
        save_object(
            "team-strength",
            "team_ratings",
            ratings,
            prefix=prefix,
            current=current,
        )

    standings_date = (
        target_date.isoformat()
    )

    standings = safe_pull(
        failures,
        "team-strength",
        "standings",
        nhl.nhl_standings,
        date=standings_date,
    )

    if standings is not None:
        save_object(
            "team-strength",
            "standings",
            standings,
            prefix=prefix,
            current=current,
        )

    standings_raw = safe_pull(
        failures,
        "team-strength",
        "standings_raw",
        nhl.nhl_standings,
        date=standings_date,
        return_parsed=False,
    )

    if standings_raw is not None:
        save_object(
            "team-strength",
            "standings_raw",
            standings_raw,
            prefix=prefix,
            current=current,
        )

    for team in teams:
        parsed = safe_pull(
            failures,
            "team-strength",
            f"{team}_club_stats",
            nhl.nhl_club_stats,
            team=team,
            season=(
                None
                if current
                else season
            ),
        )

        if parsed is not None:
            save_object(
                "team-strength",
                f"{team}_club_stats",
                parsed,
                prefix=prefix,
                current=current,
            )

        raw = safe_pull(
            failures,
            "team-strength",
            f"{team}_club_stats_raw",
            nhl.nhl_club_stats,
            team=team,
            season=(
                None
                if current
                else season
            ),
            return_parsed=False,
        )

        if raw is not None:
            save_object(
                "team-strength",
                f"{team}_club_stats_raw",
                raw,
                prefix=prefix,
                current=current,
            )

    return ratings


def pull_rosters(
    failures: list[str],
    *,
    current: bool,
    season: int,
    prefix: str,
    teams: list[str],
) -> dict[str, Any]:
    rosters: dict[
        str,
        Any,
    ] = {}

    for team in teams:
        parsed = safe_pull(
            failures,
            "lineup-strength",
            f"{team}_roster",
            nhl.nhl_roster,
            team=team,
            season=(
                None
                if current
                else season
            ),
        )

        if parsed is not None:
            rosters[
                team
            ] = parsed

            save_object(
                "lineup-strength",
                f"{team}_roster",
                parsed,
                prefix=prefix,
                current=current,
            )

        raw = safe_pull(
            failures,
            "lineup-strength",
            f"{team}_roster_raw",
            nhl.nhl_roster,
            team=team,
            season=(
                None
                if current
                else season
            ),
            return_parsed=False,
        )

        if raw is not None:
            save_object(
                "lineup-strength",
                f"{team}_roster_raw",
                raw,
                prefix=prefix,
                current=current,
            )

    return rosters


def pull_goalie_live_profiles(
    failures: list[str],
    *,
    current: bool,
    prefix: str,
    rosters: dict[str, Any],
):
    for team, roster in rosters.items():
        if frame_empty(
            roster
        ):
            continue

        pdf = as_pandas(
            roster
        )

        if (
            "position_group"
            not in pdf.columns
        ):
            continue

        goalies = pdf.loc[
            pdf[
                "position_group"
            ]
            .astype(str)
            .str.lower()
            .eq("goalies")
        ].copy()

        if goalies.empty:
            continue

        save_object(
            "goalie",
            f"{team}_goalies_roster",
            goalies,
            prefix=prefix,
            current=current,
        )

        id_col = first_col(
            goalies,
            (
                "id",
                "player_id",
                "playerId",
            ),
        )

        if id_col is None:
            continue

        for value in (
            goalies[
                id_col
            ]
            .dropna()
            .unique()
            .tolist()
        ):
            try:
                player_id = int(
                    value
                )
            except Exception:
                continue

            parsed = safe_pull(
                failures,
                "goalie",
                f"{team}_{player_id}_player_landing",
                nhl.nhl_player_landing,
                player_id=player_id,
            )

            if parsed is not None:
                save_object(
                    "goalie",
                    f"{team}_{player_id}_player_landing",
                    parsed,
                    prefix=prefix,
                    current=current,
                )

            raw = safe_pull(
                failures,
                "goalie",
                f"{team}_{player_id}_player_landing_raw",
                nhl.nhl_player_landing,
                player_id=player_id,
                return_parsed=False,
            )

            if raw is not None:
                save_object(
                    "goalie",
                    f"{team}_{player_id}_player_landing_raw",
                    raw,
                    prefix=prefix,
                    current=current,
                )


def pull_season_context(
    failures: list[str],
    *,
    season: int,
):
    context = {
        "pbp": None,
        "shifts": None,
        "goalie_box": None,
        "skater_box": None,
        "rosters": None,
    }

    context[
        "pbp"
    ] = safe_pull(
        failures,
        "lineup-strength",
        "load_nhl_pbp_full",
        nhl.load_nhl_pbp_full,
        season,
    )

    context[
        "shifts"
    ] = safe_pull(
        failures,
        "lineup-strength",
        "load_nhl_shifts",
        nhl.load_nhl_shifts,
        season,
    )

    context[
        "goalie_box"
    ] = safe_pull(
        failures,
        "goalie",
        "load_nhl_goalie_boxscores",
        nhl.load_nhl_goalie_boxscores,
        season,
    )

    context[
        "skater_box"
    ] = safe_pull(
        failures,
        "lineup-strength",
        "load_nhl_skater_boxscores",
        nhl.load_nhl_skater_boxscores,
        season,
    )

    context[
        "rosters"
    ] = safe_pull(
        failures,
        "lineup-strength",
        "load_nhl_rosters",
        nhl.load_nhl_rosters,
        season,
    )

    return context


def save_season_context(
    *,
    current: bool,
    prefix: str,
    context: dict[str, Any],
):
    mapping = (
        (
            "goalie",
            "goalie_boxscores",
            context.get(
                "goalie_box"
            ),
        ),
        (
            "lineup-strength",
            "skater_boxscores",
            context.get(
                "skater_box"
            ),
        ),
        (
            "lineup-strength",
            "season_rosters",
            context.get(
                "rosters"
            ),
        ),
    )

    for (
        category,
        label,
        obj,
    ) in mapping:
        if obj is not None:
            save_object(
                category,
                label,
                obj,
                prefix=prefix,
                current=current,
            )


def pull_advanced_player_strength(
    failures: list[str],
    *,
    current: bool,
    prefix: str,
    context: dict[str, Any],
):
    pbp = context.get(
        "pbp"
    )

    shifts = context.get(
        "shifts"
    )

    if (
        frame_empty(pbp)
        or frame_empty(shifts)
    ):
        return

    gsax = safe_pull(
        failures,
        "goalie",
        "goalie_gsax",
        nhl.nhl_goalie_gsax,
        pbp,
        shifts,
    )

    if gsax is not None:
        save_object(
            "goalie",
            "goalie_gsax",
            gsax,
            prefix=prefix,
            current=current,
        )

    rapm = safe_pull(
        failures,
        "lineup-strength",
        "skater_rapm",
        nhl.nhl_skater_rapm,
        pbp,
        shifts,
    )

    if rapm is not None:
        save_object(
            "lineup-strength",
            "skater_rapm",
            rapm,
            prefix=prefix,
            current=current,
        )

    war = safe_pull(
        failures,
        "lineup-strength",
        "skater_war",
        nhl.nhl_skater_war,
        pbp,
        shifts,
    )

    if war is not None:
        war = clean_war_rows(
            war
        )

        save_object(
            "lineup-strength",
            "skater_war",
            war,
            prefix=prefix,
            current=current,
        )

    special = safe_pull(
        failures,
        "lineup-strength",
        "special_teams_value",
        nhl.nhl_special_teams_value,
        pbp,
        shifts,
    )

    if special is not None:
        save_object(
            "lineup-strength",
            "special_teams_value",
            special,
            prefix=prefix,
            current=current,
        )

    for unit_type in (
        "forward_line",
        "defense_pair",
    ):
        units = safe_pull(
            failures,
            "lineup-strength",
            f"unit_ratings_{unit_type}",
            nhl.nhl_unit_ratings,
            pbp,
            shifts,
            unit_type=unit_type,
        )

        if units is not None:
            save_object(
                "lineup-strength",
                f"unit_ratings_{unit_type}",
                units,
                prefix=prefix,
                current=current,
            )


def pull_fatigue(
    failures: list[str],
    *,
    current: bool,
    target_date: date,
    season: int,
    prefix: str,
    teams: list[str],
    schedule: Any,
):
    if not current:
        fatigue = (
            build_fatigue_from_league_schedule(
                schedule
            )
        )

        save_object(
            "fatigue",
            "fatigue",
            fatigue,
            prefix=prefix,
            current=False,
        )

        return

    fatigue_frames = []

    for team in teams:
        parsed = safe_pull(
            failures,
            "fatigue",
            f"{team}_club_schedule",
            nhl.nhl_club_schedule_season,
            team=team,
        )

        if parsed is not None:
            save_object(
                "fatigue",
                f"{team}_club_schedule",
                parsed,
                prefix=prefix,
                current=True,
            )

            f = (
                build_fatigue_from_team_schedule(
                    team,
                    parsed,
                    target_date=target_date,
                )
            )

            if not f.is_empty():
                fatigue_frames.append(
                    f
                )

        raw = safe_pull(
            failures,
            "fatigue",
            f"{team}_club_schedule_raw",
            nhl.nhl_club_schedule_season,
            team=team,
            return_parsed=False,
        )

        if raw is not None:
            save_object(
                "fatigue",
                f"{team}_club_schedule_raw",
                raw,
                prefix=prefix,
                current=True,
            )

    fatigue = (
        pl.concat(
            fatigue_frames,
            how="diagonal_relaxed",
        )
        if fatigue_frames
        else pl.DataFrame()
    )

    save_object(
        "fatigue",
        "fatigue",
        fatigue,
        prefix=prefix,
        current=True,
    )


def pull_predictions(
    failures: list[str],
    *,
    current: bool,
    target_date: date,
    season: int,
    prefix: str,
    schedule: Any,
    slate: Any,
    ratings: Any,
    context: dict[str, Any] | None,
):
    if current:
        if (
            frame_empty(slate)
            or frame_empty(ratings)
        ):
            save_object(
                "sdv_predictions",
                "predictions",
                pl.DataFrame(),
                prefix=prefix,
                current=True,
            )

            return

        games = prediction_games(
            slate
        )

        if games.is_empty():
            return

        if (
            "game_date"
            in games.columns
        ):
            games = games.select(
                "game_id",
                "home_team",
                "away_team",
                "neutral_site",
            )

        pred = safe_pull(
            failures,
            "sdv_predictions",
            "nhl_predict_games",
            nhl.nhl_predict_games,
            games,
            ratings,
        )

        if pred is not None:
            save_object(
                "sdv_predictions",
                "predictions",
                pred,
                prefix=prefix,
                current=True,
            )

        return

    if context is None:
        return

    pbp = context.get(
        "pbp"
    )

    if (
        frame_empty(schedule)
        or frame_empty(pbp)
    ):
        return

    pred = safe_pull(
        failures,
        "sdv_predictions",
        "historical_predictions",
        historical_predictions,
        schedule,
        pbp,
    )

    if pred is not None:
        save_object(
            "sdv_predictions",
            "predictions",
            pred,
            prefix=prefix,
            current=False,
        )


def espn_event_ids(
    schedule: Any,
) -> list[str]:
    if frame_empty(schedule):
        return []

    pdf = as_pandas(
        schedule
    )

    id_col = first_col(
        pdf,
        (
            "game_id",
            "id",
            "event_id",
        ),
    )

    if id_col is None:
        return []

    return sorted(
        {
            str(v).strip()
            for v in (
                pdf[
                    id_col
                ]
                .dropna()
                .tolist()
            )
            if str(v).strip()
        }
    )


def pull_odds(
    failures: list[str],
    *,
    current: bool,
    target_date: date,
    season: int,
    prefix: str,
):
    if current:
        query_dates = [
            target_date
        ]

    else:
        historical_schedule = (
            safe_pull(
                failures,
                "odds",
                f"load_nhl_schedule_{season}",
                nhl.load_nhl_schedule,
                season,
            )
        )

        query_dates = (
            historical_schedule_game_dates(
                historical_schedule
            )
        )

        if not query_dates:
            failures.append(
                "odds/historical_schedule_dates: "
                f"no game dates found for season={season}"
            )

            save_object(
                "odds",
                "espn_schedule",
                pl.DataFrame(),
                prefix=prefix,
                current=False,
            )

            save_object(
                "odds",
                "espn_scoreboard_raw",
                {},
                prefix=prefix,
                current=False,
            )

            save_object(
                "odds",
                "espn_odds",
                pl.DataFrame(),
                prefix=prefix,
                current=False,
            )

            save_object(
                "odds",
                "espn_odds_raw",
                {},
                prefix=prefix,
                current=False,
            )

            return

    schedule_frames = []
    scoreboard_raw_by_date: dict[
        str,
        Any,
    ] = {}

    for game_day in query_dates:
        dates_arg = int(
            game_day.strftime(
                "%Y%m%d"
            )
        )

        espn_schedule_day = (
            safe_pull(
                failures,
                "odds",
                f"espn_schedule_{dates_arg}",
                nhl.espn_nhl_schedule,
                dates=dates_arg,
                limit=5000,
            )
        )

        if (
            espn_schedule_day
            is not None
            and not frame_empty(
                espn_schedule_day
            )
        ):
            schedule_frames.append(
                pl.from_pandas(
                    as_pandas(
                        espn_schedule_day
                    )
                )
            )

        scoreboard_raw = (
            safe_pull(
                failures,
                "odds",
                f"espn_scoreboard_raw_{dates_arg}",
                nhl.espn_nhl_scoreboard,
                dates=dates_arg,
                limit=5000,
                return_parsed=False,
            )
        )

        if scoreboard_raw is not None:
            scoreboard_raw_by_date[
                game_day.isoformat()
            ] = scoreboard_raw

        if not current:
            time.sleep(
                0.02
            )

    espn_schedule = (
        pl.concat(
            schedule_frames,
            how="diagonal_relaxed",
        )
        if schedule_frames
        else pl.DataFrame()
    )

    if (
        not espn_schedule.is_empty()
    ):
        id_col = first_col(
            espn_schedule,
            (
                "game_id",
                "id",
                "event_id",
            ),
        )

        if id_col is not None:
            pdf = as_pandas(
                espn_schedule
            )

            pdf[
                id_col
            ] = (
                pdf[
                    id_col
                ]
                .astype(str)
                .str.strip()
            )

            pdf = (
                pdf.loc[
                    pdf[
                        id_col
                    ].ne("")
                ]
                .drop_duplicates(
                    subset=[
                        id_col
                    ],
                    keep="first",
                )
                .reset_index(
                    drop=True
                )
            )

            espn_schedule = (
                pl.from_pandas(
                    pdf
                )
            )

    save_object(
        "odds",
        "espn_schedule",
        espn_schedule,
        prefix=prefix,
        current=current,
    )

    save_object(
        "odds",
        "espn_scoreboard_raw",
        scoreboard_raw_by_date,
        prefix=prefix,
        current=current,
    )

    event_ids = espn_event_ids(
        espn_schedule
    )

    print(
        f"ESPN odds discovery | "
        f"dates={len(query_dates)} | "
        f"events={len(event_ids)}"
    )

    odds_frames = []

    raw_by_event: dict[
        str,
        Any,
    ] = {}

    for event_id in event_ids:
        parsed = safe_pull(
            failures,
            "odds",
            f"espn_odds_{event_id}",
            nhl.espn_nhl_game_odds,
            event_id=event_id,
        )

        if (
            parsed is not None
            and not frame_empty(
                parsed
            )
        ):
            pdf = as_pandas(
                parsed
            )

            pdf.insert(
                0,
                "espn_event_id",
                event_id,
            )

            odds_frames.append(
                pl.from_pandas(
                    pdf
                )
            )

        raw = safe_pull(
            failures,
            "odds",
            f"espn_odds_raw_{event_id}",
            nhl.espn_nhl_game_odds,
            event_id=event_id,
            return_parsed=False,
        )

        if raw is not None:
            raw_by_event[
                event_id
            ] = raw

        if not current:
            time.sleep(
                0.05
            )

    combined = (
        pl.concat(
            odds_frames,
            how="diagonal_relaxed",
        )
        if odds_frames
        else pl.DataFrame()
    )

    save_object(
        "odds",
        "espn_odds",
        combined,
        prefix=prefix,
        current=current,
    )

    save_object(
        "odds",
        "espn_odds_raw",
        raw_by_event,
        prefix=prefix,
        current=current,
    )


def run_one(
    *,
    current: bool,
    target_date: date,
    season: int,
    categories: set[str],
) -> list[str]:
    failures: list[str] = []

    prefix = (
        current_or_historical_prefix(
            current=current,
            season=(
                None
                if current
                else season
            ),
        )
    )

    schedule = pl.DataFrame()
    slate = pl.DataFrame()

    if (
        "schedule" in categories
        or "team-strength" in categories
        or "goalie" in categories
        or "lineup-strength" in categories
        or "fatigue" in categories
        or "sdv_predictions" in categories
    ):
        (
            schedule,
            slate,
        ) = pull_schedule(
            failures,
            current=current,
            target_date=target_date,
            season=season,
            prefix=prefix,
        )

    relevant_schedule = (
        slate
        if current
        else schedule
    )

    teams = schedule_teams(
        relevant_schedule
    )

    team_strength_date = (
        target_date
    )

    if not current:
        (
            _,
            max_day,
        ) = historical_schedule_date_bounds(
            schedule
        )

        if max_day is not None:
            team_strength_date = (
                max_day
            )

    ratings = None

    if (
        "team-strength"
        in categories
        or "sdv_predictions"
        in categories
    ):
        ratings = pull_team_strength(
            failures,
            current=current,
            target_date=(
                team_strength_date
                if not current
                else target_date
            ),
            season=season,
            prefix=prefix,
            teams=teams,
        )

    rosters: dict[
        str,
        Any,
    ] = {}

    if (
        "lineup-strength"
        in categories
        or "goalie"
        in categories
    ):
        rosters = pull_rosters(
            failures,
            current=current,
            season=season,
            prefix=prefix,
            teams=teams,
        )

    if "goalie" in categories:
        pull_goalie_live_profiles(
            failures,
            current=current,
            prefix=prefix,
            rosters=rosters,
        )

    need_context = bool(
        {
            "goalie",
            "lineup-strength",
            "sdv_predictions",
        }
        & categories
    )

    context = None

    if need_context:
        context = (
            pull_season_context(
                failures,
                season=season,
            )
        )

        save_season_context(
            current=current,
            prefix=prefix,
            context=context,
        )

    if (
        context is not None
        and (
            "goalie"
            in categories
            or "lineup-strength"
            in categories
        )
    ):
        pull_advanced_player_strength(
            failures,
            current=current,
            prefix=prefix,
            context=context,
        )

    if "fatigue" in categories:
        pull_fatigue(
            failures,
            current=current,
            target_date=target_date,
            season=season,
            prefix=prefix,
            teams=teams,
            schedule=schedule,
        )

    if (
        "sdv_predictions"
        in categories
    ):
        pull_predictions(
            failures,
            current=current,
            target_date=target_date,
            season=season,
            prefix=prefix,
            schedule=schedule,
            slate=slate,
            ratings=ratings,
            context=context,
        )

    if "odds" in categories:
        pull_odds(
            failures,
            current=current,
            target_date=target_date,
            season=season,
            prefix=prefix,
        )

    return failures


def main() -> None:
    ensure_dirs()

    args = parse_args()

    categories = (
        requested_categories(
            args
        )
    )

    seasons = (
        requested_seasons(
            args
        )
    )

    now = datetime.now(
        NY
    )

    target_date = now.date()

    all_failures: list[
        str
    ] = []

    if not seasons:
        current_season = (
            season_start_for_day(
                target_date
            )
        )

        print(
            f"SDV NHL current pull | "
            f"date={target_date.isoformat()} "
            f"| season={current_season} "
            f"| categories={sorted(categories)}"
        )

        all_failures.extend(
            run_one(
                current=True,
                target_date=target_date,
                season=current_season,
                categories=categories,
            )
        )

    else:
        for season in seasons:
            print(
                f"SDV NHL historical pull | "
                f"season={season} "
                f"| categories={sorted(categories)}"
            )

            all_failures.extend(
                run_one(
                    current=False,
                    target_date=target_date,
                    season=season,
                    categories=categories,
                )
            )

    if all_failures:
        print(
            "\nSDV pull completed with warnings:",
            file=sys.stderr,
        )

        for item in all_failures:
            print(
                f"  - {item}",
                file=sys.stderr,
            )

    print(
        "SDV NHL pull complete."
    )


if __name__ == "__main__":
    try:
        main()

    except Exception:
        traceback.print_exc()
        sys.exit(1)