#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/05_final_scores/transform_final_scores.py

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import pandas as pd


SPORT = "hockey"
LEAGUE = "nhl"
LEAGUE_OUT = "NHL"

RAW_DIR = Path("docs/win/hockey/nhl/00_intake/drat_raw")
GAMES_DIR = Path("docs/win/hockey/nhl/00_intake/games")

OUT_DIR = Path("docs/win/hockey/nhl/05_final_scores/final_scores")
ERROR_DIR = Path("docs/win/hockey/nhl/errors/05_final_scores")
LOG_FILE = ERROR_DIR / "transform_final_scores.txt"

RAW_PATTERN = "*_nhl_raw.json"

OUTPUT_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_id",
    "away_team",
    "home_team",
    "away_score",
    "home_score",
    "total_score",
    "away_puck_line_result",
    "home_puck_line_result",
]


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)


def reset_log() -> None:
    LOG_FILE.write_text("", encoding="utf-8")


def log(msg: str) -> None:
    stamp = datetime.now(UTC).isoformat()
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{stamp} | {msg}\n")


def fail(msg: str) -> None:
    log(f"ERROR: {msg}")
    raise RuntimeError(msg)


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        fail(f"Failed reading JSON {path}: {e}")


def flatten_raw_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for key in ["rows", "data", "games", "events", "raw_rows"]:
            if key in payload and isinstance(payload[key], list):
                rows = payload[key]
                break
        else:
            rows = [payload]
    else:
        fail(f"Unsupported raw JSON payload type: {type(payload).__name__}")

    return [r for r in rows if isinstance(r, dict)]


def norm_key(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def first_present(row: dict[str, Any], names: list[str]) -> Any:
    for name in names:
        if name in row and row[name] not in [None, ""]:
            return row[name]

    lower_map = {str(k).lower(): k for k in row.keys()}
    for name in names:
        key = lower_map.get(name.lower())
        if key is not None and row[key] not in [None, ""]:
            return row[key]

    return None


def parse_int_score(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if pd.isna(value):
            return None
        if value.is_integer():
            return int(value)
        return None

    s = str(value).strip()
    if not s:
        return None

    m = re.search(r"-?\d+", s)
    if not m:
        return None

    return int(m.group(0))


def parse_game_date(row: dict[str, Any]) -> str:
    raw_date = first_present(
        row,
        [
            "game_date",
            "date",
            "date_time",
            "datetime",
            "game_time",
            "start_time",
        ],
    )

    if raw_date is None:
        return ""

    s = str(raw_date).strip()
    if not s:
        return ""

    formats = [
        "%Y_%m_%d",
        "%Y-%m-%d",
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y_%m_%d")
        except ValueError:
            pass

    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        mm, dd, yyyy = m.groups()
        return f"{yyyy}_{mm}_{dd}"

    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", s)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}_{mm}_{dd}"

    return ""


def is_completed(row: dict[str, Any]) -> bool:
    status = first_present(
        row,
        [
            "game_status",
            "status",
            "status_type",
            "event_status",
            "state",
            "game_state",
        ],
    )

    completed_values = {
        "completed",
        "complete",
        "final",
        "final ot",
        "final so",
        "ended",
        "closed",
    }

    return norm_key(status) in completed_values


def get_team(row: dict[str, Any], side: str) -> str:
    if side == "away":
        names = [
            "away_team",
            "away",
            "visitor_team",
            "visitor",
            "road_team",
            "team_away",
            "away_name",
            "team1",
        ]
    elif side == "home":
        names = [
            "home_team",
            "home",
            "team_home",
            "home_name",
            "team2",
        ]
    else:
        fail(f"Invalid side: {side}")

    value = first_present(row, names)
    if value is None:
        return ""

    return str(value).strip()


def get_score(row: dict[str, Any], side: str) -> int | None:
    if side == "away":
        names = [
            "away_score",
            "away_team_score",
            "visitor_score",
            "road_score",
            "away_points",
            "score_away",
            "score1",
        ]
    elif side == "home":
        names = [
            "home_score",
            "home_team_score",
            "home_points",
            "score_home",
            "score2",
        ]
    else:
        fail(f"Invalid side: {side}")

    return parse_int_score(first_present(row, names))


def load_games_for_date(game_date: str) -> pd.DataFrame:
    path = GAMES_DIR / f"{game_date}_nhl_games.csv"

    if not path.exists():
        log(f"Stage 00 games file not found for {game_date}: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str)

    required = {"game_id", "game_date", "away_team", "home_team"}
    missing = sorted(required - set(df.columns))
    if missing:
        fail(f"Stage 00 games file missing required columns {missing}: {path}")

    df = df.copy()
    df["game_date"] = df["game_date"].astype(str).str.strip()
    df["away_team_key"] = df["away_team"].map(norm_key)
    df["home_team_key"] = df["home_team"].map(norm_key)

    return df


def attach_game_id(row: dict[str, Any], games_cache: dict[str, pd.DataFrame]) -> str:
    raw_game_id = first_present(row, ["game_id", "id", "event_id", "gameId", "eventId"])

    if raw_game_id not in [None, ""]:
        return str(raw_game_id).strip()

    game_date = str(row.get("game_date", "")).strip()
    if not game_date:
        return ""

    if game_date not in games_cache:
        games_cache[game_date] = load_games_for_date(game_date)

    games_df = games_cache[game_date]

    if games_df.empty:
        return ""

    away_key = norm_key(row.get("away_team"))
    home_key = norm_key(row.get("home_team"))

    matches = games_df[
        (games_df["game_date"] == game_date)
        & (games_df["away_team_key"] == away_key)
        & (games_df["home_team_key"] == home_key)
    ]

    if len(matches) == 1:
        return str(matches.iloc[0]["game_id"]).strip()

    if len(matches) > 1:
        log(
            f"Multiple Stage 00 game_id matches for {game_date}: "
            f"{row.get('away_team')} at {row.get('home_team')}"
        )
        return ""

    log(
        f"No Stage 00 game_id match for {game_date}: "
        f"{row.get('away_team')} at {row.get('home_team')}"
    )
    return ""


def build_final_score_rows(raw_rows: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    games_cache: dict[str, pd.DataFrame] = {}

    raw_count = 0
    completed_count = 0
    skipped_missing_date = 0
    skipped_missing_team = 0
    skipped_missing_score = 0

    for raw in raw_rows:
        raw_count += 1

        if not is_completed(raw):
            continue

        completed_count += 1

        game_date = parse_game_date(raw)
        away_team = get_team(raw, "away")
        home_team = get_team(raw, "home")
        away_score = get_score(raw, "away")
        home_score = get_score(raw, "home")

        if not game_date:
            skipped_missing_date += 1
            log(f"Skipped completed row with missing/unreadable game date: {raw}")
            continue

        if not away_team or not home_team:
            skipped_missing_team += 1
            log(f"Skipped completed row with missing team name: {raw}")
            continue

        if away_score is None or home_score is None:
            skipped_missing_score += 1
            log(f"Skipped completed row with missing score: {raw}")
            continue

        away_score_int = int(away_score)
        home_score_int = int(home_score)

        shaped = {
            "sport": SPORT,
            "league": LEAGUE,
            "game_date": game_date,
            "game_id": "",
            "away_team": away_team,
            "home_team": home_team,
            "away_score": away_score_int,
            "home_score": home_score_int,
            "total_score": away_score_int + home_score_int,
            "away_puck_line_result": away_score_int - home_score_int,
            "home_puck_line_result": home_score_int - away_score_int,
        }

        shaped["game_id"] = attach_game_id(shaped | raw, games_cache)

        rows.append(shaped)

    log(
        f"raw_rows={raw_count}, completed_rows={completed_count}, "
        f"built_rows={len(rows)}, skipped_missing_date={skipped_missing_date}, "
        f"skipped_missing_team={skipped_missing_team}, skipped_missing_score={skipped_missing_score}"
    )

    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    if df.empty:
        return df

    missing_game_id = df["game_id"].astype(str).str.strip().eq("")
    if missing_game_id.any():
        for _, r in df.loc[missing_game_id, ["game_date", "away_team", "home_team"]].iterrows():
            log(
                f"Missing game_id after transform: "
                f"{r['game_date']} | {r['away_team']} at {r['home_team']}"
            )

    keyed = df[df["game_id"].astype(str).str.strip().ne("")]
    dupes = keyed.duplicated(subset=["game_id"], keep=False)
    if dupes.any():
        duped_ids = sorted(keyed.loc[dupes, "game_id"].astype(str).unique().tolist())
        fail(f"Duplicate game_id values in transformed final scores: {duped_ids}")

    return df


def normalize_output_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[OUTPUT_COLUMNS]
    df = df.fillna("")

    return df


def read_existing_output(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    try:
        df = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return normalize_output_df(df)


def existing_key_sets(existing_df: pd.DataFrame) -> tuple[set[str], set[str]]:
    game_ids: set[str] = set()
    team_keys: set[str] = set()

    if existing_df.empty:
        return game_ids, team_keys

    for _, row in existing_df.iterrows():
        game_id = str(row.get("game_id", "")).strip()
        game_date = str(row.get("game_date", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        home_team = str(row.get("home_team", "")).strip()

        if game_id:
            game_ids.add(game_id)

        if game_date and away_team and home_team:
            team_keys.add(f"{game_date}|{norm_key(away_team)}|{norm_key(home_team)}")

    return game_ids, team_keys


def row_exists_in_output(row: pd.Series, existing_game_ids: set[str], existing_team_keys: set[str]) -> bool:
    game_id = str(row.get("game_id", "")).strip()
    game_date = str(row.get("game_date", "")).strip()
    away_team = str(row.get("away_team", "")).strip()
    home_team = str(row.get("home_team", "")).strip()

    if game_id and game_id in existing_game_ids:
        return True

    team_key = f"{game_date}|{norm_key(away_team)}|{norm_key(home_team)}"
    if team_key in existing_team_keys:
        return True

    return False


def write_final_scores_by_date(df: pd.DataFrame) -> dict[str, int]:
    stats = {
        "rows_added": 0,
        "rows_skipped_existing": 0,
        "files_written": 0,
    }

    if df.empty:
        return stats

    for game_date, date_df in df.groupby("game_date", dropna=False):
        out_path = OUT_DIR / f"{game_date}_{LEAGUE_OUT}_final_scores.csv"

        date_df = normalize_output_df(date_df)
        existing_df = read_existing_output(out_path)

        existing_game_ids, existing_team_keys = existing_key_sets(existing_df)

        add_rows = []

        for _, row in date_df.iterrows():
            if row_exists_in_output(row, existing_game_ids, existing_team_keys):
                stats["rows_skipped_existing"] += 1
                log(
                    f"Skipped existing final score: "
                    f"{row.get('game_date', '')} | {row.get('away_team', '')} at {row.get('home_team', '')} "
                    f"| game_id={row.get('game_id', '')}"
                )
                continue

            add_rows.append(row.to_dict())

            game_id = str(row.get("game_id", "")).strip()
            if game_id:
                existing_game_ids.add(game_id)

            team_key = (
                f"{row.get('game_date', '')}|"
                f"{norm_key(row.get('away_team', ''))}|"
                f"{norm_key(row.get('home_team', ''))}"
            )
            existing_team_keys.add(team_key)

        if not add_rows:
            log(f"No new final-score rows to add for {game_date}: {out_path}")
            continue

        additions_df = pd.DataFrame(add_rows, columns=OUTPUT_COLUMNS)
        combined_df = pd.concat([existing_df, additions_df], ignore_index=True)
        combined_df = normalize_output_df(combined_df)

        combined_df = combined_df.sort_values(
            ["game_date", "game_id", "away_team", "home_team"],
            kind="stable",
        )

        combined_df.to_csv(out_path, index=False)

        stats["rows_added"] += len(additions_df)
        stats["files_written"] += 1

        log(
            f"Wrote {out_path} | existing_rows={len(existing_df)} "
            f"| added_rows={len(additions_df)} | total_rows={len(combined_df)}"
        )

    return stats


def main() -> int:
    ensure_dirs()
    reset_log()

    log("=== transform_final_scores START ===")
    log(f"RAW_DIR={RAW_DIR}")
    log(f"GAMES_DIR={GAMES_DIR}")
    log(f"OUT_DIR={OUT_DIR}")

    if not RAW_DIR.exists():
        fail(f"Raw final-score source folder does not exist: {RAW_DIR}")

    raw_files = sorted(RAW_DIR.glob(RAW_PATTERN))
    if not raw_files:
        log(f"No raw files found matching {RAW_PATTERN} in {RAW_DIR}")
        log("=== transform_final_scores END ===")
        return 0

    all_frames: list[pd.DataFrame] = []
    total_files = 0

    for raw_path in raw_files:
        total_files += 1
        log(f"Processing raw file: {raw_path}")

        payload = load_json(raw_path)
        raw_rows = flatten_raw_payload(payload)
        df = build_final_score_rows(raw_rows)

        if not df.empty:
            all_frames.append(df)

    if all_frames:
        final_df = pd.concat(all_frames, ignore_index=True)
        final_df = final_df.drop_duplicates(
            subset=["game_date", "away_team", "home_team"],
            keep="last",
        )
    else:
        final_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    write_stats = write_final_scores_by_date(final_df)

    log(f"Files processed: {total_files}")
    log(f"New final-score rows added: {write_stats['rows_added']}")
    log(f"Existing final-score rows skipped: {write_stats['rows_skipped_existing']}")
    log(f"Output files written: {write_stats['files_written']}")
    log("=== transform_final_scores END ===")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        ensure_dirs()
        log(f"FATAL: {e}")
        print(f"transform_final_scores failed: {e}", file=sys.stderr)
        raise
