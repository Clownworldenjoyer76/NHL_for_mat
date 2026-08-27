#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/00_intake/odds_name_normalization.py

import csv
import re
import traceback
import unicodedata
from pathlib import Path
from datetime import datetime

SPORTSBOOK_DIR = Path("docs/win/hockey/nhl/00_intake/sportsbook")
MAP_FILE = Path("docs/win/hockey/nhl/config/mapping/team_map_nhl.csv")
NO_MAP_FILE = Path("docs/win/hockey/nhl/config/mapping/no_map_nhl_odds.csv")

ERROR_DIR = Path("docs/win/hockey/nhl/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "odds_name_normalization.txt"

NO_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== odds_name_normalization RUN {datetime.utcnow().isoformat()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")


def normalize_alias_key(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value).strip())
    text = "".join(
        char for char in text
        if not unicodedata.combining(char)
    )
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def load_team_map(source: str) -> dict[str, dict[str, str]]:
    if not MAP_FILE.exists():
        raise FileNotFoundError(
            f"team_map_nhl.csv not found: {MAP_FILE}"
        )

    mapping: dict[str, dict[str, str]] = {}
    identity_by_id: dict[str, tuple[str, str]] = {}
    allowed_sources = {source, "shared", "official_nhl"}

    with open(MAP_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        required = {
            "league",
            "source",
            "alias",
            "canonical_team",
            "nhl_team_id",
            "nhl_abbrev",
        }
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(required - fieldnames)

        if missing:
            raise ValueError(
                f"{MAP_FILE} missing required columns: {missing}"
            )

        for row_number, row in enumerate(reader, start=2):
            league = str(row.get("league", "")).strip().lower()
            row_source = str(row.get("source", "")).strip().lower()

            if league != "nhl" or row_source not in allowed_sources:
                continue

            alias = str(row.get("alias", "")).strip()
            canonical = str(row.get("canonical_team", "")).strip()
            team_id = str(row.get("nhl_team_id", "")).strip()
            abbrev = str(row.get("nhl_abbrev", "")).strip().upper()

            if not alias or not canonical:
                continue

            if canonical != "TBD":
                if not team_id or not team_id.isdigit():
                    raise ValueError(
                        f"{MAP_FILE} row {row_number} has invalid "
                        f"nhl_team_id={team_id!r}"
                    )

                if not re.fullmatch(r"[A-Z]{3}", abbrev):
                    raise ValueError(
                        f"{MAP_FILE} row {row_number} has invalid "
                        f"nhl_abbrev={abbrev!r}"
                    )

                prior_identity = identity_by_id.get(team_id)
                identity_value = (canonical, abbrev)

                if (
                    prior_identity is not None
                    and prior_identity != identity_value
                ):
                    raise ValueError(
                        f"{MAP_FILE} has conflicting identity for "
                        f"nhl_team_id={team_id}: "
                        f"{prior_identity} != {identity_value}"
                    )

                identity_by_id[team_id] = identity_value

            identity = {
                "canonical_team": canonical,
                "nhl_team_id": team_id,
                "nhl_abbrev": abbrev,
            }

            key = normalize_alias_key(alias)
            prior = mapping.get(key)

            if prior is not None and prior != identity:
                raise ValueError(
                    f"{MAP_FILE} has conflicting {source} mapping for "
                    f"alias={alias!r}: {prior} != {identity}"
                )

            mapping[key] = identity

    if not mapping:
        raise ValueError(
            f"No NHL mappings loaded for source={source} from {MAP_FILE}"
        )

    stable_ids = {
        identity["nhl_team_id"]
        for identity in mapping.values()
        if identity["nhl_team_id"]
    }

    log(
        f"Team map loaded: {len(mapping)} aliases | "
        f"source={source} | stable_ids={len(stable_ids)}"
    )

    return mapping


team_map = load_team_map("sportsbook")

target_files = sorted(SPORTSBOOK_DIR.glob("NHL_*.csv"))
log(f"Files to process: {len(target_files)}")

unmapped = set()
files_processed = 0
rows_processed = 0
names_normalized = 0

try:
    for csv_file in target_files:
        try:
            files_processed += 1
            updated_rows = []
            modified = False

            with open(csv_file, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []

                for row in reader:
                    rows_processed += 1

                    for col in ["home_team", "away_team"]:
                        team = row.get(col, "").strip()

                        if not team:
                            continue

                        identity = team_map.get(
                            normalize_alias_key(team)
                        )

                        if identity:
                            canonical = identity["canonical_team"]

                            if row.get(col) != canonical:
                                row[col] = canonical
                                modified = True
                                names_normalized += 1
                        else:
                            unmapped.add(team)

                    updated_rows.append(row)

            if modified and fieldnames:
                with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(updated_rows)

                log(f"UPDATED: {csv_file}")

        except Exception as e:
            log(f"ERROR processing {csv_file}: {e}\n{traceback.format_exc()}")

    with open(NO_MAP_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["league", "team"])

        for team in sorted(unmapped):
            writer.writerow(["nhl", team])

    log("--- SUMMARY ---")
    log(f"Files processed: {files_processed}")
    log(f"Rows processed: {rows_processed}")
    log(f"Names normalized: {names_normalized}")
    log(f"Unmapped teams: {len(unmapped)}")
    log(f"No-map output: {NO_MAP_FILE}")
    log("STATUS: SUCCESS")

except Exception as e:
    log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
    log("STATUS: FAILED")
    raise

print("NHL odds name normalization complete.")
