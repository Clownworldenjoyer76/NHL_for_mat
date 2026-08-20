#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/00_intake/pred_name_normalization.py

import csv
import traceback
from pathlib import Path
from datetime import datetime


PREDICTIONS_DIR = Path("docs/win/hockey/nhl/00_intake/predictions")
MAP_FILE = Path("docs/win/hockey/nhl/config/mapping/team_map_nhl.csv")
NO_MAP_FILE = Path("docs/win/hockey/nhl/config/mapping/no_map_nhl_pred.csv")

ERROR_DIR = Path("docs/win/hockey/nhl/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "pred_name_normalization.txt"

NO_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== pred_name_normalization RUN {datetime.utcnow().isoformat()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")


team_map = {}

if MAP_FILE.exists():
    with open(MAP_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            league = row.get("league", "").strip().lower()
            alias = row.get("alias", "").strip()
            canonical = row.get("canonical_team", "").strip()

            if league == "nhl" and alias and canonical:
                team_map[alias.lower()] = canonical

    log(f"Team map loaded: {len(team_map)} entries")
else:
    log(f"WARNING: team_map_nhl.csv not found: {MAP_FILE}")


target_files = sorted(PREDICTIONS_DIR.glob("hockey_*.csv"))
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

                        canonical = team_map.get(team.lower())

                        if canonical:
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

print("NHL prediction name normalization complete.")
