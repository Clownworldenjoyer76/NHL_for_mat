#!/usr/bin/env python3
# docs/win/soccer/scripts/00_parsing/name_normalization.py

import csv
import re
from pathlib import Path
from datetime import datetime

INTAKE_DIR = Path("docs/win/soccer/00_intake")
MAP_FILE   = Path("mappings/soccer/team_map_soccer.csv")

NO_MAP_DIR  = Path("mappings/soccer/no_map")
NO_MAP_DIR.mkdir(parents=True, exist_ok=True)
NO_MAP_FILE = NO_MAP_DIR / "no_map_soccer.csv"

ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE  = ERROR_DIR / "name_normalization_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")


# =========================
# MARKET NORMALIZATION
# =========================

MARKET_MAP = {
    "la liga":    "laliga",
    "laliga":     "laliga",
    "epl":        "epl",
    "serie a":    "seriea",
    "seriea":     "seriea",
    "bundesliga": "bundesliga",
    "ligue 1":    "ligue1",
    "ligue1":     "ligue1",
}

def normalize_market(value: str) -> str:
    if not value:
        return ""
    return MARKET_MAP.get(value.strip().lower(), value.strip().lower())


# =========================
# LEAGUE NORMALIZATION
# =========================

def normalize_league(value: str) -> str:
    if not value:
        return value
    if value.strip().lower() == "soccer":
        return "Soccer"
    return value


# =========================
# LOAD TEAM MAP
# =========================

team_map = {}

if MAP_FILE.exists():
    with open(MAP_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            market    = normalize_market(row.get("market", ""))
            alias     = (row.get("alias") or "").strip().lower()
            canonical = (row.get("canonical_team") or "").strip()
            if market and alias and canonical:
                team_map[(market, alias)] = canonical
else:
    log("WARNING: team_map_soccer.csv not found")


# =========================
# BUILD FILE LIST
# =========================
# Target only:
#   docs/win/soccer/00_intake/sportsbook/{date}_soccer.csv
#   docs/win/soccer/00_intake/predictions/{league}/{date}_{league}.csv

DATE_PAT = re.compile(r"\d{4}_\d{2}_\d{2}")

files_to_process = []

# Sportsbook files
sb_dir = INTAKE_DIR / "sportsbook"
if sb_dir.exists():
    for f in sorted(sb_dir.glob("*.csv")):
        if DATE_PAT.search(f.stem) and f.stem.endswith("_soccer"):
            files_to_process.append(f)

# Predictions files
pred_dir = INTAKE_DIR / "predictions"
if pred_dir.exists():
    for league_dir in sorted(pred_dir.iterdir()):
        if not league_dir.is_dir():
            continue
        league = league_dir.name
        for f in sorted(league_dir.glob("*.csv")):
            # expects {date}_{league}.csv
            if DATE_PAT.search(f.stem) and f.stem.endswith(f"_{league}"):
                files_to_process.append(f)


# =========================
# PROCESS FILES
# =========================

unmapped       = set()
files_processed = 0
rows_processed  = 0
rows_updated    = 0

for csv_file in files_to_process:

    files_processed += 1
    updated_rows = []
    modified     = False

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader     = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        if "home_team" not in fieldnames or "away_team" not in fieldnames:
            log(f"SKIP (no team columns): {csv_file}")
            continue

        for row in reader:
            rows_processed += 1

            if "market" in row:
                orig = row["market"]
                norm = normalize_market(orig)
                if orig != norm:
                    row["market"] = norm
                    modified = True

            if "league" in row:
                orig = row["league"]
                norm = normalize_league(orig)
                if orig != norm:
                    row["league"] = norm
                    modified = True

            market = row.get("market", "")

            for side in ["home_team", "away_team"]:
                team_raw  = (row.get(side) or "").strip()
                team_norm = team_raw.lower()

                if not team_raw:
                    continue

                key = (market, team_norm)

                if key in team_map:
                    canonical = team_map[key]
                    if row[side] != canonical:
                        row[side] = canonical
                        modified  = True
                        rows_updated += 1
                else:
                    unmapped.add((market, team_raw))

            updated_rows.append(row)

    if modified and fieldnames:
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(updated_rows)
        log(f"UPDATED: {csv_file}")


# =========================
# WRITE UNMAPPED
# =========================

with open(NO_MAP_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["market", "team"])
    for market, team in sorted(unmapped):
        writer.writerow([market, team])


# =========================
# LOG SUMMARY
# =========================

log(
    f"SUMMARY: files_processed={files_processed}, "
    f"rows_processed={rows_processed}, "
    f"rows_updated={rows_updated}, "
    f"unmapped_found={len(unmapped)}"
)

print("Name normalization complete.")
