#!/usr/bin/env python3
# docs/win/soccer/scripts/00_parsing/name_normalization.py

import csv
from pathlib import Path
from datetime import datetime

INTAKE_DIR = Path("docs/win/soccer/00_intake")
MAP_FILE = Path("mappings/soccer/team_map_soccer.csv")

NO_MAP_DIR = Path("mappings/soccer/no_map")
NO_MAP_DIR.mkdir(parents=True, exist_ok=True)
NO_MAP_FILE = NO_MAP_DIR / "no_map_soccer.csv"

ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "name_normalization_log.txt"

# Overwrite log each run
with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# LOAD TEAM MAP (CASE INSENSITIVE)
# =========================

team_map = {}
if MAP_FILE.exists():
    with open(MAP_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row.get("market", "").strip().lower(),
                row.get("alias", "").strip().lower(),
            )
            canonical = row.get("canonical_team", "").strip()
            if key[0] and key[1] and canonical:
                team_map[key] = canonical
else:
    log("WARNING: team_map_soccer.csv not found")

# =========================
# PROCESS FILES
# =========================

unmapped = set()  # (market_lower, team_original)
files_processed = 0
rows_processed = 0
rows_updated = 0

for csv_file in INTAKE_DIR.rglob("*.csv"):
    files_processed += 1
    updated_rows = []
    modified = False

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        for row in reader:
            rows_processed += 1
            market = row.get("market", "").strip().lower()

            for side in ["home_team", "away_team"]:
                team = row.get(side, "").strip()
                if not team:
                    continue

                key = (market, team.lower())
                if key in team_map:
                    canonical = team_map[key]
                    if row.get(side, "") != canonical:
                        row[side] = canonical
                        modified = True
                        rows_updated += 1
                else:
                    unmapped.add((market, team))

            updated_rows.append(row)

    if modified and fieldnames:
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(updated_rows)

# =========================
# WRITE UNMAPPED (AND COUNT NEW)
# =========================

existing = set()  # (market_lower, team_original)

if NO_MAP_FILE.exists():
    with open(NO_MAP_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # tolerate wrong/empty headers
        if reader.fieldnames and ("market" in reader.fieldnames) and ("team" in reader.fieldnames):
            for row in reader:
                m = (row.get("market") or "").strip().lower()
                t = (row.get("team") or "").strip()
                if m and t:
                    existing.add((m, t))

new_only = unmapped - existing
combined = existing | unmapped

with open(NO_MAP_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["market", "team"])
    for market, team in sorted(combined):
        writer.writerow([market, team])

# =========================
# LOG SUMMARY
# =========================

log(
    f"SUMMARY: files_processed={files_processed}, "
    f"rows_processed={rows_processed}, "
    f"rows_updated={rows_updated}, "
    f"unmapped_found={len(unmapped)}, "
    f"unmapped_new_added={len(new_only)}"
)

print("Name normalization complete.")
