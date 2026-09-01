#!/usr/bin/env python3
# scripts/parsing_combine.py

import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/soccer/00_intake/predictions")
ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = ERROR_DIR / "parsing_combine.txt"

FIELDNAMES = [
    "league",
    "market",
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "home_prob",
    "draw_prob",
    "away_prob",
]

# =========================
# LOGGING
# =========================

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== parsing_combine run @ {datetime.utcnow().isoformat()} ===\n")

def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# =========================
# COLLECT FILES
# =========================

files = list(INPUT_DIR.glob("soccer_*_*.csv"))

if not files:
    log("No input files found.")
    raise ValueError("No soccer_{date}_{market}.csv files found.")

rows_by_date = defaultdict(list)
files_processed = 0
rows_processed = 0
rows_skipped = 0

for file_path in files:
    name = file_path.stem  # soccer_YYYY_MM_DD_market

    parts = name.split("_")
    if len(parts) < 5:
        log(f"Skipped malformed filename: {file_path.name}")
        rows_skipped += 1
        continue

    # Extract YYYY_MM_DD
    date_key = "_".join(parts[1:4])

    try:
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:

                # Validate headers
                if not all(h in row for h in FIELDNAMES):
                    rows_skipped += 1
                    continue

                rows_by_date[date_key].append({
                    "league": row["league"],
                    "market": row["market"],
                    "match_date": row["match_date"],
                    "match_time": row["match_time"],
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                    "home_prob": row["home_prob"],
                    "draw_prob": row["draw_prob"],
                    "away_prob": row["away_prob"],
                })

                rows_processed += 1

        files_processed += 1

    except Exception as e:
        log(f"Error reading {file_path.name}: {e}")
        rows_skipped += 1

# =========================
# WRITE COMBINED FILES
# =========================

if not rows_by_date:
    log("No valid rows found to combine.")
    raise ValueError("No rows combined.")

for date_key, rows in rows_by_date.items():
    outfile = INPUT_DIR / f"soccer_{date_key}.csv"

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    log(f"Wrote {outfile.name} ({len(rows)} rows)")

# =========================
# SUMMARY
# =========================

log("---- SUMMARY ----")
log(f"Files processed: {files_processed}")
log(f"Rows processed: {rows_processed}")
log(f"Rows skipped: {rows_skipped}")
log("Done.")
