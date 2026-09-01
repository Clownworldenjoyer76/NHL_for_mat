# docs/win/soccer/scripts/00_parsing/dedupe.py

#!/usr/bin/env python3

import csv
from pathlib import Path
from datetime import datetime

# =========================
# PATHS
# =========================

BASE_DIR = Path("docs/win/soccer/00_intake")
PRED_DIR = BASE_DIR / "predictions"
SPORTSBOOK_DIR = BASE_DIR / "sportsbook"

ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "dedupe.txt"

# overwrite log each run
with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# DEDUPE FUNCTION
# =========================

def dedupe_file(csv_file: Path):
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        if not fieldnames:
            log(f"SKIP: {csv_file} has no header")
            return

        rows = list(reader)

    # determine key fields
    required_fields = ["match_date", "market", "home_team", "away_team"]
    if not all(k in fieldnames for k in required_fields):
        log(f"SKIP: {csv_file} missing required columns")
        return

    seen = set()
    deduped = []
    duplicates = 0

    for r in rows:
        key = (
            r.get("match_date", ""),
            r.get("market", ""),
            r.get("home_team", ""),
            r.get("away_team", ""),
        )

        if key in seen:
            duplicates += 1
            continue

        seen.add(key)
        deduped.append(r)

    # atomic rewrite
    temp_file = csv_file.with_suffix(".tmp")

    with open(temp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in deduped:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    temp_file.replace(csv_file)

    log(f"{csv_file.name}: removed {duplicates} duplicates, final_rows={len(deduped)}")

# =========================
# PROCESS ALL FILES
# =========================

files_processed = 0

for directory in [PRED_DIR, SPORTSBOOK_DIR]:
    if not directory.exists():
        continue

    for csv_file in directory.glob("*.csv"):
        files_processed += 1
        dedupe_file(csv_file)

log(f"SUMMARY: files_processed={files_processed}")
print("Dedupe complete.")
