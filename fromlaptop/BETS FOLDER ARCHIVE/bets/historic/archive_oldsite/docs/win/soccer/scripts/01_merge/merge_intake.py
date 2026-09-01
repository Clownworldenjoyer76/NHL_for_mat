# docs/win/soccer/scripts/01_merge/merge_intake.py

#!/usr/bin/env python3

import sys
import csv
from pathlib import Path
from datetime import datetime

# =========================
# ARGS
# =========================

if len(sys.argv) != 2:
    raise ValueError("Usage: merge_intake.py YYYY_MM_DD")

slate_date = sys.argv[1].strip()

# =========================
# PATHS
# =========================

INTAKE_DIR = Path("docs/win/soccer/00_intake")
SPORTSBOOK_FILE = INTAKE_DIR / "sportsbook" / f"soccer_{slate_date}.csv"
PRED_FILE = INTAKE_DIR / "predictions" / f"soccer_{slate_date}.csv"

MERGE_DIR = Path("docs/win/soccer/01_merge")
MERGE_DIR.mkdir(parents=True, exist_ok=True)
OUTFILE = MERGE_DIR / f"soccer_{slate_date}.csv"

ERROR_DIR = Path("docs/win/soccer/errors/01_merge")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "merge_intake.txt"

# reset log each run
with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# SAFE INPUT VALIDATION
# =========================

if not SPORTSBOOK_FILE.exists() or not PRED_FILE.exists():
    log(f"No soccer slate found for {slate_date}. Skipping merge.")
    print(f"No soccer slate found for {slate_date}. Skipping.")
    sys.exit(0)

# =========================
# LOAD + DEDUPE INTAKE
# =========================

def load_dedupe(path, key_fields):
    data = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            key = tuple(r[k] for k in key_fields)
            data[key] = r  # overwrite duplicates
    return data

pred_key_fields = ["match_date", "market", "home_team", "away_team"]
dk_key_fields = ["match_date", "market", "home_team", "away_team"]

pred_data = load_dedupe(PRED_FILE, pred_key_fields)
dk_data = load_dedupe(SPORTSBOOK_FILE, dk_key_fields)

# =========================
# MERGE
# =========================

FIELDNAMES = [
    "league","market","match_date","match_time",
    "home_team","away_team",
    "home_prob","draw_prob","away_prob",
    "home_american","draw_american","away_american",
    "game_id"
]

merged_rows = {}

for key, p in pred_data.items():
    if key not in dk_data:
        continue

    d = dk_data[key]

    game_id = f"{p['match_date']}_{p['home_team']}_{p['away_team']}"

    merged_rows[key] = {
        "league": p["league"],
        "market": p["market"],
        "match_date": p["match_date"],
        "match_time": p["match_time"],
        "home_team": p["home_team"],
        "away_team": p["away_team"],
        "home_prob": p["home_prob"],
        "draw_prob": p["draw_prob"],
        "away_prob": p["away_prob"],
        "home_american": d["dk_home_american"],
        "draw_american": d["dk_draw_american"],
        "away_american": d["dk_away_american"],
        "game_id": game_id,
    }

# If nothing matched, skip writing
if not merged_rows:
    log(f"No matching rows to merge for slate {slate_date}.")
    print(f"No matching rows to merge for slate {slate_date}.")
    sys.exit(0)

# =========================
# LOAD EXISTING (HEADER VALIDATION)
# =========================

existing = {}

if OUTFILE.exists():
    with open(OUTFILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames != FIELDNAMES:
            log("WARNING: Invalid header detected. Rebuilding clean.")
        else:
            for r in reader:
                key = (
                    r["match_date"],
                    r["market"],
                    r["home_team"],
                    r["away_team"],
                )
                existing[key] = r

# =========================
# UPSERT
# =========================

for key, row in merged_rows.items():
    existing[key] = row

# =========================
# ATOMIC WRITE
# =========================

temp_file = OUTFILE.with_suffix(".tmp")

with open(temp_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    for r in existing.values():
        writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

temp_file.replace(OUTFILE)

log(f"SUMMARY: merged {len(merged_rows)} rows for slate {slate_date}")
print(f"Wrote {OUTFILE}")
