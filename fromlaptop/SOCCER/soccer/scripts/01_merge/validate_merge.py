#!/usr/bin/env python3
# docs/win/soccer/scripts/01_merge/validate_merge.py

import sys
import csv
from pathlib import Path
from datetime import datetime

# =========================
# ARGS
# =========================

if len(sys.argv) != 2:
    print("Usage: validate_merge.py YYYY_MM_DD or soccer_YYYY_MM_DD")
    sys.exit(0)

raw_input = sys.argv[1].strip()

# Accept BOTH formats:
# 2026_03_07
# soccer_2026_03_07

if raw_input.startswith("soccer_"):
    slate_date = raw_input.replace("soccer_", "", 1)
else:
    slate_date = raw_input

# =========================
# PATHS
# =========================

MERGE_FILE = Path(f"docs/win/soccer/01_merge/soccer_{slate_date}.csv")

ERROR_DIR = Path("docs/win/soccer/errors/01_merge")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = ERROR_DIR / "validate_merge.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# SKIP IF NO MERGE FILE
# =========================

if not MERGE_FILE.exists():
    log(f"No merge file found for {slate_date}. Skipping validation.")
    print(f"No soccer slate found for {slate_date}. Skipping validation.")
    sys.exit(0)

# =========================
# VALIDATION
# =========================

required_fields = [
    "league","market","match_date","match_time",
    "home_team","away_team",
    "home_prob","draw_prob","away_prob",
    "dk_home_american","dk_draw_american","dk_away_american",
    "game_id"
]

optional_fields = [
    "home_xg",
    "away_xg",
    "expected_total_goals"
]

rows_checked = 0
errors = 0
game_ids = set()

with open(MERGE_FILE, newline="", encoding="utf-8") as f:

    reader = csv.DictReader(f)

    for field in required_fields:
        if field not in reader.fieldnames:
            msg = (
                f"ERROR in file: {MERGE_FILE}\n"
                f"Missing column: {field}\n"
                f"Available columns: {reader.fieldnames}"
            )
            log(msg)
            print(msg)
            sys.exit(1)

    has_xg = all(f in reader.fieldnames for f in optional_fields)

    for r in reader:

        rows_checked += 1

        try:
            hp = float(r["home_prob"])
            dp = float(r["draw_prob"])
            ap = float(r["away_prob"])
        except:
            log(f"ERROR: Invalid probability format row {rows_checked}")
            errors += 1
            continue

        total = hp + dp + ap

        if abs(total - 1.0) > 0.02:
            log(f"ERROR: Prob sum != 1.0 ({total}) row {rows_checked}")
            errors += 1

        if has_xg:

            hxg = r.get("home_xg","")
            axg = r.get("away_xg","")
            txg = r.get("expected_total_goals","")

            if hxg and axg and txg:

                try:
                    hxg = float(hxg)
                    axg = float(axg)
                    txg = float(txg)

                    if abs((hxg + axg) - txg) > 0.25:
                        log(f"WARNING: xG mismatch row {rows_checked}")

                except:
                    log(f"WARNING: invalid xG row {rows_checked}")

        gid = r["game_id"]

        if gid in game_ids:
            log(f"ERROR: Duplicate game_id {gid}")
            errors += 1

        game_ids.add(gid)

# =========================
# RESULT
# =========================

if errors > 0:
    log(f"FAILED: {errors} errors detected")
    sys.exit(1)

log(f"SUCCESS: rows_checked={rows_checked}")

print(f"Validation passed for {MERGE_FILE}")
