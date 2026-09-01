#!/usr/bin/env python3
# docs/win/basketball/scripts/01_merge/merge_intake.py

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
# CONSTANTS
# =========================

LEAGUES = ["NBA", "NCAAB"]

ROOT_DIR = Path("docs/win/basketball")
INTAKE_DIR = ROOT_DIR / "00_intake"
MERGE_DIR = ROOT_DIR / "01_merge"
ERROR_DIR = ROOT_DIR / "errors/01_merge"

MERGE_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = ERROR_DIR / "merge_intake.txt"

# reset log each run
with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")


# =========================
# HELPERS
# =========================

def load_dedupe(path, key_fields):
    data = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            key = tuple(r[k] for k in key_fields)
            data[key] = r
    return data


key_fields = ["game_date", "home_team", "away_team"]

FIELDNAMES = [
    "league",
    "market",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "game_id",

    # model
    "home_prob",
    "away_prob",
    "away_projected_points",
    "home_projected_points",
    "total_projected_points",

    # sportsbook lines
    "away_spread",
    "home_spread",
    "total",

    # sportsbook odds
    "away_dk_spread_american",
    "home_dk_spread_american",
    "dk_total_over_american",
    "dk_total_under_american",
    "away_dk_moneyline_american",
    "home_dk_moneyline_american",
]

# =========================
# PROCESS EACH LEAGUE
# =========================

for league in LEAGUES:

    PRED_FILE = INTAKE_DIR / "predictions" / f"basketball_{league}_{slate_date}.csv"
    SPORTSBOOK_FILE = INTAKE_DIR / "sportsbook" / f"basketball_{league}_{slate_date}.csv"
    OUTFILE = MERGE_DIR / f"basketball_{league}_{slate_date}.csv"

    if not PRED_FILE.exists() or not SPORTSBOOK_FILE.exists():
        log(f"No {league} slate found for {slate_date}. Skipping merge.")
        print(f"No {league} slate found for {slate_date}. Skipping.")
        continue

    pred_data = load_dedupe(PRED_FILE, key_fields)
    dk_data = load_dedupe(SPORTSBOOK_FILE, key_fields)

    merged_rows = {}

    for key, p in pred_data.items():

        if key not in dk_data:
            continue

        d = dk_data[key]

        # team validation
        if d.get("home_team") != p.get("home_team") or d.get("away_team") != p.get("away_team"):
            log(f"{league} TEAM MISMATCH: {p.get('home_team')} vs {p.get('away_team')}")
            continue

        # ✅ Make game_id consistent with explicit away_team/home_team columns
        game_id = f"{p['game_date']}_{p['away_team']}_{p['home_team']}"

        merged_rows[key] = {
            "league": p.get("league", ""),
            "market": p.get("market", ""),
            "game_date": p.get("game_date", ""),
            "game_time": p.get("game_time", ""),
            "home_team": p.get("home_team", ""),
            "away_team": p.get("away_team", ""),
            "game_id": game_id,

            # model
            "home_prob": p.get("home_prob", ""),
            "away_prob": p.get("away_prob", ""),
            "away_projected_points": p.get("away_projected_points", ""),
            "home_projected_points": p.get("home_projected_points", ""),
            "total_projected_points": p.get("total_projected_points", ""),

            # sportsbook
            "away_spread": d.get("away_spread", ""),
            "home_spread": d.get("home_spread", ""),
            "total": d.get("total", ""),
            "away_dk_spread_american": d.get("away_dk_spread_american", ""),
            "home_dk_spread_american": d.get("home_dk_spread_american", ""),
            "dk_total_over_american": d.get("dk_total_over_american", ""),
            "dk_total_under_american": d.get("dk_total_under_american", ""),
            "away_dk_moneyline_american": d.get("away_dk_moneyline_american", ""),
            "home_dk_moneyline_american": d.get("home_dk_moneyline_american", ""),
        }

    if not merged_rows:
        log(f"No matching {league} rows to merge for slate {slate_date}.")
        print(f"No matching {league} rows to merge for slate {slate_date}.")
        continue

    # =========================
    # UPSERT SAFE
    # =========================

    existing = {}

    if OUTFILE.exists():
        with open(OUTFILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames == FIELDNAMES:
                for r in reader:
                    key = (
                        r["game_date"],
                        r["home_team"],
                        r["away_team"],
                    )
                    existing[key] = r
            else:
                log(f"{league} WARNING: Header mismatch detected. Rebuilding clean.")

    for key, row in merged_rows.items():
        existing[key] = row

    # =========================
    # ATOMIC WRITE
    # =========================

    temp_file = OUTFILE.with_suffix(".tmp")

    with open(temp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in sorted(existing.values(), key=lambda x: (x["game_date"], x["game_time"], x["home_team"])):
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

    temp_file.replace(OUTFILE)

    log(f"SUMMARY: merged {len(merged_rows)} {league} games for slate {slate_date}")
    print(f"Wrote {OUTFILE}")
