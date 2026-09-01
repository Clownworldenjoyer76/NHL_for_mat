#!/usr/bin/env python3
# docs/win/soccer/scripts/01_merge/merge_intake.py

import sys
import csv
from pathlib import Path
from datetime import datetime
import re

# =========================
# PATHS
# =========================
INTAKE_DIR = Path("docs/win/soccer/00_intake")
SPORTSBOOK_DIR = INTAKE_DIR / "sportsbook" / "combined"
PRED_DIR = INTAKE_DIR / "predictions" / "combined"

MERGE_DIR = Path("docs/win/soccer/01_merge")
MERGE_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/soccer/errors/01_merge")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "merge_intake.txt"

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

def normalize_id(text):
    text = text.lower().replace("&", "and")
    text = re.sub(r"\s+", "_", text.strip())
    text = re.sub(r"[^a-z0-9_]", "", text)
    return text

# =========================
# FIELDNAMES (UPDATED - RAW DK FIELDS)
# =========================
FIELDNAMES = [
    "league","market","match_date","match_time",
    "home_team","away_team",
    "home_prob","draw_prob","away_prob",
    "home_xg","away_xg","expected_total_goals",

    # Raw sportsbook fields (no renaming)
    "dk_home_american","dk_draw_american","dk_away_american",
    "dk_over25_american","dk_under25_american",
    "dk_over35_american","dk_under35_american",
    "dk_btts_yes_american","dk_btts_no_american",

    "game_id"
]

# =========================
# PROCESS SLATES
# =========================
prediction_files = list(PRED_DIR.glob("soccer_*.csv"))

for pred_file in prediction_files:
    slate_date = pred_file.stem.replace("soccer_", "")
    SPORTSBOOK_FILE = SPORTSBOOK_DIR / f"soccer_{slate_date}.csv"
    OUTFILE = MERGE_DIR / f"soccer_{slate_date}.csv"

    if not SPORTSBOOK_FILE.exists():
        continue

    pred_key_fields = ["match_date", "market", "home_team", "away_team"]
    dk_key_fields = ["match_date", "market", "home_team", "away_team"]

    pred_data = load_dedupe(pred_file, pred_key_fields)
    dk_data = load_dedupe(SPORTSBOOK_FILE, dk_key_fields)

    merged_rows = {}

    for key, p in pred_data.items():
        if key not in dk_data:
            continue

        d = dk_data[key]
        home_id = normalize_id(p["home_team"])
        away_id = normalize_id(p["away_team"])
        game_id = f"{p['match_date']}_{home_id}_{away_id}"

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
            "home_xg": p.get("home_xg",""),
            "away_xg": p.get("away_xg",""),
            "expected_total_goals": p.get("expected_total_goals",""),

            # Raw sportsbook passthrough (NO fallback)
            "dk_home_american": d.get("dk_home_american",""),
            "dk_draw_american": d.get("dk_draw_american",""),
            "dk_away_american": d.get("dk_away_american",""),

            "dk_over25_american": d.get("dk_over25_american",""),
            "dk_under25_american": d.get("dk_under25_american",""),
            "dk_over35_american": d.get("dk_over35_american",""),
            "dk_under35_american": d.get("dk_under35_american",""),

            "dk_btts_yes_american": d.get("dk_btts_yes_american",""),
            "dk_btts_no_american": d.get("dk_btts_no_american",""),

            "game_id": game_id,
        }

    # =========================
    # ATOMIC WRITE
    # =========================
    temp_file = OUTFILE.with_suffix(".tmp")
    with open(temp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in merged_rows.values():
            writer.writerow(r)

    temp_file.replace(OUTFILE)
    print(f"Wrote {OUTFILE}")
