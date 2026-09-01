#!/usr/bin/env python3
# docs/win/soccer/scripts/soccer_cleaner.py

import csv
import re
from pathlib import Path
from datetime import datetime

SPORTSBOOK_DIR  = Path("docs/win/soccer/00_intake/sportsbook")
PREDICTIONS_DIR = Path("docs/win/soccer/00_intake/predictions")

SB_NORM_DIR   = SPORTSBOOK_DIR  / "normalized"
PRED_NORM_DIR = PREDICTIONS_DIR / "normalized"

SB_NORM_DIR.mkdir(parents=True, exist_ok=True)
PRED_NORM_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/soccer/errors/soccer_cleaner")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE  = ERROR_DIR / "soccer_cleaner_log.txt"

DATE_PAT = re.compile(r"\d{4}_\d{2}_\d{2}")

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

def normalize_league(value: str) -> str:
    return (value or "").strip().lower().replace(" ", "_")

def pct_to_decimal(value: str) -> str:
    """Convert '62.5%' or '62.5' to '0.625'."""
    if not value:
        return ""
    cleaned = value.strip().rstrip("%")
    try:
        return str(round(float(cleaned) / 100, 6))
    except ValueError:
        return value.strip()

# =========================
# SPORTSBOOK GAME_ID INDEX
# =========================
# key: (league, match_date, match_time, home_team, away_team) -> game_id

def build_game_id_index() -> dict:
    index = {}
    for sb_file in sorted(SPORTSBOOK_DIR.glob("*.csv")):
        if not DATE_PAT.search(sb_file.stem):
            continue
        if not sb_file.stem.endswith("_soccer"):
            continue
        with open(sb_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                league    = normalize_league(row.get("league", ""))
                match_date = (row.get("match_date") or "").strip()
                match_time = (row.get("match_time") or "").strip()
                home_team  = (row.get("home_team")  or "").strip()
                away_team  = (row.get("away_team")  or "").strip()
                game_id    = (row.get("game_id")    or "").strip()
                if game_id:
                    key = (league, match_date, match_time, home_team, away_team)
                    index[key] = game_id
    return index

# =========================
# 1. CLEAN SPORTSBOOK
# =========================

def clean_sportsbook():
    sb_fields = [
        "sport", "league", "game_id", "match_date", "match_time",
        "home_team", "away_team",
        "dk_home_decimal", "dk_draw_decimal", "dk_away_decimal",
        "dk_over25_decimal", "dk_under25_decimal",
        "dk_over35_decimal", "dk_under35_decimal",
        "btts_yes", "btts_no",
    ]

    for sb_file in sorted(SPORTSBOOK_DIR.glob("*.csv")):
        if not DATE_PAT.search(sb_file.stem):
            continue
        if not sb_file.stem.endswith("_soccer"):
            continue

        date_match = DATE_PAT.search(sb_file.stem)
        date_str   = date_match.group(0)

        log(f"SPORTSBOOK: processing {sb_file.name}")

        # Group rows by league
        by_league = {}
        with open(sb_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                league_raw  = (row.get("league") or "").strip()
                league_norm = normalize_league(league_raw)
                if not league_norm:
                    log(f"  SKIP row — no league value: {row}")
                    continue
                by_league.setdefault(league_norm, []).append(row)

        for league_norm, rows in by_league.items():
            out_name = f"{date_str}_{league_norm}.csv"
            out_path = SB_NORM_DIR / out_name
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=sb_fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            log(f"  WROTE {out_path} ({len(rows)} rows)")

# =========================
# 2. CLEAN PREDICTIONS
# =========================

PROB_COLS = ["home_prob", "draw_prob", "away_prob"]

PRED_FIELDS = [
    "sport", "league", "market", "game_id",
    "match_date", "match_time", "home_team", "away_team",
    "home_prob", "draw_prob", "away_prob",
    "home_xg", "away_xg", "expected_total_goals",
]

def clean_predictions(game_id_index: dict):
    for league_dir in sorted(PREDICTIONS_DIR.iterdir()):
        if not league_dir.is_dir():
            continue
        if league_dir.name == "normalized":
            continue

        league = league_dir.name

        for pred_file in sorted(league_dir.glob("*.csv")):
            if not DATE_PAT.search(pred_file.stem):
                continue
            expected_suffix = f"_{league}"
            if not pred_file.stem.endswith(expected_suffix):
                continue

            date_match = DATE_PAT.search(pred_file.stem)
            date_str   = date_match.group(0)
            league_norm = normalize_league(league)

            log(f"PREDICTIONS: processing {pred_file.name}")

            out_name = f"{date_str}_{league_norm}.csv"
            out_path = PRED_NORM_DIR / out_name

            rows_out  = []
            missing_id = 0

            with open(pred_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # A. Attach game_id from sportsbook index
                    match_date = (row.get("match_date") or "").strip()
                    match_time = (row.get("match_time") or "").strip()
                    home_team  = (row.get("home_team")  or "").strip()
                    away_team  = (row.get("away_team")  or "").strip()
                    row_league = normalize_league(row.get("league", "") or league)

                    key     = (row_league, match_date, match_time, home_team, away_team)
                    game_id = game_id_index.get(key, "")
                    if not game_id:
                        missing_id += 1
                        log(f"  NO GAME_ID match: {key}")

                    row["game_id"] = game_id

                    # B. Convert prob columns from pct to decimal
                    for col in PROB_COLS:
                        row[col] = pct_to_decimal(row.get(col, ""))

                    rows_out.append(row)

            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=PRED_FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows_out)

            log(f"  WROTE {out_path} ({len(rows_out)} rows, {missing_id} missing game_id)")

# =========================
# MAIN
# =========================

def main():
    log("=== soccer_cleaner START ===")

    clean_sportsbook()

    game_id_index = build_game_id_index()
    log(f"Game ID index built: {len(game_id_index)} entries")

    clean_predictions(game_id_index)

    log("=== soccer_cleaner COMPLETE ===")
    print("Soccer cleaner complete.")

if __name__ == "__main__":
    main()
