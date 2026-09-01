# scripts/dk_02.py
#!/usr/bin/env python3

import csv
from pathlib import Path
import re
import sys
import os

# =========================
# FIX IMPORT PATH
# =========================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, ROOT_DIR)

from scripts.name_normalization import (
    load_team_maps,
    normalize_value,
)

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/manual/cleaned")

ERROR_DIR = Path("docs/win/errors/02_dk_prep")
ERROR_LOG = ERROR_DIR / "dk_02_game_id.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOAD TEAM MAPS (ONCE)
# =========================

team_map, canonical_sets = load_team_maps()
unmapped = set()

# ---- DEBUG CHECK ----
print("---- DEBUG CHECK ----")
print("Hawaii:", "Hawaii" in team_map.get("ncaab", {}))
print("Louisiana:", "Louisiana" in team_map.get("ncaab", {}))
print("South Alabama:", "South Alabama" in team_map.get("ncaab", {}))
print("USC Upstate:", "USC Upstate" in team_map.get("ncaab", {}))
print("Total ncaab aliases loaded:", len(team_map.get("ncaab", {})))
print("---------------------")

# =========================
# REGEX
# =========================

MD_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})\s*$")
YMD_UNDERSCORE_RE = re.compile(r"^\d{4}_\d{2}_\d{2}$")
YMD_DASH_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# =========================
# HELPERS
# =========================

def log(msg: str):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def extract_year_from_filename(path: Path) -> str:
    parts = path.stem.split("_")
    if len(parts) < 4 or not parts[-3].isdigit():
        raise ValueError(f"Cannot extract year from filename: {path.name}")
    return parts[-3]

def normalize_date(md: str, year: str):
    if md is None:
        return md, False, False

    s = str(md).strip()

    # Already normalized YYYY_MM_DD
    if YMD_UNDERSCORE_RE.match(s):
        return s, False, False

    # Handle YYYY-MM-DD → convert to YYYY_MM_DD
    if YMD_DASH_RE.match(s):
        return s.replace("-", "_"), True, False

    # Handle MM/DD → convert to YYYY_MM_DD
    m = MD_RE.match(s)
    if m:
        month, day = m.groups()
        return f"{year}_{month.zfill(2)}_{day.zfill(2)}", True, False

    # Unrecognized format
    return s, False, True

# =========================
# CORE
# =========================

def process_file(path: Path):
    try:
        year = extract_year_from_filename(path)

        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []

        if "game_id" not in fieldnames:
            fieldnames.append("game_id")

        rows_seen = 0
        rows_updated = 0
        rows_unrecognized = 0
        rows_team_normalized = 0

        parts = path.stem.split("_")
        league = parts[1].lower()

        for row in rows:

            # ---- DATE FIX ----
            if "date" in row:
                raw_date = row.get("date")
                if raw_date is not None:
                    rows_seen += 1
                    new_date, updated, unrecognized = normalize_date(raw_date, year)
                    if updated:
                        row["date"] = new_date
                        rows_updated += 1
                    elif unrecognized:
                        rows_unrecognized += 1

            # ---- TEAM NORMALIZATION ----
            if "team" in row and row["team"]:
                original = row["team"]
                normalized = normalize_value(
                    original, league, team_map, canonical_sets, unmapped
                )
                if normalized != original:
                    rows_team_normalized += 1
                row["team"] = normalized

            if "opponent" in row and row["opponent"]:
                original = row["opponent"]
                normalized = normalize_value(
                    original, league, team_map, canonical_sets, unmapped
                )
                if normalized != original:
                    rows_team_normalized += 1
                row["opponent"] = normalized

            row["game_id"] = ""

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        log(
            f"{path.name} | rows={len(rows)} | "
            f"dates_seen={rows_seen} dates_fixed={rows_updated} "
            f"dates_unrecognized={rows_unrecognized} "
            f"teams_normalized={rows_team_normalized} | game_id deferred"
        )

        if unmapped:
            log("Unmapped teams:")
            for team in sorted(unmapped):
                log(f"- {team}")

    except Exception as e:
        log(f"FILE ERROR: {path.name}")
        log(str(e))
        log("-" * 80)

# =========================
# MAIN
# =========================

def main():
    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("")

    log("DK_02 START (date fix + team normalization + identity deferred)")

    for path in INPUT_DIR.glob("dk_*_*.csv"):
        process_file(path)

    log("DK_02 END\n")

if __name__ == "__main__":
    main()
