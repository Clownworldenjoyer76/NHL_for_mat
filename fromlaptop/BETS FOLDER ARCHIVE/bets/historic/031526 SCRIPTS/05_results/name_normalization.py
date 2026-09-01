#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/name_normalization.py

import pandas as pd
import glob
from pathlib import Path
from datetime import datetime

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_LOG = ERROR_DIR / "name_normalization_summary.txt"

TARGET_DIRS = [
    "docs/win/basketball/04_select/daily_slate",
    "docs/win/hockey/04_select",
    "docs/win/soccer/04_select",
    "docs/win/final_scores/results/nba/final_scores",
    "docs/win/final_scores/results/ncaab/final_scores",
    "docs/win/final_scores/results/nhl/final_scores",
    "docs/win/final_scores/results/soccer/final_scores",
]

MAP_FILES = {
    "NBA": "mappings/basketball/team_map_nba.csv",
    "NCAAB": "mappings/basketball/team_map_ncaab.csv",
    "NHL": "mappings/hockey/team_map_hockey.csv",
    "SOCCER": "mappings/soccer/team_map_soccer.csv",
}

NO_MAP_FILE = Path("mappings/05_no_map/no_team_map.csv")
NO_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_maps():
    maps = {}

    for market, path in MAP_FILES.items():

        p = Path(path)

        if not p.exists():
            continue

        df = pd.read_csv(path)

        df["alias"] = df["alias"].astype(str).str.strip().str.lower()
        df["canonical_team"] = df["canonical_team"].astype(str).str.strip()

        maps[market] = dict(zip(df["alias"], df["canonical_team"]))

    return maps


def detect_market(file_path):

    name = Path(file_path).name.lower()

    if "nba" in name:
        return "NBA"
    elif "ncaab" in name:
        return "NCAAB"
    elif "nhl" in name:
        return "NHL"
    elif "soccer" in name:
        return "SOCCER"

    return None


def normalize_file(file_path, market, team_map, missing, counters):

    try:

        df = pd.read_csv(file_path)

        if "away_team" not in df.columns or "home_team" not in df.columns:
            return

        updated = False

        for col in ["away_team", "home_team"]:

            normalized = []

            for team in df[col]:

                if pd.isna(team):
                    normalized.append(team)
                    continue

                alias = str(team).strip().lower()

                if team_map and alias in team_map:

                    canonical = team_map[alias]

                    if canonical != team:
                        counters["normalized"] += 1
                        updated = True

                    normalized.append(canonical)

                else:

                    normalized.append(team)
                    missing.add((market, alias))

            df[col] = normalized

        if updated:
            df.to_csv(file_path, index=False)

    except Exception as e:

        missing.add((market, f"FILE_ERROR::{file_path}::{str(e)}"))


def write_summary(files_scanned, normalized_count, missing):

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(SUMMARY_LOG, "w", encoding="utf-8") as f:

        f.write("=== TEAM NAME NORMALIZATION SUMMARY ===\n")
        f.write(f"Timestamp: {ts}\n\n")

        f.write(f"Files scanned: {files_scanned}\n")
        f.write(f"Names normalized: {normalized_count}\n\n")

        if missing:

            f.write("Unmapped team names:\n")

            for market, alias in sorted(missing):

                if alias.startswith("FILE_ERROR::"):

                    parts = alias.split("::", 2)
                    file_path = parts[1]
                    reason = parts[2]

                    f.write(f"  FILE ERROR | {market} | {file_path} | {reason}\n")

                else:

                    f.write(
                        f"  {market} | '{alias}' | reason: alias not found in mapping file\n"
                    )

        else:

            f.write("No unmapped team names detected.\n")


def main():

    team_maps = load_maps()

    missing = set()

    counters = {"normalized": 0}

    files_scanned = 0

    for directory in TARGET_DIRS:

        files = glob.glob(f"{directory}/*.csv")

        for file_path in files:

            market = detect_market(file_path)

            if not market:
                continue

            files_scanned += 1

            team_map = team_maps.get(market)

            normalize_file(file_path, market, team_map, missing, counters)

    if missing:

        df_missing = pd.DataFrame(
            sorted(list(missing)),
            columns=["market", "alias"]
        )

        if NO_MAP_FILE.exists():

            existing = pd.read_csv(NO_MAP_FILE)

            df_missing = pd.concat([existing, df_missing]).drop_duplicates()

        df_missing.to_csv(NO_MAP_FILE, index=False)

    write_summary(files_scanned, counters["normalized"], missing)


if __name__ == "__main__":
    main()
