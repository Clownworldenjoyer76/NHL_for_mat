# scripts/name_normalization_dump.py

#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import traceback
import os
import sys

# -------------------------
# FIX IMPORT PATH
# -------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, ROOT_DIR)

from scripts.name_normalization import (
    load_team_maps,
    normalize_value,
    base_league,
)

INPUT_DIR = Path("docs/win/dump/csvs/cleaned")
MAP_DIR = Path("mappings")
NO_MAP_DIR = Path("mappings/need_map")
NO_MAP_PATH = NO_MAP_DIR / "dump_no_map.csv"

ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "name_normalization_dump.txt"

NO_MAP_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# LOAD TEAM MAPS (ONCE)
# -------------------------
team_map, canonical_sets = load_team_maps()

def main():

    # Always start fresh
    ERROR_LOG.write_text(
        "NAME NORMALIZATION DUMP SUMMARY\n"
        "===============================\n",
        encoding="utf-8"
    )

    unmapped = set()
    files_processed = 0
    values_updated = 0
    crash_error = None

    try:
        for file_path in INPUT_DIR.glob("*.csv"):
            df = pd.read_csv(file_path, dtype=str)

            if "league" not in df.columns:
                continue

            league = str(df["league"].iloc[0]).strip()
            lg = base_league(league)

            updated = False

            for col in ("home_team", "away_team"):
                if col not in df.columns:
                    continue

                new_vals = []
                for v in df[col]:
                    new_v = normalize_value(
                        v,
                        lg,
                        team_map,
                        canonical_sets,
                        unmapped
                    )
                    if new_v != v:
                        values_updated += 1
                        updated = True
                    new_vals.append(new_v)

                df[col] = new_vals

            if updated:
                df.to_csv(file_path, index=False)

            files_processed += 1

        if unmapped:
            pd.DataFrame(
                sorted(unmapped),
                columns=["team", "league"]
            ).to_csv(NO_MAP_PATH, index=False)

    except Exception:
        crash_error = traceback.format_exc()

    # Always append summary
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"Files processed: {files_processed}\n")
        f.write(f"Values updated: {values_updated}\n")
        f.write(f"Unmapped values: {len(unmapped)}\n")

        if crash_error:
            f.write("\nCRASH DETECTED\n")
            f.write("----------------\n")
            f.write(crash_error)

if __name__ == "__main__":
    main()
