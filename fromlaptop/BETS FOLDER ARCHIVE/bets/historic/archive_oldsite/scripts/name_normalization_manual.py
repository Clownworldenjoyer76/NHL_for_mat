# scripts/name_normalization_manual.py

#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/manual/cleaned")
MAP_DIR = Path("mappings")
NO_MAP_DIR = Path("mappings/need_map")
NO_MAP_PATH = NO_MAP_DIR / "dump_no_map.csv"

ERROR_DIR = Path("docs/win/errors/02_dk_prep")
ERROR_LOG = ERROR_DIR / "name_normalization_manual.txt"

NO_MAP_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

TARGET_COLS = ("team", "opponent", "away_team", "home_team")

# =========================
# HELPERS
# =========================

def clean_string(v: str) -> str:
    """
    Normalize punctuation and spacing before mapping.
    """
    if pd.isna(v):
        return v

    v = str(v).strip()

    # Normalize unicode dashes to space
    v = v.replace("–", " ")
    v = v.replace("—", " ")

    # Replace hyphens with space
    v = v.replace("-", " ")

    # Collapse whitespace
    v = " ".join(v.split())

    return v


def load_team_map_for_league(league: str):
    # Explicit mapping rules per instruction
    if league == "ncaab_ml":
        map_path = MAP_DIR / "team_map_ncaab_moneyline.csv"
    elif league == "ncaab_ou":
        map_path = MAP_DIR / "team_map_ncaab_spreads.csv"
    elif league == "ncaab_spreads":
        map_path = MAP_DIR / "team_map_ncaab_spreads.csv"
    else:
        map_path = MAP_DIR / f"team_map_{league}.csv"

    if not map_path.exists():
        raise RuntimeError(f"No team map file found for league '{league}' at {map_path}")

    df = pd.read_csv(map_path, dtype=str)

    if df.empty:
        raise RuntimeError(f"Team map file exists but is empty: {map_path}")

    team_map = {}
    canonical_set = set()

    for _, row in df.iterrows():
        alias = clean_string(row["alias"])
        canonical = clean_string(row["canonical_team"])
        team_map[alias] = canonical
        canonical_set.add(canonical)

    return team_map, canonical_set


def normalize_team(val, team_map, canonical_set, unmapped, league):
    if pd.isna(val):
        return val

    v = clean_string(val)

    if v in canonical_set:
        return v

    if v in team_map:
        return team_map[v]

    unmapped.add((v, league))
    return v


# =========================
# MAIN
# =========================

def main():
    unmapped = set()
    files_processed = 0
    values_updated = 0

    try:
        for file_path in INPUT_DIR.glob("dk_*_*.csv"):
            df = pd.read_csv(file_path, dtype=str)

            if "league" not in df.columns:
                continue

            league = str(df["league"].iloc[0]).strip()

            team_map, canonical_set = load_team_map_for_league(league)

            updated = False

            for col in TARGET_COLS:
                if col not in df.columns:
                    continue

                new_vals = []
                for v in df[col]:
                    new_v = normalize_team(
                        v, team_map, canonical_set, unmapped, league
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

        with open(ERROR_LOG, "w", encoding="utf-8") as f:
            f.write("NAME NORMALIZATION MANUAL SUMMARY\n")
            f.write("=================================\n")
            f.write(f"Files processed: {files_processed}\n")
            f.write(f"Values updated: {values_updated}\n")
            f.write(f"Unmapped values: {len(unmapped)}\n")

    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(str(e) + "\n")
            f.write(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
