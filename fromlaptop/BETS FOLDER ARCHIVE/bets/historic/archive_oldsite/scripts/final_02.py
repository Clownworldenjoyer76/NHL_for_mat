# scripts/final_02.py

import pandas as pd
import glob
from pathlib import Path

FINAL_BASE = Path("docs/win/final/step_1")
MANUAL_BASE = Path("docs/win/manual/cleaned")

# League-specific thresholds
THRESHOLDS = {
    "nba": 0.80,
    "ncaab": 0.50,  # allow expected drops
}

FILES_PROCESSED = 0
TOTAL_ROWS = 0


def load_spread_lookup(pattern):
    files = glob.glob(str(pattern))
    if not files:
        raise RuntimeError(f"No manual files found for {pattern}")

    dfs = []
    for f in files:
        df = pd.read_csv(f)

        required = {"team", "odds"}
        if not required.issubset(df.columns):
            continue

        df["team"] = df["team"].astype(str).str.strip()
        dfs.append(df[["team", "odds"]])

    if not dfs:
        raise RuntimeError(f"Manual files missing required columns for {pattern}")

    combined = pd.concat(dfs, ignore_index=True)

    # If duplicates exist, keep last occurrence
    combined = combined.drop_duplicates(subset=["team"], keep="last")

    return combined.set_index("team")


def update_spreads(final_glob, manual_glob, league_key):
    global FILES_PROCESSED, TOTAL_ROWS

    manual = load_spread_lookup(manual_glob)

    league_rows = 0
    league_filled = 0

    for f in glob.glob(str(final_glob)):
        df = pd.read_csv(f)
        FILES_PROCESSED += 1

        required_cols = {"away_team", "home_team"}
        if not required_cols.issubset(df.columns):
            raise RuntimeError(f"{f} missing required columns")

        df["away_team"] = df["away_team"].astype(str).str.strip()
        df["home_team"] = df["home_team"].astype(str).str.strip()

        rows = len(df)
        TOTAL_ROWS += rows
        league_rows += rows

        df["dk_away_odds"] = df["away_team"].map(manual["odds"])
        df["dk_home_odds"] = df["home_team"].map(manual["odds"])

        rows_fully_filled = df[["dk_away_odds", "dk_home_odds"]].notna().all(axis=1).sum()
        league_filled += rows_fully_filled

        df.to_csv(f, index=False)

        print(
            f"Updated {f} | rows={rows} | fully_filled_rows={rows_fully_filled}"
        )

    if league_rows == 0:
        raise RuntimeError(f"{league_key}: 0 rows processed")

    coverage = league_filled / league_rows
    threshold = THRESHOLDS[league_key]

    print(
        f"{league_key.upper()} Coverage: {coverage:.2%} "
        f"(threshold {threshold:.0%})"
    )

    if coverage < threshold:
        raise RuntimeError(
            f"final_02: {league_key} coverage below threshold "
            f"({coverage:.2%} < {threshold:.0%})"
        )


def run():
    # ---------------- NBA SPREADS ----------------
    update_spreads(
        FINAL_BASE / "nba/spreads/*.csv",
        MANUAL_BASE / "dk_nba_spreads_*.csv",
        league_key="nba",
    )

    # ---------------- NCAAB SPREADS ----------------
    update_spreads(
        FINAL_BASE / "ncaab/spreads/*.csv",
        MANUAL_BASE / "dk_ncaab_spreads_*.csv",
        league_key="ncaab",
    )

    print("\n=== FINAL_02 SUMMARY ===")
    print(f"Files processed: {FILES_PROCESSED}")
    print(f"Total rows processed: {TOTAL_ROWS}")

    if FILES_PROCESSED == 0:
        raise RuntimeError("final_02: 0 files processed")

    if TOTAL_ROWS == 0:
        raise RuntimeError("final_02: 0 rows processed")


if __name__ == "__main__":
    run()
