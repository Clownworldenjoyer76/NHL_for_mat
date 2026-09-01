# scripts/final_03.py

import pandas as pd
import glob
from pathlib import Path

FINAL_BASE = Path("docs/win/final/step_1")
MANUAL_BASE = Path("docs/win/manual/normalized")

# League-specific thresholds
THRESHOLDS = {
    "nba": 0.80,
    "ncaab": 0.50,  # allow expected drops
}

FILES_PROCESSED = 0
TOTAL_ROWS = 0


def load_lookup(pattern, game_col="game_id"):
    dfs = []

    for f in glob.glob(str(pattern)):
        df = pd.read_csv(f)

        if game_col not in df.columns:
            continue

        df[game_col] = df[game_col].astype(str).str.strip()
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return (
        pd.concat(dfs, ignore_index=True)
        .drop_duplicates(subset=game_col, keep="last")
        .set_index(game_col)
    )


def update_files(final_glob, manual_glob, mappings, league_key):
    global FILES_PROCESSED, TOTAL_ROWS

    manual = load_lookup(manual_glob)

    if manual.empty:
        raise RuntimeError(f"No manual data found for {manual_glob}")

    league_rows = 0
    league_filled = 0

    for f in glob.glob(str(final_glob)):
        df = pd.read_csv(f)
        FILES_PROCESSED += 1

        if "game_id" not in df.columns:
            raise RuntimeError(f"{f} missing game_id column")

        df["game_id"] = df["game_id"].astype(str).str.strip()

        rows = len(df)
        TOTAL_ROWS += rows
        league_rows += rows

        for out_col, src_col in mappings.items():
            if src_col not in manual.columns:
                raise RuntimeError(
                    f"Manual source column '{src_col}' missing in {manual_glob}"
                )
            df[out_col] = df["game_id"].map(manual[src_col])

        rows_fully_filled = len(df)
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
            f"final_03: {league_key} coverage below threshold "
            f"({coverage:.2%} < {threshold:.0%})"
        )


def run():
    update_files(
        FINAL_BASE / "nba/ml/juice_nba_ml_*.csv",
        MANUAL_BASE / "dk_nba_moneyline_*.csv",
        {
            "away_handle_pct": "away_handle_pct",
            "home_handle_pct": "home_handle_pct",
            "away_bets_pct": "away_bets_pct",
            "home_bets_pct": "home_bets_pct",
        },
        league_key="nba",
    )

    update_files(
        FINAL_BASE / "ncaab/ml/juice_ncaab_ml_*.csv",
        MANUAL_BASE / "dk_ncaab_moneyline_*.csv",
        {
            "away_handle_pct": "away_handle_pct",
            "home_handle_pct": "home_handle_pct",
            "away_bets_pct": "away_bets_pct",
            "home_bets_pct": "home_bets_pct",
        },
        league_key="ncaab",
    )

    print("\n=== FINAL_03 SUMMARY ===")
    print(f"Files processed: {FILES_PROCESSED}")
    print(f"Total rows processed: {TOTAL_ROWS}")

    if FILES_PROCESSED == 0:
        raise RuntimeError("final_03: 0 files processed")

    if TOTAL_ROWS == 0:
        raise RuntimeError("final_03: 0 rows processed")


if __name__ == "__main__":
    run()
