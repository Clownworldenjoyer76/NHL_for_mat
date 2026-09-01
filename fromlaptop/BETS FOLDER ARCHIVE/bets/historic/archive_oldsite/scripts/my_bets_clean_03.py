# scripts/my_bets_clean_03.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_02")
OUTPUT_DIR = Path("docs/win/my_bets/step_03")
ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_03.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def format_date(series):
    """
    Convert YYYY-MM-DD → YYYY_MM_DD
    """
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y_%m_%d")


# =========================
# MAIN
# =========================

def process_files():
    files = glob.glob(str(INPUT_DIR / "juiceReelBets_*.csv"))

    files_processed = 0
    rows_in_total = 0
    rows_out_total = 0

    try:
        for file_path in files:
            df = pd.read_csv(file_path)
            rows_in = len(df)
            rows_in_total += rows_in

            # =========================
            # TRANSFORMATIONS
            # =========================

            # Date format update
            df["date"] = format_date(df["date"])

            # Create league column
            df["league"] = df["leg_league"].astype(str) + "_" + df["leg_type"].astype(str)

            # Blank required fields
            df["time"] = ""
            df["game_id"] = ""

            # Create empty betting metric columns
            new_blank_columns = [
                "home_ml_edge",
                "away_ml_edge",
                "away_ml_odds",
                "home_ml_odds",
                "away_spread",
                "home_spread",
                "home_spread_edge",
                "away_spread_edge",
                "away_spread_odds",
                "home_spread_odds",
                "over_edge",
                "under_edge",
                "over_odds",
                "under_odds",
                "total",
                "bet",
            ]

            for col in new_blank_columns:
                df[col] = ""

            # =========================
            # BUILD OUTPUT (STRICT ORDER)
            # =========================

            output_columns = [
                "date",
                "time",
                "game_id",
                "risk_amount",
                "max_potential_win",
                "bet_result",
                "amount_won_or_lost",
                "odds_american",
                "clv_percent",
                "leg_type",
                "bet_on",
                "bet_on_spread_total_number",
                "leg_sport",
                "leg_league",
                "leg_vig",
                "away_team",
                "home_team",
                "league",
                "home_ml_edge",
                "away_ml_edge",
                "away_ml_odds",
                "home_ml_odds",
                "away_spread",
                "home_spread",
                "home_spread_edge",
                "away_spread_edge",
                "away_spread_odds",
                "home_spread_odds",
                "over_edge",
                "under_edge",
                "over_odds",
                "under_odds",
                "total",
                "bet",
            ]

            out = df[output_columns].copy()

            # =========================
            # WRITE OUTPUT
            # =========================

            output_path = OUTPUT_DIR / Path(file_path).name
            out.to_csv(output_path, index=False)

            rows_out_total += len(out)
            files_processed += 1

        # =========================
        # WRITE SUMMARY LOG (OVERWRITE)
        # =========================

        with open(ERROR_LOG, "w") as log:
            log.write("MY_BETS_CLEAN_03 SUMMARY\n")
            log.write("=========================\n\n")
            log.write(f"Files processed: {files_processed}\n")
            log.write(f"Rows in: {rows_in_total}\n")
            log.write(f"Rows out: {rows_out_total}\n")

    except Exception as e:
        with open(ERROR_LOG, "w") as log:
            log.write("ERROR DURING PROCESSING\n")
            log.write(str(e) + "\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    process_files()
