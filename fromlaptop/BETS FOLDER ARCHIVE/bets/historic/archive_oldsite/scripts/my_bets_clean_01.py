# scripts/my_bets_clean_01.py

#!/usr/bin/env python3

import pandas as pd
import glob
from pathlib import Path
from datetime import timedelta
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/raw")
OUTPUT_DIR = Path("docs/win/my_bets/step_01")
ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_01.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# OUTPUT HEADERS (STRICT ORDER)
# =========================

OUTPUT_COLUMNS = [
    "date",
    "game_id",
    "juice_bet_id",
    "risk_amount",
    "max_potential_win",
    "bet_result",
    "amount_won_or_lost",
    "odds_american",
    "number_of_legs",
    "date_placed",
    "clv_percent",
    "leg_type",
    "bet_on",
    "bet_on_spread_total_number",
    "leg_sport",
    "leg_league",
    "leg_vig",
    "leg_description",
    "long_description_of_leg",
    "event_start_date",
    "event_name",
]

# =========================
# MAIN
# =========================

def convert_utc_to_est(series):
    """
    Convert UTC timestamp string to EST (UTC-5).
    Hard subtract 5 hours per spec.
    """
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    dt_est = dt - timedelta(hours=5)
    return dt_est.dt.strftime("%Y-%m-%d %H:%M:%S")


def process_files():
    files = glob.glob(str(INPUT_DIR / "juiceReelBets_*.csv"))

    files_processed = 0
    rows_in_total = 0
    rows_out_total = 0
    rows_dropped_legs = 0

    for file_path in files:
        try:
            df = pd.read_csv(file_path)
            rows_in = len(df)
            rows_in_total += rows_in

            # Keep ONLY single-leg bets
            df = df[df["number_of_legs"].astype(str) == "1"].copy()
            rows_after_filter = len(df)
            rows_dropped_legs += (rows_in - rows_after_filter)

            # Convert event_start_date to EST (UTC-5)
            df["event_start_date"] = convert_utc_to_est(df["event_start_date"])

            # Build output dataframe
            out = pd.DataFrame()

            # Blank columns
            out["date"] = ""
            out["game_id"] = ""

            # Copied columns
            out["juice_bet_id"] = df["juice_bet_id"]
            out["risk_amount"] = df["risk_amount"]
            out["max_potential_win"] = df["max_potential_win"]
            out["bet_result"] = df["bet_result"]
            out["amount_won_or_lost"] = df["amount_won_or_lost"]
            out["odds_american"] = df["odds_american"]
            out["number_of_legs"] = df["number_of_legs"]
            out["date_placed"] = df["date_placed"]
            out["clv_percent"] = df["clv_percent"]
            out["leg_type"] = df["leg_type"]
            out["bet_on"] = df["bet_on"]
            out["bet_on_spread_total_number"] = df["bet_on_spread_total_number"]
            out["leg_sport"] = df["leg_sport"]
            out["leg_league"] = df["leg_league"]
            out["leg_vig"] = df["leg_vig"]
            out["leg_description"] = df["leg_description"]
            out["long_description_of_leg"] = df["long_description_of_leg"]
            out["event_start_date"] = df["event_start_date"]
            out["event_name"] = df["event_name"]

            # Enforce strict column order
            out = out[OUTPUT_COLUMNS]

            # Write output
            input_filename = Path(file_path).name
            output_path = OUTPUT_DIR / input_filename
            out.to_csv(output_path, index=False)

            rows_out_total += len(out)
            files_processed += 1

        except Exception as e:
            with open(ERROR_LOG, "w") as log:
                log.write("ERROR DURING PROCESSING\n")
                log.write(str(e) + "\n")
                log.write(traceback.format_exc())
            return

    # Write summary log (ALWAYS overwrite)
    with open(ERROR_LOG, "w") as log:
        log.write("MY_BETS_CLEAN_01 SUMMARY\n")
        log.write("=========================\n\n")
        log.write(f"Files processed: {files_processed}\n")
        log.write(f"Rows in: {rows_in_total}\n")
        log.write(f"Rows out: {rows_out_total}\n")
        log.write(f"Rows dropped (multi-leg): {rows_dropped_legs}\n")


if __name__ == "__main__":
    process_files()
