# scripts/my_bets_clean_02.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_01")
OUTPUT_DIR = Path("docs/win/my_bets/step_02")
ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_02.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def extract_date_from_event_start(series):
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m-%d")


def map_leg_type(value):
    if value == "GameOu":
        return "totals"
    if value == "Spread":
        return "spreads"
    if value == "Moneyline":
        return "moneyline"
    return value


def map_leg_league(value):
    if value == "CBB":
        return "ncaab"
    if value == "NBA":
        return "nba"
    if value == "NHL":
        return "nhl"
    return value


def extract_teams(description):
    """
    Extract away and home from:
    'Away Team @ Home Team - ...'
    """
    if not isinstance(description, str):
        return "", ""

    try:
        matchup_part = description.split(" - ")[0]
        if " @ " in matchup_part:
            away, home = matchup_part.split(" @ ", 1)
            return away.strip(), home.strip()
    except:
        pass

    return "", ""


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

            df["date"] = extract_date_from_event_start(df["event_start_date"])
            df["time"] = ""
            df["game_id"] = ""

            df["leg_type"] = df["leg_type"].apply(map_leg_type)
            df["leg_league"] = df["leg_league"].apply(map_leg_league)

            teams = df["leg_description"].apply(extract_teams)
            df["away_team"] = teams.apply(lambda x: x[0])
            df["home_team"] = teams.apply(lambda x: x[1])

            # =========================
            # BUILD OUTPUT
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
                "event_start_date",
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
            log.write("MY_BETS_CLEAN_02 SUMMARY\n")
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
