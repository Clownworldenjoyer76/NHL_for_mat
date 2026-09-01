# scripts/my_bets_clean_05.py

#!/usr/bin/env python3

import pandas as pd
import glob
from pathlib import Path
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_03")
GAMES_MASTER_DIR = Path("docs/win/games_master")
OUTPUT_DIR = Path("docs/win/my_bets/step_04")

ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_05.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOG (OVERWRITE ALWAYS)
# =========================

with open(ERROR_LOG, "w", encoding="utf-8") as f:
    f.write(f"=== MY_BETS_CLEAN_05 RUN @ {datetime.utcnow().isoformat()}Z ===\n")

def log(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# =========================
# LOAD GAMES MASTER LOOKUP
# =========================

def build_lookup():
    files = glob.glob(str(GAMES_MASTER_DIR / "games_*.csv"))
    records = []

    for file in files:
        try:
            df = pd.read_csv(file)
            df = df[["date", "away_team", "home_team", "game_id"]]
            records.append(df)
        except Exception as e:
            log(f"ERROR loading {file}: {e}")

    if not records:
        return {}

    all_games = pd.concat(records, ignore_index=True)

    lookup = {
        (row["date"], row["away_team"], row["home_team"]): row["game_id"]
        for _, row in all_games.iterrows()
    }

    return lookup

# =========================
# MAIN
# =========================

def process():
    lookup = build_lookup()

    input_files = glob.glob(str(INPUT_DIR / "juiceReelBets_*.csv"))

    total_files = 0
    total_rows = 0
    total_matched = 0
    total_unmatched = 0

    for file_path in input_files:
        try:
            total_files += 1
            df = pd.read_csv(file_path)
            rows = len(df)
            total_rows += rows

            # overwrite game_id using lookup
            matched = 0
            unmatched = 0

            def get_game_id(row):
                key = (row["date"], row["away_team"], row["home_team"])
                if key in lookup:
                    nonlocal matched
                    matched += 1
                    return lookup[key]
                else:
                    nonlocal unmatched
                    unmatched += 1
                    return ""

            df["game_id"] = df.apply(get_game_id, axis=1)

            total_matched += matched
            total_unmatched += unmatched

            # drop time column
            if "time" in df.columns:
                df = df.drop(columns=["time"])

            output_cols = [
                "date",
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
                "bet"
            ]

            for col in output_cols:
                if col not in df.columns:
                    df[col] = ""

            df = df[output_cols]

            output_path = OUTPUT_DIR / Path(file_path).name
            df.to_csv(output_path, index=False)

            log(f"Wrote {output_path} | rows={rows} matched={matched} unmatched={unmatched}")

        except Exception:
            log(f"ERROR processing {file_path}")
            log(traceback.format_exc())

    log(f"Files processed: {total_files}")
    log(f"Rows processed: {total_rows}")
    log(f"Rows matched: {total_matched}")
    log(f"Rows unmatched: {total_unmatched}")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    process()
