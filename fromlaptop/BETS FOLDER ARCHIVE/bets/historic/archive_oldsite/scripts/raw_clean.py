# scripts/raw_clean.py

import os
import sys
import pandas as pd
import glob
from datetime import datetime
import traceback

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

INPUT_DIR = "docs/win/dump/csvs/"
OUTPUT_DIR = "docs/win/dump/csvs/cleaned/"
GAMES_MASTER_DIR = "docs/win/games_master/"
ERROR_DIR = "docs/win/errors/01_raw/"
ERROR_LOG = os.path.join(ERROR_DIR, "raw_clean.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(GAMES_MASTER_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)

###################################
# LOAD TEAM MAPS (ONCE)
###################################

team_map, canonical_sets = load_team_maps()
unmapped = set()

###################################
# ODDS HELPERS
###################################

def conv_american(dec):
    if dec <= 1.001:
        return 0
    if dec >= 2.0:
        return int((dec - 1) * 100)
    return int(-100 / (dec - 1))

###################################
# MAIN PROCESS
###################################

def process_files():

    # Always start with fresh log
    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("RAW CLEAN SUMMARY\n")
        f.write("=================\n")

    bad_date = 0
    bad_teams = 0
    bad_probs = 0
    crash_error = None

    try:
        files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
        all_games_master_rows = []

        for file_path in files:
            if OUTPUT_DIR in file_path:
                continue

            filename = os.path.basename(file_path)
            raw_league = filename.split("_")[0].lower()
            league = raw_league

            df = pd.read_csv(file_path)
            processed_data = []

            for _, row in df.iterrows():

                time_parts = str(row.get("Time", "")).split("\n")
                raw_date = time_parts[0].strip()

                try:
                    dt_obj = datetime.strptime(raw_date, "%m/%d/%Y")
                    f_date = dt_obj.strftime("%Y_%m_%d")
                except Exception:
                    bad_date += 1
                    continue

                team_parts = str(row.get("Teams", "")).split("\n")
                if len(team_parts) < 2:
                    bad_teams += 1
                    continue

                away_raw = team_parts[0].split("(")[0].strip()
                home_raw = team_parts[1].split("(")[0].strip()

                lg = base_league(league)

                away_team = normalize_value(
                    away_raw, lg, team_map, canonical_sets, unmapped
                )
                home_team = normalize_value(
                    home_raw, lg, team_map, canonical_sets, unmapped
                )

                game_id = f"{league}_{f_date}_{away_team}_{home_team}".replace(" ", "_")

                all_games_master_rows.append({
                    "date": f_date,
                    "league": league,
                    "game_id": game_id,
                    "away_team": away_team,
                    "home_team": home_team,
                })

                win_parts = str(row.get("Win", "")).replace("%", "").split("\n")
                try:
                    p_away_pct = float(win_parts[0])
                    p_home_pct = float(win_parts[1])
                except Exception:
                    bad_probs += 1
                    continue

                entry = {
                    "date": f_date,
                    "league": league,
                    "game_id": game_id,
                    "away_team": away_team,
                    "home_team": home_team,
                    "away_team_moneyline_win_prob": round(p_away_pct / 100, 4),
                    "home_team_moneyline_win_prob": round(p_home_pct / 100, 4),
                }

                try:
                    if raw_league in ("nba", "ncaab"):
                        score_parts = str(row.get("Points", "")).split("\n")
                        if len(score_parts) < 2:
                            continue
                        entry["away_team_projected_points"] = float(score_parts[0])
                        entry["home_team_projected_points"] = float(score_parts[1])
                        entry["game_projected_points"] = round(
                            entry["away_team_projected_points"] +
                            entry["home_team_projected_points"], 2
                        )
                    else:
                        score_parts = str(row.get("Goals", "")).split("\n")
                        if len(score_parts) < 2:
                            continue
                        entry["away_team_projected_goals"] = float(score_parts[0])
                        entry["home_team_projected_goals"] = float(score_parts[1])
                        entry["game_projected_goals"] = round(
                            entry["away_team_projected_goals"] +
                            entry["home_team_projected_goals"], 2
                        )
                except Exception:
                    continue

                dec_away = round(1 / (p_away_pct / 100), 4)

                entry["fair_decimal_odds"] = dec_away
                entry["fair_american_odds"] = conv_american(dec_away)
                entry["acceptable_decimal_odds"] = dec_away
                entry["acceptable_american_odds"] = conv_american(dec_away)

                processed_data.append(entry)

            if processed_data:
                out_df = pd.DataFrame(processed_data)
                for d_val, d_grp in out_df.groupby("date"):
                    out_path = os.path.join(OUTPUT_DIR, f"{league}_{d_val}.csv")
                    d_grp.to_csv(out_path, index=False)

        if not all_games_master_rows:
            raise RuntimeError("No games written to games_master")

        gm_df = pd.DataFrame(all_games_master_rows).drop_duplicates()

        for d_val, d_grp in gm_df.groupby("date"):
            gm_path = os.path.join(GAMES_MASTER_DIR, f"games_{d_val}.csv")
            d_grp.to_csv(gm_path, index=False)

    except Exception as e:
        crash_error = traceback.format_exc()

    # Always write summary
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"Bad dates: {bad_date}\n")
        f.write(f"Bad teams: {bad_teams}\n")
        f.write(f"Bad win probabilities: {bad_probs}\n\n")

        if unmapped:
            f.write("Unmapped teams:\n")
            for team in sorted(unmapped):
                f.write(f"- {team}\n")

        if crash_error:
            f.write("\nCRASH DETECTED\n")
            f.write("----------------\n")
            f.write(crash_error)

if __name__ == "__main__":
    process_files()
