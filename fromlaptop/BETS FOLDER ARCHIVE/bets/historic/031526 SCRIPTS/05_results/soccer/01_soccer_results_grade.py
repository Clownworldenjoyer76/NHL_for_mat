#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/soccer/01_soccer_results_grade.py

from datetime import datetime
from pathlib import Path
import pandas as pd

###############################################################
######################## PATH CONFIG ##########################
###############################################################

SELECT_DIR = Path("docs/win/soccer/04_select")
FINAL_SCORES_DIR = Path("docs/win/final_scores/results/soccer/final_scores")

OUTPUT_DIR = Path("docs/win/final_scores/results/soccer/graded")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

ERROR_LOG = ERROR_DIR / "soccer_results_grade_errors.txt"
SUMMARY_LOG = ERROR_DIR / "soccer_results_grade_summary.txt"

MASTER_FILE = OUTPUT_DIR / "SOCCER_final.csv"

###############################################################
######################## LOGGING ##############################
###############################################################

def reset_logs():
    ERROR_LOG.write_text("", encoding="utf-8")
    SUMMARY_LOG.write_text("", encoding="utf-8")


def log_error(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {msg}\n")


def log_summary(msg):
    with open(SUMMARY_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {msg}\n")

###############################################################
######################## HELPERS ##############################
###############################################################

def safe_read(path):

    try:
        path = Path(path)

        if not path.exists():
            log_error(f"MISSING FILE | {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df.empty:
            log_error(f"EMPTY FILE | {path}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"READ ERROR | {path} | {e}")
        return pd.DataFrame()

###############################################################
######################## GRADING ##############################
###############################################################

def grade_row(row):

    try:

        market = str(row.get("market_type","")).lower()
        take_bet = str(row.get("take_bet","")).lower()

        home = row.get("home_score")
        away = row.get("away_score")

        if pd.isna(home) or pd.isna(away):
            return "Push"

        if market == "result":

            if take_bet == "home":
                return "Win" if home > away else "Loss"

            if take_bet == "away":
                return "Win" if away > home else "Loss"

        if market == "total":

            goals = home + away
            line = 2.5

            if take_bet == "over25":
                return "Win" if goals > line else "Loss"

            if take_bet == "under25":
                return "Win" if goals < line else "Loss"

    except Exception:
        pass

    return "Push"

###############################################################
######################## PROCESS ##############################
###############################################################

def process():

    select_files = list(SELECT_DIR.glob("*.csv"))

    if not select_files:
        log_error("NO SELECT FILES FOUND")
        return

    rows = []

    for file in select_files:

        df = safe_read(file)

        if df.empty:
            continue

        scores_file = FINAL_SCORES_DIR / f"{file.stem}_final_scores_SOCCER.csv"
        scores = safe_read(scores_file)

        if scores.empty:
            log_error(f"MISSING SCORE FILE | {scores_file}")
            continue

        merged = df.merge(
            scores,
            on=["home_team","away_team","game_date"],
            how="left"
        )

        merged["bet_result"] = merged.apply(grade_row, axis=1)

        out_file = OUTPUT_DIR / f"{file.stem}_results_SOCCER.csv"
        merged.to_csv(out_file, index=False)

        rows.append(merged)

        log_summary(f"GRADED FILE | {out_file} | ROWS={len(merged)}")

    if rows:

        final = pd.concat(rows, ignore_index=True)
        final.to_csv(MASTER_FILE, index=False)

        log_summary(f"MASTER FILE WRITTEN | {MASTER_FILE} | ROWS={len(final)}")

###############################################################
######################## MAIN #################################
###############################################################

def main():

    reset_logs()
    log_summary("START soccer_results_grade.py")

    process()

    log_summary("END soccer_results_grade.py")

    print("Soccer grading complete.")

if __name__ == "__main__":
    main()
