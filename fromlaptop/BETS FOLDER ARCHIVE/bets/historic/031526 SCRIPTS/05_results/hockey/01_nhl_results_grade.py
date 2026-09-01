#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/nhl_results_grade.py

import glob
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

###############################################################
######################## PATH CONFIG ##########################
###############################################################

BASE = Path("docs/win/hockey")
SELECT_DIR = BASE / "04_select/daily_slate"

NHL_SCORE_DIR = Path("docs/win/final_scores/results/nhl/final_scores")

NHL_OUTPUT = Path("docs/win/final_scores/results/nhl/graded")

INTERMEDIATE_DIR = Path("docs/win/final_scores/intermediate")
INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

GRADE_ERROR_LOG = ERROR_DIR / "nhl_results_grade_errors.txt"
GRADE_SUMMARY_LOG = ERROR_DIR / "nhl_results_grade_summary.txt"

###############################################################
######################## LOGGING ##############################
###############################################################

def reset_logs():
    GRADE_ERROR_LOG.write_text("", encoding="utf-8")
    GRADE_SUMMARY_LOG.write_text("", encoding="utf-8")


def log_error(msg):
    with open(GRADE_ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {msg}\n")


def log_summary(msg):
    with open(GRADE_SUMMARY_LOG, "a", encoding="utf-8") as f:
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

        if df is None or df.empty:
            log_error(f"EMPTY FILE | {path}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"READ ERROR | {path} | {e}")
        return pd.DataFrame()


def clear_old_outputs():
    try:
        NHL_OUTPUT.mkdir(parents=True, exist_ok=True)

        for f in NHL_OUTPUT.glob("*_results_NHL.csv"):
            f.unlink(missing_ok=True)

        for f in NHL_OUTPUT.glob("NHL_final.csv"):
            f.unlink(missing_ok=True)

        log_summary("CLEARED OLD NHL GRADED OUTPUTS")

    except Exception as e:
        log_error(f"CLEAR OUTPUT ERROR | {e}")


###############################################################
######################## OUTCOME LOGIC ########################
###############################################################

def determine_outcome(row):

    try:

        market = str(row.get("market_type", "")).strip().lower()
        side = str(row.get("bet_side", "")).strip().lower()

        away = float(row["away_score"])
        home = float(row["home_score"])

        if market == "moneyline":

            if away == home:
                return "Push"

            if side == "home":
                return "Win" if home > away else "Loss"

            if side == "away":
                return "Win" if away > home else "Loss"

        if market == "puck_line":

            line = float(row.get("line", 0))

            if side == "home":
                diff = (home + line) - away
            elif side == "away":
                diff = (away + line) - home
            else:
                return "Unknown"

            if abs(diff) < 1e-9:
                return "Push"

            return "Win" if diff > 0 else "Loss"

        if market == "total":

            line = float(row.get("line", 0))
            total = away + home

            if abs(total - line) < 1e-9:
                return "Push"

            if side == "over":
                return "Win" if total > line else "Loss"

            if side == "under":
                return "Win" if total < line else "Loss"

    except Exception as e:
        log_error(f"DETERMINE OUTCOME ERROR | {e}")

    return "Unknown"


###############################################################
######################## GRADING ##############################
###############################################################

def grade_league():

    score_dir = NHL_SCORE_DIR
    output_dir = NHL_OUTPUT
    pattern = "*_nhl.csv"
    suffix = "NHL"

    output_dir.mkdir(parents=True, exist_ok=True)

    bet_files = glob.glob(str(SELECT_DIR / pattern))

    dates = set()

    for f in bet_files:
        m = re.search(r"(\d{4}_\d{2}_\d{2})", f)
        if m:
            dates.add(m.group(1))

    for date in sorted(dates):

        try:

            score_file = score_dir / f"{date}_final_scores_{suffix}.csv"

            if not score_file.exists():
                log_error(f"SCORE FILE MISSING | {score_file}")
                continue

            bet_paths = glob.glob(str(SELECT_DIR / f"{date}_nhl.csv"))

            dfs = [safe_read(x) for x in bet_paths]
            dfs = [d for d in dfs if not d.empty]

            if not dfs:
                log_error(f"NO BET FILES | {date}")
                continue

            bets = pd.concat(dfs, ignore_index=True)

            scores = safe_read(score_file)

            if scores.empty:
                log_error(f"SCORE FILE EMPTY | {date}")
                continue

            df = pd.merge(
                bets,
                scores,
                on=["away_team", "home_team", "game_date"],
                validate="many_to_one",
            )

            df["bet_result"] = df.apply(determine_outcome, axis=1)

            outfile = output_dir / f"{date}_results_{suffix}.csv"

            df.to_csv(outfile, index=False)

            result_counts = df["bet_result"].astype(str).value_counts().to_dict()

            log_summary(
                f"NHL GRADED | DATE={date} | ROWS={len(df)} | RESULTS={result_counts}"
            )

        except Exception as e:
            log_error(f"GRADE LOOP ERROR | {date} | {e}")


def build_master():

    try:

        files = sorted(glob.glob(str(NHL_OUTPUT / "*_results_NHL.csv")))

        dfs = [safe_read(f) for f in files]
        dfs = [d for d in dfs if not d.empty]

        if not dfs:
            log_error("NO GRADED FILES FOR MASTER")
            return

        df = pd.concat(dfs, ignore_index=True)

        sort_cols = [
            c for c in ["game_date","away_team","home_team","market_type","bet_side"]
            if c in df.columns
        ]

        if sort_cols:
            df = df.sort_values(sort_cols, kind="mergesort")

        master = NHL_OUTPUT / "NHL_final.csv"

        df.to_csv(master, index=False)

        log_summary(f"NHL MASTER BUILT | ROWS={len(df)} | OUT={master}")

    except Exception as e:
        log_error(f"BUILD MASTER ERROR | {e}")


###############################################################
######################## MAIN #################################
###############################################################

def main():

    reset_logs()

    log_summary("START nhl_results_grade.py")

    clear_old_outputs()

    grade_league()

    build_master()

    log_summary("END nhl_results_grade.py")

    print("NHL grading complete.")


if __name__ == "__main__":
    main()
