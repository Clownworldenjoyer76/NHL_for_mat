#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/nhl_results_analyze.py

from datetime import datetime
from pathlib import Path
import pandas as pd

###############################################################
######################## PATH CONFIG ##########################
###############################################################

NHL_OUTPUT = Path("docs/win/final_scores/results/nhl/graded")

INTERMEDIATE_DIR = Path("docs/win/final_scores/intermediate")
INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

ERROR_LOG = ERROR_DIR / "nhl_results_analyze_errors.txt"
SUMMARY_LOG = ERROR_DIR / "nhl_results_analyze_summary.txt"

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
######################## EDGE EXTRACTION ######################
###############################################################

def selected_edge(row):

    market = str(row.get("market_type","")).lower()
    side = str(row.get("bet_side","")).lower()

    if market=="moneyline":

        if side=="home":
            return row.get("home_ml_edge_decimal")

        if side=="away":
            return row.get("away_ml_edge_decimal")

    if market=="puck_line":

        if side=="home":
            return row.get("home_spread_edge_decimal")

        if side=="away":
            return row.get("away_spread_edge_decimal")

    if market=="total":

        if side=="over":
            return row.get("over_edge_decimal")

        if side=="under":
            return row.get("under_edge_decimal")

    return None

###############################################################
######################## WORK FILE ############################
###############################################################

def build_work():

    path = NHL_OUTPUT / "NHL_final.csv"

    df = safe_read(path)

    if df.empty:
        return

    df["selected_edge"] = df.apply(selected_edge,axis=1)

    out = INTERMEDIATE_DIR / "work_nhl.csv"

    df.to_csv(out,index=False)

    log_summary(f"NHL WORK FILE CREATED | ROWS={len(df)} | OUT={out}")

###############################################################
######################## MAIN #################################
###############################################################

def main():

    reset_logs()

    log_summary("START nhl_results_analyze.py")

    build_work()

    log_summary("END nhl_results_analyze.py")

    print("NHL analysis prep complete.")


if __name__=="__main__":
    main()
