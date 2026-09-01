#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/soccer/02_soccer_results_analyze.py

from datetime import datetime
from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path("docs/win/final_scores/results/soccer/graded")
INTERMEDIATE_DIR = Path("docs/win/final_scores/intermediate")

ERROR_DIR = Path("docs/win/final_scores/errors")

INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

ERROR_LOG = ERROR_DIR / "soccer_results_analyze_errors.txt"
SUMMARY_LOG = ERROR_DIR / "soccer_results_analyze_summary.txt"

###############################################################

def reset_logs():
    ERROR_LOG.write_text("", encoding="utf-8")
    SUMMARY_LOG.write_text("", encoding="utf-8")

def log_error(msg):
    with open(ERROR_LOG,"a") as f:
        f.write(f"[{datetime.now()}] {msg}\n")

def log_summary(msg):
    with open(SUMMARY_LOG,"a") as f:
        f.write(f"[{datetime.now()}] {msg}\n")

###############################################################

def edge_bucket(v):

    if pd.isna(v): return ""

    if v < .01: return "0_to_0.01"
    if v < .02: return "0.01_to_0.02"
    if v < .03: return "0.02_to_0.03"
    if v < .05: return "0.03_to_0.05"
    return "0.05_plus"

def odds_bucket(v):

    if pd.isna(v): return ""

    if v < -150: return "minus_150_or_lower"
    if v < -110: return "minus_149_to_minus_110"
    if v < 100: return "minus_109_to_plus_100"
    if v < 150: return "plus_101_to_plus_150"

    return "plus_151_or_higher"

###############################################################

def prepare():

    path = OUTPUT_DIR / "SOCCER_final.csv"

    if not path.exists():
        log_error("MASTER FILE MISSING")
        return

    df = pd.read_csv(path)

    if df.empty:
        log_error("MASTER FILE EMPTY")
        return

    df["market"] = "SOCCER"
    df["selected_edge"] = df["edge_pct"]
    df["selected_odds"] = df["odds_american"]

    df["edge_bucket"] = df["selected_edge"].apply(edge_bucket)
    df["odds_bucket"] = df["selected_odds"].apply(odds_bucket)

    out = INTERMEDIATE_DIR / "work_soccer.csv"
    df.to_csv(out,index=False)

    log_summary(f"WORK FILE CREATED | {out} | ROWS={len(df)}")

###############################################################

def main():

    reset_logs()
    log_summary("START soccer_results_analyze.py")

    prepare()

    log_summary("END soccer_results_analyze.py")

    print("Soccer analytics preparation complete.")

if __name__ == "__main__":
    main()
