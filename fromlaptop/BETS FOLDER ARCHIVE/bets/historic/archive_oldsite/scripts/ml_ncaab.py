# scripts/ml_ncaab.py

import pandas as pd
import glob
import os
from pathlib import Path
from datetime import datetime

EDGE_NCAAB = 0.05
INPUT_DIR = Path("docs/win/dump/csvs/cleaned")
OUTPUT_DIR = Path("docs/win/ncaab/moneyline")
ERROR_DIR = Path("docs/win/errors/04_moneyline")

ERROR_LOG = ERROR_DIR / "ml_ncaab_errors.txt"
SUMMARY_LOG = ERROR_DIR / "ml_ncaab_summary.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

def to_american(dec):
    if pd.isna(dec) or dec <= 1.01:
        return ""
    if dec >= 2.0:
        return f"+{int(round((dec - 1.0) * 100))}"
    return f"{int(round(-100.0 / (dec - 1.0)))}"

def log_error(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

def process_ncaab_files():
    # Overwrite error log at start of run
    with open(ERROR_LOG, "w", encoding="utf-8"):
        pass

    files = glob.glob(str(INPUT_DIR / "ncaab_*.csv"))

    files_written = 0
    rows_processed = 0
    errors = 0

    if not files:
        log_error("No NCAAB files found")
        errors += 1

    for file_path in files:
        try:
            df = pd.read_csv(file_path)
            rows_processed += len(df)

            away = "away_team_moneyline_win_prob"
            home = "home_team_moneyline_win_prob"

            if away not in df.columns or home not in df.columns:
                log_error(f"{file_path} missing probability columns")
                errors += 1
                continue

            df["league"] = "ncaab_moneyline"

            df["away_ml_fair_decimal_odds"] = (1 / df[away]).round(2)
            df["away_ml_fair_american_odds"] = df["away_ml_fair_decimal_odds"].apply(to_american)
            df["away_ml_acceptable_decimal_odds"] = (df["away_ml_fair_decimal_odds"] * (1 + EDGE_NCAAB)).round(2)
            df["away_ml_acceptable_american_odds"] = df["away_ml_acceptable_decimal_odds"].apply(to_american)

            df["home_ml_fair_decimal_odds"] = (1 / df[home]).round(2)
            df["home_ml_fair_american_odds"] = df["home_ml_fair_decimal_odds"].apply(to_american)
            df["home_ml_acceptable_decimal_odds"] = (df["home_ml_fair_decimal_odds"] * (1 + EDGE_NCAAB)).round(2)
            df["home_ml_acceptable_american_odds"] = df["home_ml_acceptable_decimal_odds"].apply(to_american)

            df = df.drop(columns=[c for c in df.columns if c.startswith("fair_") or c.startswith("acceptable_")], errors="ignore")

            out = OUTPUT_DIR / f"ml_{os.path.basename(file_path)}"
            df.to_csv(out, index=False)
            files_written += 1

        except Exception as e:
            log_error(f"{file_path} failed: {e}")
            errors += 1

    with open(SUMMARY_LOG, "w", encoding="utf-8") as f:
        f.write(
            f"script=ml_ncaab\n"
            f"timestamp={datetime.utcnow().isoformat()}\n"
            f"files_found={len(files)}\n"
            f"files_written={files_written}\n"
            f"rows_processed={rows_processed}\n"
            f"errors={errors}\n"
        )

if __name__ == "__main__":
    process_ncaab_files()
