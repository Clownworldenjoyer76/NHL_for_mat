# scripts/juice_ml.py

import pandas as pd
import glob
from pathlib import Path
import math
from datetime import datetime

# ---------- LOGGING ----------

ERROR_DIR = Path("docs/win/errors/07_juice")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "juice_ml.log"

files_scanned = 0
files_written = 0
rows_processed = 0
rows_defaulted = 0

def log(msg):
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ---------- ODDS HELPERS ----------

def american_to_decimal(a):
    return 1 + (a / 100 if a > 0 else 100 / abs(a))

def decimal_to_american(d):
    if not math.isfinite(d) or d <= 1:
        raise ValueError("Invalid decimal odds")
    return int(round((d - 1) * 100)) if d >= 2 else int(round(-100 / (d - 1)))

# ---------- JUICE LOOKUP ----------

def prob_bin_lookup(p, jt):
    r = jt[(jt.prob_bin_min <= p) & (p < jt.prob_bin_max)]
    return float(r.iloc[0].extra_juice) if not r.empty else 0.0

# ---------- MAIN ----------

def normalize_date(val):
    return str(val).replace("-", "_")

def run():
    global files_scanned, files_written, rows_processed, rows_defaulted

    LOG_FILE.write_text("", encoding="utf-8")

    log(f"\n=== JUICE ML RUN @ {datetime.utcnow().isoformat()}Z ===")

    JOBS = [
        ("nba", "ml", "config/nba/nba_ml_juice.csv",
         "docs/win/nba/moneyline/ml_nba_*.csv",
         [
             ("home_ml_acceptable_american_odds", "home_team_moneyline_win_prob"),
             ("away_ml_acceptable_american_odds", "away_team_moneyline_win_prob"),
         ]),
        ("nhl", "ml", "config/nhl/nhl_ml_juice.csv",
         "docs/win/nhl/moneyline/ml_nhl_*.csv",
         [
             ("home_ml_acceptable_american_odds", "home_team_moneyline_win_prob"),
             ("away_ml_acceptable_american_odds", "away_team_moneyline_win_prob"),
         ]),
        ("ncaab", "ml", "config/ncaab/ncaab_ml_juice.csv",
         "docs/win/ncaab/moneyline/ml_ncaab_*.csv",
         [
             ("home_ml_acceptable_american_odds", "home_team_moneyline_win_prob"),
             ("away_ml_acceptable_american_odds", "away_team_moneyline_win_prob"),
         ]),
    ]

    for league, market, juice_file, pattern, legs in JOBS:
        jt = pd.read_csv(juice_file)
        if league == "nba" and "lookup_type" in jt.columns:
            jt = jt[jt.lookup_type == "prob"].copy()

        out_dir = Path(f"docs/win/juice/{league}/{market}")
        out_dir.mkdir(parents=True, exist_ok=True)

        for f in glob.glob(pattern):
            files_scanned += 1
            df = pd.read_csv(f)
            rows_processed += len(df)
            game_date = normalize_date(df["date"].iloc[0])

            for odds_col, prob_col in legs:
                out_col = odds_col.replace("acceptable_american_odds", "juice_odds")

                def apply(row):
                    global rows_defaulted
                    try:
                        base_dec = american_to_decimal(row[odds_col])
                        juice = prob_bin_lookup(row[prob_col], jt)
                        return decimal_to_american(base_dec * (1 + juice))
                    except Exception:
                        rows_defaulted += 1
                        return row[odds_col]

                df[out_col] = df.apply(apply, axis=1)

            out = out_dir / f"juice_{league}_{market}_{game_date}.csv"
            df.to_csv(out, index=False)
            files_written += 1
            log(f"Wrote {out}")

    log(f"Files scanned: {files_scanned}")
    log(f"Files written: {files_written}")
    log(f"Rows processed: {rows_processed}")
    log(f"Rows defaulted: {rows_defaulted}")

if __name__ == "__main__":
    run()
