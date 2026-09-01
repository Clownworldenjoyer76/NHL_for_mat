# scripts/my_bets_clean_07.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_05")
ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_07.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOG (OVERWRITE ALWAYS)
# =========================

with open(ERROR_LOG, "w", encoding="utf-8") as f:
    f.write(f"=== MY_BETS_CLEAN_07 RUN @ {datetime.utcnow().isoformat()}Z ===\n")

def log(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# =========================
# MAIN
# =========================

def process():
    files = glob.glob(str(INPUT_DIR / "juiceReelBets_*.csv"))

    total_files = 0
    total_rows = 0

    for file_path in files:
        try:
            total_files += 1
            df = pd.read_csv(file_path)
            rows = len(df)
            total_rows += rows

            # 1. delete 'bet' column
            if "bet" in df.columns:
                df = df.drop(columns=["bet"])

            # 2. create bet_taken + bet_diff
            bet_taken_values = []
            bet_diff_values = []

            for _, row in df.iterrows():
                leg_type = str(row.get("leg_type", "")).strip()
                bet_on = str(row.get("bet_on", "")).strip()

                away_team = str(row.get("away_team", "")).strip()
                home_team = str(row.get("home_team", "")).strip()

                # normalize bet_on
                if bet_on.lower() == "over":
                    bet_side = "over"
                elif bet_on.lower() == "under":
                    bet_side = "under"
                elif bet_on == away_team:
                    bet_side = "away"
                elif bet_on == home_team:
                    bet_side = "home"
                else:
                    bet_side = bet_on.lower()

                bet_taken = f"{leg_type}_{bet_side}" if leg_type else ""

                bet_taken_values.append(bet_taken)
                bet_diff_values.append("")  # per spec, just create column

            df["bet_taken"] = bet_taken_values
            df["bet_diff"] = bet_diff_values

            # overwrite same file
            df.to_csv(file_path, index=False)

            log(f"Wrote {file_path} | rows={rows}")

        except Exception:
            log(f"ERROR processing {file_path}")
            log(traceback.format_exc())

    log(f"Files processed: {total_files}")
    log(f"Rows processed: {total_rows}")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    process()
