# scripts/my_bets_clean_08.py

#!/usr/bin/env python3

import pandas as pd
import glob
from pathlib import Path
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_05")
OUTPUT_DIR = Path("docs/win/my_bets/step_06")

ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_08.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOG (OVERWRITE ALWAYS)
# =========================

with open(ERROR_LOG, "w", encoding="utf-8") as f:
    f.write(f"=== MY_BETS_CLEAN_08 RUN @ {datetime.utcnow().isoformat()}Z ===\n")

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
    skipped_rows = 0

    mapping = {
        "totals_over": "over_odds",
        "totals_under": "under_odds",
        "spreads_away": "away_spread_odds",
        "spreads_home": "home_spread_odds",
        "moneyline_home": "home_ml_odds",
        "moneyline_away": "away_ml_odds"
    }

    for file_path in files:
        try:
            total_files += 1
            df = pd.read_csv(file_path)
            rows = len(df)
            total_rows += rows

            bet_diff_values = []

            for idx, row in df.iterrows():
                bet_taken = str(row.get("bet_taken", "")).strip()
                odds_american = row.get("odds_american")

                if bet_taken in mapping and mapping[bet_taken] in df.columns:
                    compare_col = mapping[bet_taken]
                    compare_value = row.get(compare_col)

                    try:
                        if pd.notna(odds_american) and pd.notna(compare_value):
                            diff = float(odds_american) - float(compare_value)
                            bet_diff_values.append(diff)
                        else:
                            bet_diff_values.append("")
                            skipped_rows += 1
                            log(f"Skipped row {idx}: missing odds values")
                    except Exception:
                        bet_diff_values.append("")
                        skipped_rows += 1
                        log(f"Skipped row {idx}: invalid numeric conversion")
                else:
                    bet_diff_values.append("")
                    skipped_rows += 1
                    log(f"Skipped row {idx}: no mapping for bet_taken '{bet_taken}'")

            df["bet_diff"] = bet_diff_values

            output_path = OUTPUT_DIR / Path(file_path).name
            df.to_csv(output_path, index=False)

            log(f"Wrote {output_path} | rows={rows}")

        except Exception:
            log(f"ERROR processing {file_path}")
            log(traceback.format_exc())

    log(f"Files processed: {total_files}")
    log(f"Rows processed: {total_rows}")
    log(f"Rows skipped: {skipped_rows}")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    process()
