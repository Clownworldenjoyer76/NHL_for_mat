# docs/win/soccer/scripts/03_edges/compute_edges.py

#!/usr/bin/env python3

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/soccer/02_juice")
OUTPUT_DIR = Path("docs/win/soccer/03_edges")
ERROR_DIR = Path("docs/win/soccer/errors/03_edges")
ERROR_LOG = ERROR_DIR / "compute_edges.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def american_to_decimal(american):
    if pd.isna(american):
        return None
    american = float(american)
    if american > 0:
        return 1 + (american / 100.0)
    return 1 + (100.0 / abs(american))

def validate_columns(df, required_cols):
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

# =========================
# CORE
# =========================

def main():

    with open(ERROR_LOG, "w") as log:

        log.write("=== COMPUTE EDGES RUN ===\n")
        log.write(f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n")

        try:

            input_files = sorted(INPUT_DIR.glob("soccer_*.csv"))

            if not input_files:
                log.write("No input files found.\n")
                return

            summary = {
                "files_processed": 0,
                "rows_processed": 0
            }

            for input_path in input_files:

                df = pd.read_csv(input_path)

                required_cols = [
                    "market",
                    "game_id",
                    "home_adjusted_decimal",
                    "draw_adjusted_decimal",
                    "away_adjusted_decimal",
                    "home_american",
                    "draw_american",
                    "away_american",
                ]

                validate_columns(df, required_cols)

                # Convert DK American → Decimal
                df["home_dk_decimal"] = df["home_american"].apply(american_to_decimal)
                df["draw_dk_decimal"] = df["draw_american"].apply(american_to_decimal)
                df["away_dk_decimal"] = df["away_american"].apply(american_to_decimal)

                for side in ["home", "draw", "away"]:

                    dk_col = f"{side}_dk_decimal"
                    adj_col = f"{side}_adjusted_decimal"

                    edge_dec_col = f"{side}_edge_decimal"
                    edge_pct_col = f"{side}_edge_pct"
                    play_col = f"{side}_play"

                    df[edge_dec_col] = df[dk_col] - df[adj_col]
                    df[edge_pct_col] = (df[dk_col] / df[adj_col]) - 1
                    df[play_col] = df[edge_dec_col] > 0

                # Deduplicate by game_id
                df = df.drop_duplicates(subset=["game_id"])

                output_path = OUTPUT_DIR / input_path.name

                write_header = not output_path.exists()

                temp_file = output_path.with_suffix(".tmp")

                if write_header:
                    df.to_csv(temp_file, index=False)
                else:
                    existing = pd.read_csv(output_path)
                    combined = pd.concat([existing, df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["game_id"])
                    combined.to_csv(temp_file, index=False)

                temp_file.replace(output_path)

                log.write(f"Wrote {output_path}\n")
                summary["files_processed"] += 1
                summary["rows_processed"] += len(df)

            log.write("\n=== SUMMARY ===\n")
            log.write(f"Files processed: {summary['files_processed']}\n")
            log.write(f"Rows processed: {summary['rows_processed']}\n")

        except Exception as e:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())

if __name__ == "__main__":
    main()
