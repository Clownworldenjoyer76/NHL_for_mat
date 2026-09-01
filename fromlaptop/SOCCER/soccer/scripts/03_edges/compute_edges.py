#!/usr/bin/env python3
# docs/win/soccer/scripts/03_edges/compute_edges.py

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
    if pd.isna(american) or american == "":
        return None
    try:
        val = float(american)
        if val > 0:
            return 1 + (val / 100)
        else:
            return 1 + (100 / abs(val))
    except Exception:
        return None


def decimal_to_american(decimal):
    if pd.isna(decimal) or decimal in ["", 0, None]:
        return None
    try:
        val = float(decimal)
        if val <= 1:
            return None
        if val >= 2:
            return round((val - 1) * 100)
        else:
            return round(-100 / (val - 1))
    except Exception:
        return None


def parse_match_time(time_str):
    if pd.isna(time_str):
        return datetime.max
    try:
        return datetime.strptime(str(time_str).strip(), "%I:%M %p")
    except Exception:
        return datetime.max


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
                log.write("No input files found in 02_juice.\n")
                return

            summary = {
                "files_processed": 0,
                "rows_processed": 0
            }

            MARKETS = [

                ("home", "dk_home_american", "home_adjusted_decimal"),
                ("draw", "dk_draw_american", "draw_adjusted_decimal"),
                ("away", "dk_away_american", "away_adjusted_decimal"),

                ("over25", "dk_over25_american", "over25_adjusted_decimal"),
                ("under25", "dk_under25_american", "under25_adjusted_decimal"),

                ("over35", "dk_over35_american", "over35_adjusted_decimal"),
                ("under35", "dk_under35_american", "under35_adjusted_decimal"),

                ("btts_yes", "dk_btts_yes_american", "btts_yes_adjusted_decimal"),
                ("btts_no", "dk_btts_no_american", "btts_no_adjusted_decimal"),
            ]

            for input_path in input_files:

                df = pd.read_csv(input_path)

                if "game_id" not in df.columns:
                    log.write(f"Skipping {input_path.name}: Missing game_id\n")
                    continue

                for label, dk_amer_col, model_adj_col in MARKETS:

                    if dk_amer_col in df.columns and model_adj_col in df.columns:

                        dk_dec_col = f"{label}_dk_decimal"
                        df[dk_dec_col] = df[dk_amer_col].apply(american_to_decimal)

                        # 🔧 FORCE NUMERIC (fixes totals issue)
                        df[dk_dec_col] = pd.to_numeric(df[dk_dec_col], errors="coerce")
                        df[model_adj_col] = pd.to_numeric(df[model_adj_col], errors="coerce")

                        dk_prob_col = f"{label}_dk_implied_prob"
                        df[dk_prob_col] = 1 / df[dk_dec_col]

                        fair_decimal_col = f"{label}_fair_decimal"
                        fair_american_col = f"{label}_fair_american"

                        fair_prob = 1 / df[model_adj_col]

                        df[fair_decimal_col] = (1 / fair_prob).round(4)
                        df[fair_american_col] = df[fair_decimal_col].apply(decimal_to_american)

                        take_price_american_col = f"{label}_take_price_american"
                        df[take_price_american_col] = df[fair_american_col]

                        edge_pct_col = f"{label}_edge_pct"

                        # 🔧 SAFE EDGE CALCULATION (prevents NaN propagation)
                        edge = (df[dk_dec_col] / df[model_adj_col]) - 1

                        df[edge_pct_col] = edge.round(4)

                        df[f"{label}_play"] = df[edge_pct_col] > 0

                if "match_time" in df.columns:
                    df["_sort_time"] = df["match_time"].apply(parse_match_time)
                    df = df.sort_values(by="_sort_time")
                    df = df.drop(columns=["_sort_time"])

                df = df.drop_duplicates(subset=["game_id"])

                output_path = OUTPUT_DIR / input_path.name
                df.to_csv(output_path, index=False)

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

            raise


if __name__ == "__main__":
    main()
