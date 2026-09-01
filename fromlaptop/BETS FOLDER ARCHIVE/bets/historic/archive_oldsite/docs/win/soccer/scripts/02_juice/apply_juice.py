
# docs/win/soccer/scripts/02_juice/apply_juice.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
import traceback
import sys
from datetime import datetime

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/soccer/01_merge")
OUTPUT_DIR = Path("docs/win/soccer/02_juice")

ERROR_DIR = Path("docs/win/soccer/errors/02_juice")
ERROR_LOG = ERROR_DIR / "01_apply_juice.txt"

JUICE_MAP = {
    "epl": Path("config/soccer/epl/epl_1x2_juice.csv"),
    "laliga": Path("config/soccer/la_liga/laliga_1x2_juice.csv"),
    "bundesliga": Path("config/soccer/bundesliga/bundesliga_1x2_juice.csv"),
    "ligue1": Path("config/soccer/ligue1/ligue1_1x2_juice.csv"),
    "seriea": Path("config/soccer/serie_a/seriea_1x2_juice.csv"),
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def decimal_to_american(decimal_odds):
    if pd.isna(decimal_odds) or decimal_odds <= 1:
        return ""
    if decimal_odds >= 2.0:
        return f"+{int(round((decimal_odds - 1) * 100))}"
    return f"-{int(round(100 / (decimal_odds - 1)))}"


def find_band(prob, juice_df):
    band = juice_df[
        (juice_df["band_min"] <= prob) &
        (prob < juice_df["band_max"])
    ]
    if band.empty:
        raise ValueError(f"No juice band found for probability {prob}")
    return band.iloc[0]


def process_side(df, side, juice_tables, summary):
    prob_col = f"{side}_prob"

    # Numeric columns
    df[f"{side}_fair_decimal"] = pd.NA
    df[f"{side}_adjusted_prob"] = pd.NA
    df[f"{side}_adjusted_decimal"] = pd.NA

    # String columns
    df[f"{side}_juice_band"] = ""
    df[f"{side}_extra_juice"] = pd.NA
    df[f"{side}_adjusted_american"] = ""

    for idx, row in df.iterrows():
        prob = row[prob_col]

        if pd.isna(prob) or prob <= 0 or prob >= 1:
            summary["rows_skipped"] += 1
            continue

        market = row["market"]

        if market not in juice_tables:
            raise ValueError(f"No juice config mapped for market: {market}")

        juice_df = juice_tables[market]

        fair_decimal = 1 / prob

        band_row = find_band(prob, juice_df)
        extra_juice = band_row["extra_juice"]
        band_label = f"{band_row['band_min']}-{band_row['band_max']}"

        adjusted_prob = prob + extra_juice
        if adjusted_prob >= 0.999:
            adjusted_prob = 0.999

        adjusted_decimal = 1 / adjusted_prob
        adjusted_american = decimal_to_american(adjusted_decimal)

        df.at[idx, f"{side}_fair_decimal"] = float(fair_decimal)
        df.at[idx, f"{side}_juice_band"] = band_label
        df.at[idx, f"{side}_extra_juice"] = float(extra_juice)
        df.at[idx, f"{side}_adjusted_prob"] = float(adjusted_prob)
        df.at[idx, f"{side}_adjusted_decimal"] = float(adjusted_decimal)
        df.at[idx, f"{side}_adjusted_american"] = adjusted_american

        summary["rows_processed"] += 1

    return df


# =========================
# CORE
# =========================

def main():

    with open(ERROR_LOG, "w", encoding="utf-8") as log:
        log.write("=== APPLY JUICE RUN ===\n")
        log.write(f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n")

    try:
        input_files = glob.glob(str(INPUT_DIR / "soccer_*.csv"))

        if not input_files:
            with open(ERROR_LOG, "a", encoding="utf-8") as log:
                log.write("No input files found.\n")
            return

        summary = {
            "files_processed": 0,
            "rows_processed": 0,
            "rows_skipped": 0
        }

        for file_path in input_files:
            input_path = Path(file_path)
            df = pd.read_csv(input_path)

            if "market" not in df.columns:
                raise ValueError("Missing 'market' column")
            if "home_prob" not in df.columns:
                raise ValueError("Missing probability columns")

            unique_markets = df["market"].unique()
            juice_tables = {}

            for m in unique_markets:
                if m not in JUICE_MAP:
                    raise ValueError(f"No juice config mapped for market: {m}")
                juice_tables[m] = pd.read_csv(JUICE_MAP[m])

            for side in ["home", "draw", "away"]:
                df = process_side(df, side, juice_tables, summary)

            output_path = OUTPUT_DIR / input_path.name
            df.to_csv(output_path, index=False)

            with open(ERROR_LOG, "a", encoding="utf-8") as log:
                log.write(f"Wrote {output_path}\n")

            summary["files_processed"] += 1

        with open(ERROR_LOG, "a", encoding="utf-8") as log:
            log.write("\n=== SUMMARY ===\n")
            log.write(f"Files processed: {summary['files_processed']}\n")
            log.write(f"Rows processed: {summary['rows_processed']}\n")
            log.write(f"Rows skipped: {summary['rows_skipped']}\n")

    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as log:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
