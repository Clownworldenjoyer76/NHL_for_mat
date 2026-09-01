#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
from datetime import datetime
import traceback
import sys
import math

INPUT_DIR = Path("docs/win/hockey/01_merge")
OUTPUT_DIR = Path("docs/win/hockey/02_juice")
JUICE_FILE = Path("config/hockey/nhl/nhl_total_juice.csv")

ERROR_DIR = Path("docs/win/hockey/errors/02_juice")
LOG_FILE = ERROR_DIR / "apply_total_juice.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


def find_band_row(juice_df, total, side):
    band = juice_df[
        (juice_df["band_min"] <= total) &
        (total <= juice_df["band_max"]) &
        (juice_df["side"] == side)
    ]

    if band.empty:
        raise ValueError(f"No total juice band for {total}, {side}")

    return float(band.iloc[0]["extra_juice"])


def process_side(df, juice_df, side):
    fair_col = f"fair_total_{side}_decimal"

    juiced_decimal_col = f"juiced_total_{side}_decimal"
    juiced_prob_col = f"juiced_total_{side}_prob"

    df[juiced_decimal_col] = pd.NA
    df[juiced_prob_col] = pd.NA

    for idx, row in df.iterrows():
        try:
            total = float(row["total"])
            fair_decimal = float(row[fair_col])
        except Exception:
            continue

        if not math.isfinite(fair_decimal) or fair_decimal <= 1:
            continue

        extra = find_band_row(juice_df, total, side)

        # Multiplicative ROI adjustment
        juiced_decimal = fair_decimal * (1 + extra)

        # Safety guard
        if not math.isfinite(juiced_decimal) or juiced_decimal <= 1:
            juiced_decimal = 1.0001

        juiced_prob = 1 / juiced_decimal

        df.at[idx, juiced_decimal_col] = juiced_decimal
        df.at[idx, juiced_prob_col] = juiced_prob

    return df


def main():
    with open(LOG_FILE, "w") as log:
        log.write(f"=== APPLY TOTAL JUICE {datetime.utcnow().isoformat()}Z ===\n\n")

    try:
        juice_df = pd.read_csv(JUICE_FILE)
        files = glob.glob(str(INPUT_DIR / "*_NHL_total.csv"))

        for file_path in files:
            df = pd.read_csv(file_path)

            df = process_side(df, juice_df, "over")
            df = process_side(df, juice_df, "under")

            output_path = OUTPUT_DIR / Path(file_path).name
            df.to_csv(output_path, index=False)

            with open(LOG_FILE, "a") as log:
                log.write(f"Wrote {output_path}\n")

    except Exception as e:
        with open(LOG_FILE, "a") as log:
            log.write("\nERROR\n")
            log.write(str(e) + "\n")
            log.write(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
