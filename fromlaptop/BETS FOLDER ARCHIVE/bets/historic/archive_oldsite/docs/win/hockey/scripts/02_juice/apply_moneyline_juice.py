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
JUICE_FILE = Path("config/hockey/nhl/nhl_moneyline_juice.csv")

ERROR_DIR = Path("docs/win/hockey/errors/02_juice")
LOG_FILE = ERROR_DIR / "apply_moneyline_juice.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


def find_band_row(juice_df, american, fav_ud, venue):
    band = juice_df[
        (juice_df["band_min"] <= american) &
        (american <= juice_df["band_max"]) &
        (juice_df["fav_ud"] == fav_ud) &
        (juice_df["venue"] == venue)
    ]

    if band.empty:
        raise ValueError(f"No juice band for {american}, {fav_ud}, {venue}")

    return float(band.iloc[0]["extra_juice"])


def process_side(df, juice_df, side):
    american_col = f"{side}_dk_moneyline_american"
    fair_col = f"{side}_fair_decimal_moneyline"

    juiced_decimal_col = f"{side}_juiced_decimal_moneyline"
    juiced_prob_col = f"{side}_juiced_prob_moneyline"

    df[juiced_decimal_col] = pd.NA
    df[juiced_prob_col] = pd.NA

    for idx, row in df.iterrows():
        american = float(row[american_col])
        fair_decimal = float(row[fair_col])

        if not math.isfinite(fair_decimal) or fair_decimal <= 1:
            continue

        fav_ud = "favorite" if american < 0 else "underdog"
        venue = side

        extra = find_band_row(juice_df, american, fav_ud, venue)

        # Multiplicative ROI adjustment
        juiced_decimal = fair_decimal * (1 + extra)

        # Safety guard
        if juiced_decimal <= 1:
            juiced_decimal = 1.0001

        juiced_prob = 1 / juiced_decimal

        df.at[idx, juiced_decimal_col] = juiced_decimal
        df.at[idx, juiced_prob_col] = juiced_prob

    return df


def main():
    with open(LOG_FILE, "w") as log:
        log.write(f"=== APPLY MONEYLINE JUICE {datetime.utcnow().isoformat()}Z ===\n\n")

    try:
        juice_df = pd.read_csv(JUICE_FILE)
        files = glob.glob(str(INPUT_DIR / "*_NHL_moneyline.csv"))

        for file_path in files:
            df = pd.read_csv(file_path)

            df = process_side(df, juice_df, "home")
            df = process_side(df, juice_df, "away")

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
