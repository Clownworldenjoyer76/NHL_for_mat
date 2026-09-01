#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import sys
import traceback

INPUT_FILE = Path("bets/historic/moneyline/nba_data_ml_bands.csv")
OUTPUT_FILE = Path("bets/nba_ml_juice.csv")


def parse_band(value: str):
    """
    Convert '-324 to -300' or '600 to 999'
    into integer (band_min, band_max).
    """
    try:
        if pd.isna(value) or value == "unknown":
            return None, None

        parts = str(value).replace(" ", "").split("to")
        band_min = int(parts[0])
        band_max = int(parts[1])
        return band_min, band_max

    except Exception:
        return None, None


def main():
    try:
        if not INPUT_FILE.exists():
            raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

        df = pd.read_csv(INPUT_FILE)

        if "band" not in df.columns:
            raise ValueError("Column 'band' not found in input file.")

        band_values = df["band"].apply(parse_band)
        df["band_min"] = band_values.apply(lambda x: x[0])
        df["band_max"] = band_values.apply(lambda x: x[1])

        # Force integer dtype (preserves blanks for unknown)
        df["band_min"] = df["band_min"].astype("Int64")
        df["band_max"] = df["band_max"].astype("Int64")

        # Reorder: band_min, band_max first
        cols = ["band_min", "band_max"] + [
            c for c in df.columns if c not in ["band_min", "band_max"]
        ]
        df = df[cols]

        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_FILE, index=False)

        print(f"Wrote {OUTPUT_FILE}")

    except Exception as e:
        print("ERROR:")
        print(str(e))
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
