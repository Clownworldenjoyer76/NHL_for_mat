# docs/win/basketball/scripts/02_juice/apply_total_juice.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import math

INPUT_DIR = Path("docs/win/basketball/01_merge")
OUTPUT_DIR = Path("docs/win/basketball/02_juice")

NBA_CONFIG = Path("config/basketball/nba/nba_totals_juice.csv")
NCAAB_CONFIG = Path("config/basketball/ncaab/ncaab_totals_juice.csv")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def american_to_decimal(a):
    return 1 + (a / 100 if a > 0 else 100 / abs(a))


def decimal_to_american(d):
    if not math.isfinite(d) or d <= 1:
        return ""
    if d >= 2:
        return f"+{int(round((d - 1) * 100))}"
    return f"-{int(round(100 / (d - 1)))}"


def apply_nba(df):

    jt = pd.read_csv(NBA_CONFIG)

    def process(row, side):

        total = float(row["total"])
        odds = float(row[f"acceptable_total_{side}_american"])

        band = jt[
            (jt["band_min"] <= total) &
            (total <= jt["band_max"]) &
            (jt["side"] == side)
        ]

        extra = band.iloc[0]["extra_juice"] if not band.empty else 0.0
        if not math.isfinite(extra):
            extra = 0.0

        base_decimal = american_to_decimal(odds)
        final_decimal = base_decimal * (1 + extra)

        return final_decimal, decimal_to_american(final_decimal)

    df[["total_over_juice_decimal", "total_over_juice_odds"]] = \
        df.apply(lambda r: process(r, "over"), axis=1, result_type="expand")

    df[["total_under_juice_decimal", "total_under_juice_odds"]] = \
        df.apply(lambda r: process(r, "under"), axis=1, result_type="expand")

    return df


def apply_ncaab(df):

    jt = pd.read_csv(NCAAB_CONFIG)

    def process(row, side):

        total = float(row["total"])
        odds = float(row[f"acceptable_total_{side}_american"])

        match = jt[
            (jt["over_under"] == total) &
            (jt["side"] == side)
        ]

        extra = match.iloc[0]["extra_juice"] if not match.empty else 0.0
        if not math.isfinite(extra):
            extra = 0.0

        base_decimal = american_to_decimal(odds)
        final_decimal = base_decimal * (1 + extra)

        return final_decimal, decimal_to_american(final_decimal)

    df[["total_over_juice_decimal", "total_over_juice_odds"]] = \
        df.apply(lambda r: process(r, "over"), axis=1, result_type="expand")

    df[["total_under_juice_decimal", "total_under_juice_odds"]] = \
        df.apply(lambda r: process(r, "under"), axis=1, result_type="expand")

    return df


def main():

    for f in INPUT_DIR.iterdir():

        name = f.name

        if name.endswith("_NBA_total.csv"):
            df = pd.read_csv(f)
            df = apply_nba(df)
            df.to_csv(OUTPUT_DIR / name, index=False)

        elif name.endswith("_NCAAB_total.csv"):
            df = pd.read_csv(f)
            df = apply_ncaab(df)
            df.to_csv(OUTPUT_DIR / name, index=False)


if __name__ == "__main__":
    main()
