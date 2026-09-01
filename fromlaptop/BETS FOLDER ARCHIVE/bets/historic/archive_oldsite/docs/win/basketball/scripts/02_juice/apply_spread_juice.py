#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import math

INPUT_DIR = Path("docs/win/basketball/01_merge")
OUTPUT_DIR = Path("docs/win/basketball/02_juice")

NBA_CONFIG = Path("config/basketball/nba/nba_spreads_juice.csv")
NCAAB_CONFIG = Path("config/basketball/ncaab/ncaab_spreads_juice.csv")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def american_to_decimal(a):
    return 1 + (a / 100 if a > 0 else 100 / abs(a))


def decimal_to_american(d):
    if not math.isfinite(d) or d <= 1:
        return ""
    if d >= 2:
        return f"+{int(round((d - 1) * 100))}"
    return f"-{int(round(100 / (d - 1)))}"


# =========================
# NBA SPREAD JUICE
# =========================

def apply_nba(df):

    jt = pd.read_csv(NBA_CONFIG)

    def process(row, side):

        spread = float(row[f"{side}_spread"])
        odds = float(row[f"{side}_acceptable_spread_american"])

        fav_ud = "favorite" if spread < 0 else "underdog"

        band = jt[
            (jt["band_min"] <= abs(spread)) &
            (abs(spread) <= jt["band_max"]) &
            (jt["fav_ud"] == fav_ud)
        ]

        extra = band.iloc[0]["extra_juice"] if not band.empty else 0.0
        if not math.isfinite(extra):
            extra = 0.0

        base_decimal = american_to_decimal(odds)
        final_decimal = base_decimal * (1 + extra)

        return final_decimal, decimal_to_american(final_decimal)

    for side in ["home", "away"]:
        df[[f"{side}_spread_juice_decimal",
            f"{side}_spread_juice_odds"]] = \
            df.apply(lambda r: process(r, side),
                     axis=1,
                     result_type="expand")

    return df


# =========================
# NCAAB SPREAD JUICE (FIXED)
# =========================

def apply_ncaab(df):

    jt = pd.read_csv(NCAAB_CONFIG)

    def process(row, side):

        spread = float(row[f"{side}_spread"])
        odds = float(row[f"{side}_acceptable_spread_american"])

        # Direct spread match (matches config structure)
        match = jt[jt["spread"] == spread]

        extra = match.iloc[0]["extra_juice"] if not match.empty else 0.0
        if not math.isfinite(extra):
            extra = 0.0

        base_decimal = american_to_decimal(odds)
        final_decimal = base_decimal * (1 + extra)

        return final_decimal, decimal_to_american(final_decimal)

    for side in ["home", "away"]:
        df[[f"{side}_spread_juice_decimal",
            f"{side}_spread_juice_odds"]] = \
            df.apply(lambda r: process(r, side),
                     axis=1,
                     result_type="expand")

    return df


# =========================
# MAIN
# =========================

def main():

    for f in INPUT_DIR.iterdir():

        name = f.name

        if name.endswith("_NBA_spread.csv"):
            df = pd.read_csv(f)
            df = apply_nba(df)
            df.to_csv(OUTPUT_DIR / name, index=False)

        elif name.endswith("_NCAAB_spread.csv"):
            df = pd.read_csv(f)
            df = apply_ncaab(df)
            df.to_csv(OUTPUT_DIR / name, index=False)


if __name__ == "__main__":
    main()
