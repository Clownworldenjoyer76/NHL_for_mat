# docs/win/basketball/scripts/02_juice/apply_moneyline_juice.py
#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import math

INPUT_DIR = Path("docs/win/basketball/01_merge")
OUTPUT_DIR = Path("docs/win/basketball/02_juice")

NBA_CONFIG = Path("config/basketball/nba/nba_ml_juice.csv")
NCAAB_CONFIG = Path("config/basketball/ncaab/ncaab_ml_juice.csv")

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

        # sportsbook odds determine band
        book_odds = float(row[f"{side}_dk_moneyline_american"])
        fav_ud = "favorite" if book_odds < 0 else "underdog"

        band = jt[
            (jt["band_min"] <= book_odds) &
            (book_odds <= jt["band_max"]) &
            (jt["fav_ud"] == fav_ud) &
            (jt["venue"] == side)
        ]

        roi = band.iloc[0]["roi"] if not band.empty else 0.0
        if not math.isfinite(roi):
            roi = 0.0

        # apply % adjustment to acceptable decimal
        base_decimal = float(row[f"{side}_acceptable_decimal_moneyline"])
        final_decimal = base_decimal * (1 - roi)

        return final_decimal, decimal_to_american(final_decimal)

    for side in ["home", "away"]:
        df[[f"{side}_juice_decimal_moneyline",
            f"{side}_juice_odds"]] = \
            df.apply(lambda r: process(r, side),
                     axis=1,
                     result_type="expand")

    return df


def apply_ncaab(df):

    jt = pd.read_csv(NCAAB_CONFIG)

    def lookup(prob):
        band = jt[(jt["prob_bin_min"] <= prob) & (prob < jt["prob_bin_max"])]
        return band.iloc[0]["extra_juice"] if not band.empty else 0.0

    def process(row, side):

        prob = float(row[f"{side}_prob"])
        odds = float(row[f"{side}_acceptable_american_moneyline"])
        extra = lookup(prob)

        base_decimal = american_to_decimal(odds)
        final_decimal = base_decimal * (1 + extra)

        return final_decimal, decimal_to_american(final_decimal)

    for side in ["home", "away"]:
        df[[f"{side}_juice_decimal_moneyline",
            f"{side}_juice_odds"]] = \
            df.apply(lambda r: process(r, side),
                     axis=1,
                     result_type="expand")

    return df


def main():

    for f in INPUT_DIR.iterdir():

        name = f.name

        if name.endswith("_NBA_moneyline.csv"):
            df = pd.read_csv(f)
            df = apply_nba(df)
            df.to_csv(OUTPUT_DIR / name, index=False)

        elif name.endswith("_NCAAB_moneyline.csv"):
            df = pd.read_csv(f)
            df = apply_ncaab(df)
            df.to_csv(OUTPUT_DIR / name, index=False)


if __name__ == "__main__":
    main()
