# docs/win/basketball/scripts/model_testing/compute_edges.py
#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import re

INPUT_DIR = Path("docs/win/basketball/02_juice")
OUTPUT_DIR = Path("docs/win/basketball/03_edges")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def implied_prob(decimal):
    return (1 / decimal).where(decimal > 1, 0)

def edge(model, book):
    model = pd.to_numeric(model, errors="coerce")
    book = pd.to_numeric(book, errors="coerce")
    return implied_prob(model) - implied_prob(book)

def extract_date(name):
    return re.search(r"\d{4}_\d{2}_\d{2}", name).group(0)

def compute_moneyline(df):

    df["home_edge_decimal"] = edge(
        df["home_dk_decimal_moneyline"],
        df["home_juice_decimal_moneyline"]
    )

    df["away_edge_decimal"] = edge(
        df["away_dk_decimal_moneyline"],
        df["away_juice_decimal_moneyline"]
    )

    return df

def compute_spread(df):

    df["home_edge_decimal"] = edge(
        df["home_dk_spread_decimal"],
        df["home_spread_juice_decimal"]
    )

    df["away_edge_decimal"] = edge(
        df["away_dk_spread_decimal"],
        df["away_spread_juice_decimal"]
    )

    return df

def compute_total(df):

    df["over_edge_decimal"] = edge(
        df["dk_total_over_decimal"],
        df["total_over_juice_decimal"]
    )

    df["under_edge_decimal"] = edge(
        df["dk_total_under_decimal"],
        df["total_under_juice_decimal"]
    )

    return df

def process(files, fn, league, market):

    for f in files:
        df = pd.read_csv(f)
        df = fn(df)

        date = extract_date(f.name)

        out = OUTPUT_DIR / f"{date}_basketball_{league}_{market}.csv"
        df.to_csv(out, index=False)

def main():

    process(INPUT_DIR.glob("*_NBA_moneyline.csv"), compute_moneyline, "NBA", "moneyline")
    process(INPUT_DIR.glob("*_NBA_spread.csv"), compute_spread, "NBA", "spread")
    process(INPUT_DIR.glob("*_NBA_total.csv"), compute_total, "NBA", "total")

    process(INPUT_DIR.glob("*_NCAAB_moneyline.csv"), compute_moneyline, "NCAAB", "moneyline")
    process(INPUT_DIR.glob("*_NCAAB_spread.csv"), compute_spread, "NCAAB", "spread")
    process(INPUT_DIR.glob("*_NCAAB_total.csv"), compute_total, "NCAAB", "total")

if __name__ == "__main__":
    main()
