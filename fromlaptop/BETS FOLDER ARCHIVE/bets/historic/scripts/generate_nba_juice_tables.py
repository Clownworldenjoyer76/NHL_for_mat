#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path

CLEAN_DIR = Path("bets/historic/clean")

ML_FILE = CLEAN_DIR / "nba_moneyline_bets.csv"
SPREAD_FILE = CLEAN_DIR / "nba_spread_bets.csv"
TOTAL_FILE = CLEAN_DIR / "nba_total_bets.csv"

OUT_DIR = Path("config/basketball/nba")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def create_moneyline_bands(df):

    bins = [
        -10000,-1000,-600,-500,-400,-350,-325,-300,
        -280,-270,-260,-250,-240,-230,-220,-210,-200,
        -190,-180,-170,-160,-150,-140,-130,-120,-110,-100,
        100,110,120,130,140,150,160,170,180,190,200,
        220,240,260,280,300,350,400,500,600,1000,10000
    ]

    df["band"] = pd.cut(df["odds"], bins=bins)

    grouped = (
        df.groupby(["band","fav_ud","venue"])
        .agg(
            bets=("result","count"),
            wins=("result",lambda x: (x=="win").sum()),
            profit=("profit","sum")
        )
        .reset_index()
    )

    grouped["roi"] = grouped["profit"] / grouped["bets"]

    grouped["extra_juice"] = -grouped["roi"]

    return grouped


def create_spread_bands(df):

    bins = [0,1,3,5,7,10,15,100]

    df["spread_abs"] = df["spread"].abs()

    df["band"] = pd.cut(df["spread_abs"], bins=bins)

    df["fav_ud"] = np.where(df["spread"] < 0,"favorite","underdog")

    grouped = (
        df.groupby(["band","fav_ud","venue"])
        .agg(
            bets=("result","count"),
            wins=("result",lambda x: (x=="win").sum())
        )
        .reset_index()
    )

    grouped["win_pct"] = grouped["wins"] / grouped["bets"]

    grouped["roi"] = grouped["win_pct"] - 0.5238

    grouped["extra_juice"] = -grouped["roi"]

    return grouped


def create_total_bands(df):

    bins = [0,205,215,225,235,245,1000]

    df["band"] = pd.cut(df["line"], bins=bins)

    grouped = (
        df.groupby(["band","side"])
        .agg(
            bets=("result","count"),
            wins=("result",lambda x: (x=="win").sum())
        )
        .reset_index()
    )

    grouped["win_pct"] = grouped["wins"] / grouped["bets"]

    grouped["roi"] = grouped["win_pct"] - 0.5238

    grouped["extra_juice"] = -grouped["roi"]

    return grouped


def main():

    ml = pd.read_csv(ML_FILE)
    spreads = pd.read_csv(SPREAD_FILE)
    totals = pd.read_csv(TOTAL_FILE)

    ml_table = create_moneyline_bands(ml)
    spread_table = create_spread_bands(spreads)
    total_table = create_total_bands(totals)

    ml_table.to_csv(OUT_DIR / "nba_ml_juice.csv", index=False)
    spread_table.to_csv(OUT_DIR / "nba_spreads_juice.csv", index=False)
    total_table.to_csv(OUT_DIR / "nba_totals_juice.csv", index=False)


if __name__ == "__main__":
    main()
