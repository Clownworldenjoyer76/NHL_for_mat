#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path

INPUT = Path("bets/historic/clean/nba_moneyline_bets.csv")
OUTPUT = Path("config/basketball/nba/nba_ml_juice.csv")

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

MIN_SAMPLE = 200
MAX_JUICE = 0.12


def create_bands(df):

    bins = [
        -10000,-1000,-600,-500,-400,-350,-325,-300,
        -280,-270,-260,-250,-240,-230,-220,-210,-200,
        -190,-180,-170,-160,-150,-140,-130,-120,-110,-100,
        100,110,120,130,140,150,160,170,180,190,200,
        220,240,260,280,300,350,400,500,600,1000,10000
    ]

    df["band"] = pd.cut(df["odds"], bins=bins)

    return df


def calculate_roi(df):

    g = (
        df.groupby(["band","fav_ud","venue"])
        .agg(
            bets=("result","count"),
            wins=("result",lambda x: (x=="win").sum()),
            profit=("profit","sum")
        )
        .reset_index()
    )

    g["roi"] = g["profit"] / g["bets"]

    g["extra_juice"] = -g["roi"]

    return g


def cap_values(df):

    df["extra_juice"] = df["extra_juice"].clip(-MAX_JUICE, MAX_JUICE)

    return df


def remove_low_samples(df):

    df.loc[df["bets"] < MIN_SAMPLE, "extra_juice"] = 0

    return df


def smooth_values(df):

    df = df.sort_values(["fav_ud","venue","band"])

    df["extra_juice"] = (
        df.groupby(["fav_ud","venue"])["extra_juice"]
        .transform(lambda x: x.rolling(3, min_periods=1, center=True).mean())
    )

    return df


def split_band(df):

    df["band_min"] = df["band"].apply(lambda x: x.left)
    df["band_max"] = df["band"].apply(lambda x: x.right)

    return df


def apply_global_adjustments(df):

    df.loc[
        (df["fav_ud"]=="favorite") & (df["venue"]=="home"),
        "extra_juice"
    ] -= 0.02

    df.loc[
        (df["fav_ud"]=="underdog") & (df["venue"]=="away"),
        "extra_juice"
    ] += 0.02

    df["extra_juice"] = df["extra_juice"].clip(-MAX_JUICE, MAX_JUICE)

    return df


def main():

    df = pd.read_csv(INPUT)

    df = create_bands(df)

    table = calculate_roi(df)

    table = remove_low_samples(table)

    table = cap_values(table)

    table = smooth_values(table)

    table = apply_global_adjustments(table)

    table = split_band(table)

    final = table[
        ["band_min","band_max","fav_ud","venue","extra_juice"]
    ].sort_values(["band_min","fav_ud","venue"])

    final.to_csv(OUTPUT, index=False)

    print("NBA moneyline juice table written to:")
    print(OUTPUT)


if __name__ == "__main__":
    main()
