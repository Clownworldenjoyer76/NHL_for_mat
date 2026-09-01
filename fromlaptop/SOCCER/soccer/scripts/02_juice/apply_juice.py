#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
import traceback
import sys
from datetime import datetime

# =========================
# CONFIG & PATHS
# =========================

INPUT_DIR = Path("docs/win/soccer/01_merge/market_model")
OUTPUT_DIR = Path("docs/win/soccer/02_juice")
ERROR_LOG = Path("docs/win/soccer/errors/02_juice/01_apply_juice.txt")

JUICE_MAP = {
    "epl": Path("config/soccer/epl/3way_juice.csv"),
    "laliga": Path("config/soccer/la_liga/3way_juice.csv"),
    "bundesliga": Path("config/soccer/bundesliga/3way_juice.csv"),
    "ligue1": Path("config/soccer/ligue1/3way_juice.csv"),
    "seriea": Path("config/soccer/serie_a/3way_juice.csv"),
    "mls": Path("config/soccer/mls/3way_juice.csv"),
}

TARGET_2WAY_JUICE = 1.04  # 4% Overround

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def find_closest_juice(prob, juice_df, side):
    side_df = juice_df[juice_df["side"] == side]
    if side_df.empty:
        raise ValueError(f"No juice data for side {side}")
    closest_idx = (side_df["fair_prob"] - prob).abs().idxmin()
    return side_df.loc[closest_idx]


def get_prob_col(df, side):
    for col in [f"{side}_win_prob", f"{side}_prob"]:
        if col in df.columns:
            return col
    return None


def decimal_to_american(decimal):
    if pd.isna(decimal):
        return pd.NA
    if decimal >= 2:
        return round((decimal - 1) * 100)
    else:
        return round(-100 / (decimal - 1))


# =========================
# MARKET BLOCKS
# =========================

def process_3way(df, juice_tables):
    for side in ["home", "draw", "away"]:
        col = get_prob_col(df, side)
        if not col:
            continue

        df[f"{side}_adjusted_decimal"] = pd.NA
        df[f"{side}_adjusted_american"] = pd.NA

        for idx, row in df.iterrows():
            prob = row[col]
            market = row["market"]

            if pd.isna(prob) or market not in juice_tables:
                continue

            matched = find_closest_juice(prob, juice_tables[market], side)

            adj_prob = min(prob + matched["extra_juice"], 0.999)
            decimal = round(1 / adj_prob, 4)

            df.at[idx, f"{side}_adjusted_decimal"] = decimal
            df.at[idx, f"{side}_adjusted_american"] = decimal_to_american(decimal)

    return df


def process_totals(df):

    # ---------- 2.5 ----------
    if "over25_prob" in df.columns:

        df["under25_prob"] = 1 - df["over25_prob"]

        df["over25_adj_prob"] = df["over25_prob"] * TARGET_2WAY_JUICE
        df["under25_adj_prob"] = df["under25_prob"] * TARGET_2WAY_JUICE

        df["over25_adjusted_decimal"] = (1 / df["over25_adj_prob"]).round(4)
        df["under25_adjusted_decimal"] = (1 / df["under25_adj_prob"]).round(4)

        df["over25_adjusted_american"] = df["over25_adjusted_decimal"].apply(decimal_to_american)
        df["under25_adjusted_american"] = df["under25_adjusted_decimal"].apply(decimal_to_american)

        df = df.drop(columns=["over25_adj_prob", "under25_adj_prob"])

    # ---------- 3.5 ----------
    if "over35_prob" in df.columns:

        df["under35_prob"] = 1 - df["over35_prob"]

        df["over35_adj_prob"] = df["over35_prob"] * TARGET_2WAY_JUICE
        df["under35_adj_prob"] = df["under35_prob"] * TARGET_2WAY_JUICE

        df["over35_adjusted_decimal"] = (1 / df["over35_adj_prob"]).round(4)
        df["under35_adjusted_decimal"] = (1 / df["under35_adj_prob"]).round(4)

        df["over35_adjusted_american"] = df["over35_adjusted_decimal"].apply(decimal_to_american)
        df["under35_adjusted_american"] = df["under35_adjusted_decimal"].apply(decimal_to_american)

        df = df.drop(columns=["over35_adj_prob", "under35_adj_prob"])

    return df


def process_btts(df):

    if "btts_prob" not in df.columns:
        return df

    df["btts_yes_adj_prob"] = df["btts_prob"] * TARGET_2WAY_JUICE
    df["btts_no_adj_prob"] = (1 - df["btts_prob"]) * TARGET_2WAY_JUICE

    df["btts_yes_adjusted_decimal"] = (1 / df["btts_yes_adj_prob"]).round(4)
    df["btts_no_adjusted_decimal"] = (1 / df["btts_no_adj_prob"]).round(4)

    df["btts_yes_adjusted_american"] = df["btts_yes_adjusted_decimal"].apply(decimal_to_american)
    df["btts_no_adjusted_american"] = df["btts_no_adjusted_decimal"].apply(decimal_to_american)

    return df.drop(columns=["btts_yes_adj_prob", "btts_no_adj_prob"])


# =========================
# CORE
# =========================

def main():

    try:

        input_files = glob.glob(str(INPUT_DIR / "**" / "soccer_*.csv"), recursive=True)

        if not input_files:
            return

        for file_path in input_files:

            df = pd.read_csv(file_path)

            if "market" not in df.columns:
                continue

            juice_tables = {
                m: pd.read_csv(JUICE_MAP[m])
                for m in df["market"].unique()
                if m in JUICE_MAP
            }

            df = process_3way(df, juice_tables)
            df = process_totals(df)
            df = process_btts(df)

            df.to_csv(OUTPUT_DIR / Path(file_path).name, index=False)

        print(f"Processed {len(input_files)} files.")

    except Exception:

        with open(ERROR_LOG, "a") as log:
            log.write(f"\n{datetime.now()}\n{traceback.format_exc()}")

        sys.exit(1)


if __name__ == "__main__":
    main()
