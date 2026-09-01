# docs/win/basketball/scripts/01_merge/build_juice_files.py

#!/usr/bin/env python3

import pandas as pd
import glob
import sys
import traceback
from pathlib import Path
from datetime import datetime
from scipy.stats import norm, poisson

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/basketball/01_merge")
ERROR_DIR = Path("docs/win/basketball/errors/01_merge")
ERROR_LOG = ERROR_DIR / "build_juice_files.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# CONSTANTS
# =========================

EDGE = 0.05                 # Default EDGE (ML + Spread)
NBA_TOTAL_EDGE = 0.04       # 🔥 Reduced NBA totals EDGE

NBA_TOTAL_STD = 14
NBA_SPREAD_STD = 12

NCAAB_TOTAL_STD = 12
NCAAB_SPREAD_STD = 15   # YOUR WINNING VALUE

# =========================
# HELPERS
# =========================

def american_to_decimal(odds):
    if pd.isna(odds) or odds == "":
        return ""
    odds = float(odds)
    if odds > 0:
        return 1 + (odds / 100)
    return 1 + (100 / abs(odds))


def to_american(dec):
    if pd.isna(dec) or dec <= 1:
        return ""
    if dec >= 2:
        return f"+{int((dec - 1) * 100)}"
    return f"-{int(100 / (dec - 1))}"


def clamp_probability(p):
    return min(max(p, 0.05), 0.95)


def get_stds(market):
    if market == "NBA":
        return NBA_TOTAL_STD, NBA_SPREAD_STD
    return NCAAB_TOTAL_STD, NCAAB_SPREAD_STD


# =========================
# MAIN
# =========================

def main():

    with open(ERROR_LOG, "w", encoding="utf-8") as log:
        log.write("=== BUILD JUICE FILES RUN ===\n")
        log.write(f"{datetime.utcnow().isoformat()}Z\n\n")

    try:

        input_files = glob.glob(str(INPUT_DIR / "basketball_*.csv"))

        if not input_files:
            with open(ERROR_LOG, "a", encoding="utf-8") as log:
                log.write("No input files found.\n")
            return

        for file_path in input_files:

            df = pd.read_csv(file_path)
            if df.empty:
                continue

            # FIX: ensure probabilities are numeric
            df["home_prob"] = pd.to_numeric(df["home_prob"], errors="coerce")
            df["away_prob"] = pd.to_numeric(df["away_prob"], errors="coerce")

            game_date = df["game_date"].iloc[0]
            market = df["market"].iloc[0]

            total_std, spread_std = get_stds(market)

            # =========================
            # MONEYLINE
            # =========================

            ml_df = df.copy()

            ml_df["away_dk_decimal_moneyline"] = ml_df["away_dk_moneyline_american"].apply(american_to_decimal)
            ml_df["home_dk_decimal_moneyline"] = ml_df["home_dk_moneyline_american"].apply(american_to_decimal)

            ml_df["away_fair_decimal_moneyline"] = 1 / ml_df["away_prob"]
            ml_df["home_fair_decimal_moneyline"] = 1 / ml_df["home_prob"]

            ml_df["away_acceptable_decimal_moneyline"] = ml_df["away_fair_decimal_moneyline"] * (1 + EDGE)
            ml_df["home_acceptable_decimal_moneyline"] = ml_df["home_fair_decimal_moneyline"] * (1 + EDGE)

            ml_df["away_acceptable_american_moneyline"] = ml_df["away_acceptable_decimal_moneyline"].apply(to_american)
            ml_df["home_acceptable_american_moneyline"] = ml_df["home_acceptable_decimal_moneyline"].apply(to_american)

            ml_output = INPUT_DIR / f"{game_date}_{market}_moneyline.csv"
            ml_df.to_csv(ml_output, index=False)

            # =========================
            # TOTALS
            # =========================

            total_df = df.copy()

            total_df["dk_total_over_decimal"] = total_df["dk_total_over_american"].apply(american_to_decimal)
            total_df["dk_total_under_decimal"] = total_df["dk_total_under_american"].apply(american_to_decimal)

            fair_over = []
            fair_under = []
            acc_over = []
            acc_under = []

            for _, row in total_df.iterrows():

                T = row["total"]
                mean = row["total_projected_points"]

                if pd.isna(T):
                    fair_over.append("")
                    fair_under.append("")
                    acc_over.append("")
                    acc_under.append("")
                    continue

                # NCAAB uses Poisson
                if market == "NCAAB":
                    p_under = poisson.cdf(T - 0.5, mean)
                else:
                    z = (T - mean) / total_std
                    p_under = norm.cdf(z)

                p_under = clamp_probability(p_under)
                p_over = 1 - p_under

                fair_under_dec = 1 / p_under
                fair_over_dec = 1 / p_over

                fair_under.append(fair_under_dec)
                fair_over.append(fair_over_dec)

                # 🔥 Use reduced EDGE only for NBA totals
                edge_used = NBA_TOTAL_EDGE if market == "NBA" else EDGE

                acc_under_dec = fair_under_dec * (1 + edge_used)
                acc_over_dec = fair_over_dec * (1 + edge_used)

                acc_under.append(acc_under_dec)
                acc_over.append(acc_over_dec)

            total_df["fair_total_over_decimal"] = fair_over
            total_df["fair_total_under_decimal"] = fair_under
            total_df["acceptable_total_over_decimal"] = acc_over
            total_df["acceptable_total_under_decimal"] = acc_under
            total_df["acceptable_total_over_american"] = total_df["acceptable_total_over_decimal"].apply(to_american)
            total_df["acceptable_total_under_american"] = total_df["acceptable_total_under_decimal"].apply(to_american)

            total_output = INPUT_DIR / f"{game_date}_{market}_total.csv"
            total_df.to_csv(total_output, index=False)

            # =========================
            # SPREAD
            # =========================

            spread_df = df.copy()

            spread_df["away_dk_spread_decimal"] = spread_df["away_dk_spread_american"].apply(american_to_decimal)
            spread_df["home_dk_spread_decimal"] = spread_df["home_dk_spread_american"].apply(american_to_decimal)

            fair_home = []
            fair_away = []
            acc_home = []
            acc_away = []

            for _, row in spread_df.iterrows():

                mean_margin = row["home_projected_points"] - row["away_projected_points"]

                try:
                    home_line = float(row["home_spread"])
                except:
                    fair_home.append("")
                    fair_away.append("")
                    acc_home.append("")
                    acc_away.append("")
                    continue

                p_home_cover = 1 - norm.cdf(-home_line, mean_margin, spread_std)
                p_home_cover = clamp_probability(p_home_cover)
                p_away_cover = 1 - p_home_cover

                fair_home_dec = 1 / p_home_cover
                fair_away_dec = 1 / p_away_cover

                fair_home.append(fair_home_dec)
                fair_away.append(fair_away_dec)

                acc_home_dec = fair_home_dec * (1 + EDGE)
                acc_away_dec = fair_away_dec * (1 + EDGE)

                acc_home.append(acc_home_dec)
                acc_away.append(acc_away_dec)

            spread_df["home_fair_spread_decimal"] = fair_home
            spread_df["away_fair_spread_decimal"] = fair_away
            spread_df["home_acceptable_spread_decimal"] = acc_home
            spread_df["away_acceptable_spread_decimal"] = acc_away
            spread_df["home_acceptable_spread_american"] = spread_df["home_acceptable_spread_decimal"].apply(to_american)
            spread_df["away_acceptable_spread_american"] = spread_df["away_acceptable_spread_decimal"].apply(to_american)

            spread_output = INPUT_DIR / f"{game_date}_{market}_spread.csv"
            spread_df.to_csv(spread_output, index=False)

            with open(ERROR_LOG, "a", encoding="utf-8") as log:
                log.write(f"Processed {file_path}\n")

        with open(ERROR_LOG, "a", encoding="utf-8") as log:
            log.write("\nCompleted successfully.\n")

    except Exception as e:
        with open(ERROR_LOG, "a", encoding="utf-8") as log:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
