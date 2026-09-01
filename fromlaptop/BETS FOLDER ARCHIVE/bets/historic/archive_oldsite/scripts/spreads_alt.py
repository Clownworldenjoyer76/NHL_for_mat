#!/usr/bin/env python3

import pandas as pd
import glob
from pathlib import Path
from scipy.stats import norm
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

CLEANED_DIR = Path("docs/win/dump/csvs/cleaned")
NORMALIZED_DIR = Path("docs/win/manual/normalized")
GAMES_MASTER_DIR = Path("docs/win/games_master")
OUTPUT_DIR = Path("docs/win/juice/spreads_alt")
ERROR_DIR = Path("docs/win/errors/06_spreads")
ERROR_LOG = ERROR_DIR / "spreads_alt.txt"

NBA_STD_DEV = 12
NCAAB_STD_DEV = 11

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def log(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def to_american(dec):
    if pd.isna(dec) or dec <= 1:
        return ""
    if dec >= 2:
        return f"+{int((dec - 1) * 100)}"
    return f"-{int(100 / (dec - 1))}"

def get_std_dev(league):
    return NBA_STD_DEV if league == "nba" else NCAAB_STD_DEV

# =========================
# JUICE LOGIC
# =========================

def load_juice_config(league):
    if league == "nba":
        return pd.read_csv("config/nba/nba_spreads_juice.csv")
    else:
        return pd.read_csv("config/ncaab/ncaab_spreads_juice.csv")

def get_extra_juice_ncaab(spread, juice_df):
    match = juice_df[juice_df["spread"] == spread]
    if match.empty:
        return 0
    return float(match.iloc[0]["extra_juice"])

def get_extra_juice_nba(spread, juice_df):
    abs_spread = abs(spread)
    band = juice_df[
        (abs_spread >= juice_df["band_min"]) &
        (abs_spread <= juice_df["band_max"])
    ]
    if band.empty:
        return 0
    return float(band.iloc[0]["extra_juice"])

# =========================
# CORE
# =========================

def process_league(league, summary):

    cleaned_files = glob.glob(str(CLEANED_DIR / f"{league}_*.csv"))

    if not cleaned_files:
        log(f"No cleaned files for {league}")
        return

    juice_df = load_juice_config(league)
    std_dev = get_std_dev(league)

    for cleaned_path in cleaned_files:
        try:
            summary["files_processed"] += 1

            cleaned_df = pd.read_csv(cleaned_path)
            if cleaned_df.empty:
                continue

            date_suffix = "_".join(Path(cleaned_path).stem.split("_")[1:])
            dk_path = NORMALIZED_DIR / f"dk_{league}_spreads_{date_suffix}.csv"

            if not dk_path.exists():
                log(f"{league} | Missing DK spreads file for {date_suffix}")
                continue

            dk_df = pd.read_csv(dk_path)

            merged = pd.merge(
                cleaned_df,
                dk_df[["game_id", "away_spread", "home_spread"]],
                on="game_id",
                how="inner"
            )

            if merged.empty:
                log(f"{league} | No merge rows for {date_suffix}")
                continue

            expanded_rows = []

            for _, row in merged.iterrows():

                proj_margin = (
                    row["home_team_projected_points"]
                    - row["away_team_projected_points"]
                )

                base_spread = float(row["home_spread"])
                alt_spreads = [base_spread + x for x in range(-5, 6)]

                for spread in alt_spreads:

                    home_prob = 1 - norm.cdf(-spread, proj_margin, std_dev)
                    away_prob = 1 - home_prob

                    if league == "nba":
                        extra_juice = get_extra_juice_nba(spread, juice_df)
                    else:
                        extra_juice = get_extra_juice_ncaab(spread, juice_df)

                    home_decimal = (1 / home_prob) * (1 + extra_juice)
                    away_decimal = (1 / away_prob) * (1 + extra_juice)

                    expanded_rows.append({
                        "date": row["date"],
                        "league": league,
                        "game_id": row["game_id"],
                        "away_team": row["away_team"],
                        "home_team": row["home_team"],
                        "alt_spread": spread,
                        "home_probability": home_prob,
                        "away_probability": away_prob,
                        "home_acceptable_decimal_odds": home_decimal,
                        "home_acceptable_american_odds": to_american(home_decimal),
                        "away_acceptable_decimal_odds": away_decimal,
                        "away_acceptable_american_odds": to_american(away_decimal),
                    })

            if expanded_rows:
                final_df = pd.DataFrame(expanded_rows)

                out_path = OUTPUT_DIR / f"{league}_altspreads_{date_suffix}.csv"
                final_df.to_csv(out_path, index=False)

                summary["files_written"] += 1
                summary["rows_generated"] += len(final_df)

                log(f"Wrote {out_path} | rows={len(final_df)}")

        except Exception as e:
            log(f"{league} | {cleaned_path} failed: {e}")
            log(traceback.format_exc())

# =========================
# MAIN
# =========================

def main():
    ERROR_LOG.write_text("")
    log(f"=== SPREADS_ALT RUN @ {datetime.utcnow().isoformat()}Z ===")

    summary = {
        "files_processed": 0,
        "files_written": 0,
        "rows_generated": 0
    }

    process_league("nba", summary)
    process_league("ncaab", summary)

    log("\n=== SPREADS_ALT SUMMARY ===")
    log(f"Files processed: {summary['files_processed']}")
    log(f"Files written: {summary['files_written']}")
    log(f"Rows generated: {summary['rows_generated']}")

if __name__ == "__main__":
    main()
