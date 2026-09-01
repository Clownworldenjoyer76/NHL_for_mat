# scripts/my_bets_clean_06.py

#!/usr/bin/env python3

import pandas as pd
import glob
from pathlib import Path
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_04")
WINNERS_DIR = Path("docs/win/winners/step_03")
OUTPUT_DIR = Path("docs/win/my_bets/step_05")

ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_06.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOG (OVERWRITE ALWAYS)
# =========================

with open(ERROR_LOG, "w", encoding="utf-8") as f:
    f.write(f"=== MY_BETS_CLEAN_06 RUN @ {datetime.utcnow().isoformat()}Z ===\n")

def log(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# =========================
# LOAD WINNERS
# =========================

def load_winners():
    files = glob.glob(str(WINNERS_DIR / "winners_*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for file in files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            log(f"ERROR loading winners file {file}: {e}")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)

    required_cols = {"game_id", "league"}
    if not required_cols.issubset(combined.columns):
        log("ERROR: winners files missing required columns (game_id, league)")
        return pd.DataFrame()

    return combined

# =========================
# MAIN
# =========================

def process():
    winners_df = load_winners()

    if winners_df.empty:
        log("No valid winners files found.")
        return

    input_files = glob.glob(str(INPUT_DIR / "juiceReelBets_*.csv"))

    total_files = 0
    total_rows = 0
    total_matched = 0
    total_unmatched = 0

    winners_fields = [
        "home_ml_edge",
        "away_ml_edge",
        "away_ml_odds",
        "home_ml_odds",
        "away_spread",
        "home_spread",
        "home_spread_edge",
        "away_spread_edge",
        "away_spread_odds",
        "home_spread_odds",
        "over_edge",
        "under_edge",
        "over_odds",
        "under_odds",
        "total",
        "bet"
    ]

    output_cols = [
        "date",
        "game_id",
        "risk_amount",
        "max_potential_win",
        "bet_result",
        "amount_won_or_lost",
        "odds_american",
        "clv_percent",
        "leg_type",
        "bet_on",
        "bet_on_spread_total_number",
        "leg_sport",
        "leg_league",
        "leg_vig",
        "away_team",
        "home_team",
        "league",
        "home_ml_edge",
        "away_ml_edge",
        "away_ml_odds",
        "home_ml_odds",
        "away_spread",
        "home_spread",
        "home_spread_edge",
        "away_spread_edge",
        "away_spread_odds",
        "home_spread_odds",
        "over_edge",
        "under_edge",
        "over_odds",
        "under_odds",
        "total",
        "bet"
    ]

    for file_path in input_files:
        try:
            total_files += 1
            df = pd.read_csv(file_path)
            rows = len(df)
            total_rows += rows

            if "game_id" not in df.columns or "league" not in df.columns:
                log(f"ERROR: {file_path} missing required columns")
                total_unmatched += rows
                continue

            # MERGE ON game_id + league
            merged = df.merge(
                winners_df,
                on=["game_id", "league"],
                how="left",
                suffixes=("", "_w")
            )

            # Overwrite from winners
            for col in winners_fields:
                col_w = f"{col}_w"
                if col_w in merged.columns:
                    merged[col] = merged[col_w]

            # Count matches
            matched_mask = merged["home_ml_edge_w"].notna() if "home_ml_edge_w" in merged.columns else merged["league"].notna()
            matched = int(matched_mask.sum())
            unmatched = rows - matched

            total_matched += matched
            total_unmatched += unmatched

            # Ensure schema
            for col in output_cols:
                if col not in merged.columns:
                    merged[col] = ""

            merged = merged[output_cols]

            output_path = OUTPUT_DIR / Path(file_path).name
            merged.to_csv(output_path, index=False)

            log(f"Wrote {output_path} | rows={rows} matched={matched} unmatched={unmatched}")

        except Exception:
            log(f"ERROR processing {file_path}")
            log(traceback.format_exc())

    log(f"Files processed: {total_files}")
    log(f"Rows processed: {total_rows}")
    log(f"Rows matched: {total_matched}")
    log(f"Rows unmatched: {total_unmatched}")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    process()
