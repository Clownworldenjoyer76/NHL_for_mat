# scripts/winners_02.py

#!/usr/bin/env python3

import pandas as pd
import glob
from pathlib import Path
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/winners/step_01")
OUTPUT_DIR = Path("docs/win/winners/step_02")
NORMALIZED_DIR = Path("docs/win/manual/normalized")

ERROR_DIR = Path("docs/win/errors/09_winners")
ERROR_LOG = ERROR_DIR / "winners_02.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def to_american(decimal_odds):
    if pd.isna(decimal_odds) or decimal_odds <= 1:
        return ""
    if decimal_odds >= 2.0:
        return int((decimal_odds - 1) * 100)
    return int(-100 / (decimal_odds - 1))


def load_normalized_time_map():
    """
    Build mapping: game_id -> time
    """
    mapping = {}
    files = glob.glob(str(NORMALIZED_DIR / "*.csv"))

    for file_path in files:
        try:
            df = pd.read_csv(file_path)
            if "game_id" in df.columns and "time" in df.columns:
                for _, row in df.iterrows():
                    mapping[row["game_id"]] = row["time"]
        except Exception:
            continue

    return mapping


# =========================
# MAIN
# =========================

def process_files():

    summary = []
    summary.append(f"=== WINNERS_02 RUN @ {datetime.utcnow().isoformat()}Z ===")

    normalized_time_map = load_normalized_time_map()

    patterns = [
        "winners_nba_ml_*.csv",
        "winners_ncaab_ml_*.csv",
        "winners_nba_spreads_*.csv",
        "winners_ncaab_spreads_*.csv",
        "winners_nba_totals_*.csv",
        "winners_ncaab_totals_*.csv",
    ]

    input_files = []
    for p in patterns:
        input_files.extend(glob.glob(str(INPUT_DIR / p)))

    files_processed = 0
    rows_processed = 0
    times_updated = 0

    for file_path in input_files:
        try:
            df = pd.read_csv(file_path)

            # =========================
            # UPDATE TIME
            # =========================
            if "game_id" in df.columns and "time" in df.columns:
                for idx, row in df.iterrows():
                    gid = row["game_id"]
                    if gid in normalized_time_map:
                        new_time = normalized_time_map[gid]
                        if row["time"] != new_time:
                            df.at[idx, "time"] = new_time
                            times_updated += 1

            filename = Path(file_path).name.lower()

            # =========================
            # MONEYLINE
            # =========================
            if "_ml_" in filename:

                df["home_ml_edge"] = ""
                df["away_ml_edge"] = ""
                df["away_ml_odds"] = ""
                df["home_ml_odds"] = ""

                if all(col in df.columns for col in [
                    "deci_dk_home_odds",
                    "deci_home_ml_juice_odds"
                ]):
                    df["home_ml_edge"] = (
                        df["deci_dk_home_odds"] -
                        df["deci_home_ml_juice_odds"]
                    )

                if all(col in df.columns for col in [
                    "deci_dk_away_odds",
                    "deci_away_ml_juice_odds"
                ]):
                    df["away_ml_edge"] = (
                        df["deci_dk_away_odds"] -
                        df["deci_away_ml_juice_odds"]
                    )

                if "deci_dk_away_odds" in df.columns:
                    df["away_ml_odds"] = df["deci_dk_away_odds"].apply(to_american)

                if "deci_dk_home_odds" in df.columns:
                    df["home_ml_odds"] = df["deci_dk_home_odds"].apply(to_american)

            # =========================
            # SPREADS
            # =========================
            elif "_spreads_" in filename:

                df["home_spread_edge"] = ""
                df["away_spread_edge"] = ""
                df["away_spread_odds"] = ""
                df["home_spread_odds"] = ""

                if all(col in df.columns for col in [
                    "deci_dk_home_odds",
                    "deci_home_spread_juice_odds"
                ]):
                    df["home_spread_edge"] = (
                        df["deci_dk_home_odds"] -
                        df["deci_home_spread_juice_odds"]
                    )

                if all(col in df.columns for col in [
                    "deci_dk_away_odds",
                    "deci_away_spread_juice_odds"
                ]):
                    df["away_spread_edge"] = (
                        df["deci_dk_away_odds"] -
                        df["deci_away_spread_juice_odds"]
                    )

                if "deci_dk_away_odds" in df.columns:
                    df["away_spread_odds"] = df["deci_dk_away_odds"].apply(to_american)

                if "deci_dk_home_odds" in df.columns:
                    df["home_spread_odds"] = df["deci_dk_home_odds"].apply(to_american)

            # =========================
            # TOTALS
            # =========================
            elif "_totals_" in filename:

                df["over_edge"] = ""
                df["under_edge"] = ""
                df["over_odds"] = ""
                df["under_odds"] = ""

                if all(col in df.columns for col in [
                    "deci_dk_over_odds",
                    "deci_over_juice_odds"
                ]):
                    df["over_edge"] = (
                        df["deci_dk_over_odds"] -
                        df["deci_over_juice_odds"]
                    )

                if all(col in df.columns for col in [
                    "deci_dk_under_odds",
                    "deci_under_juice_odds"
                ]):
                    df["under_edge"] = (
                        df["deci_dk_under_odds"] -
                        df["deci_under_juice_odds"]
                    )

                if "deci_dk_over_odds" in df.columns:
                    df["over_odds"] = df["deci_dk_over_odds"].apply(to_american)

                if "deci_dk_under_odds" in df.columns:
                    df["under_odds"] = df["deci_dk_under_odds"].apply(to_american)

            # =========================
            # OUTPUT
            # =========================

            output_path = OUTPUT_DIR / Path(file_path).name
            df.to_csv(output_path, index=False)

            files_processed += 1
            rows_processed += len(df)

        except Exception:
            summary.append(f"ERROR processing {file_path}")
            summary.append(traceback.format_exc())

    summary.append(f"Files processed: {files_processed}")
    summary.append(f"Rows processed: {rows_processed}")
    summary.append(f"Times updated: {times_updated}")

    with open(ERROR_LOG, "w") as f:
        f.write("\n".join(summary))


if __name__ == "__main__":
    process_files()
