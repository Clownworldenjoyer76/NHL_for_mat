# scripts/my_bets_clean_04.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import glob
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/my_bets/step_03")
MAPPINGS_DIR = Path("mappings")
ERROR_DIR = Path("docs/win/errors/01_raw")
ERROR_LOG = ERROR_DIR / "my_bets_clean_04.txt"  # overwrite

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# MAIN
# =========================

def process_files():
    files = glob.glob(str(INPUT_DIR / "juiceReelBets_*.csv"))

    files_processed = 0
    rows_total = 0
    no_match_records = []

    try:
        for file_path in files:
            df = pd.read_csv(file_path)
            rows_total += len(df)

            for idx, row in df.iterrows():
                league_value = str(row.get("league", "")).strip()
                map_file = MAPPINGS_DIR / f"team_map_{league_value}.csv"

                if not map_file.exists():
                    no_match_records.append(
                        f"{league_value} | mapping file not found"
                    )
                    continue

                map_df = pd.read_csv(map_file)

                alias_to_canonical = dict(
                    zip(
                        map_df["alias"].astype(str).str.strip(),
                        map_df["canonical_team"].astype(str).str.strip(),
                    )
                )

                # Normalize away_team
                away = str(row.get("away_team", "")).strip()
                if away in alias_to_canonical:
                    df.at[idx, "away_team"] = alias_to_canonical[away]
                else:
                    no_match_records.append(
                        f"{league_value} | away_team no match in mapping file: {away}"
                    )

                # Normalize home_team
                home = str(row.get("home_team", "")).strip()
                if home in alias_to_canonical:
                    df.at[idx, "home_team"] = alias_to_canonical[home]
                else:
                    no_match_records.append(
                        f"{league_value} | home_team no match in mapping file: {home}"
                    )

                # Normalize bet_on
                bet_on_value = str(row.get("bet_on", "")).strip()

                if bet_on_value == "Over":
                    df.at[idx, "bet_on"] = "over"
                elif bet_on_value == "Under":
                    df.at[idx, "bet_on"] = "under"
                else:
                    if bet_on_value in alias_to_canonical:
                        df.at[idx, "bet_on"] = alias_to_canonical[bet_on_value]
                    else:
                        no_match_records.append(
                            f"{league_value} | bet_on no match in mapping file: {bet_on_value}"
                        )

            # Overwrite same file
            df.to_csv(file_path, index=False)

            files_processed += 1

        # =========================
        # WRITE SUMMARY / ERROR LOG (OVERWRITE)
        # =========================

        with open(ERROR_LOG, "w") as log:
            log.write("MY_BETS_CLEAN_04 SUMMARY\n")
            log.write("=========================\n\n")
            log.write(f"Files processed: {files_processed}\n")
            log.write(f"Rows processed: {rows_total}\n\n")

            if no_match_records:
                log.write("NO MATCH IN MAPPING FILE:\n")
                for record in no_match_records:
                    log.write(record + "\n")
            else:
                log.write("All teams matched successfully.\n")

    except Exception as e:
        with open(ERROR_LOG, "w") as log:
            log.write("ERROR DURING PROCESSING\n")
            log.write(str(e) + "\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    process_files()
