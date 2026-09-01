#!/usr/bin/env python3

import csv
from pathlib import Path
import pandas as pd
from datetime import datetime
import traceback

# =========================
# PATHS
# =========================

NORMALIZED_DIR = Path("docs/win/manual/normalized")
CLEANED_DIR = Path("docs/win/manual/cleaned")

ERROR_DIR = Path("docs/win/errors/03_dk_iv")
ERROR_LOG = ERROR_DIR / "dk_05.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def log(msg: str):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def parse_filename(path: Path):
    parts = path.stem.split("_")
    if len(parts) < 6:
        return None
    _, league, market, year, month, day = parts
    return league, market, f"{year}_{month}_{day}"

# =========================
# CORE
# =========================

def process_file(norm_path: Path):
    try:
        parsed = parse_filename(norm_path)
        if not parsed:
            return

        league, market, date = parsed

        if market == "spreads":
            cleaned_path = CLEANED_DIR / f"dk_{league}_spreads_{date}.csv"
        elif market == "moneyline":
            cleaned_path = CLEANED_DIR / f"dk_{league}_moneyline_{date}.csv"
        else:
            return

        if not cleaned_path.exists():
            log(f"{norm_path.name} | CLEANED FILE NOT FOUND: {cleaned_path.name}")
            return

        norm_df = pd.read_csv(norm_path, dtype=str)
        cleaned_df = pd.read_csv(cleaned_path, dtype=str)

        if norm_df.empty:
            log(f"{norm_path.name} | EMPTY NORMALIZED FILE")
            return

        matched_rows = 0
        missing_rows = 0

        if market == "spreads":
            norm_df["away_spread"] = ""
            norm_df["home_spread"] = ""
        elif market == "moneyline":
            norm_df["away_odds"] = ""
            norm_df["home_odds"] = ""

        for idx, row in norm_df.iterrows():
            away_team = row.get("away_team")
            home_team = row.get("home_team")

            away_row = cleaned_df[cleaned_df["team"] == away_team]
            home_row = cleaned_df[cleaned_df["team"] == home_team]

            if away_row.empty or home_row.empty:
                missing_rows += 1
                continue

            away_row = away_row.iloc[0]
            home_row = home_row.iloc[0]

            if market == "spreads":
                norm_df.at[idx, "away_spread"] = away_row.get("spread", "")
                norm_df.at[idx, "home_spread"] = home_row.get("spread", "")
            elif market == "moneyline":
                norm_df.at[idx, "away_odds"] = away_row.get("odds", "")
                norm_df.at[idx, "home_odds"] = home_row.get("odds", "")

            matched_rows += 1

        norm_df.to_csv(norm_path, index=False)

        log(
            f"{norm_path.name} | matched={matched_rows} "
            f"missing={missing_rows}"
        )

    except Exception as e:
        log(f"FILE ERROR: {norm_path.name}")
        log(str(e))
        log(traceback.format_exc())
        log("-" * 80)

# =========================
# MAIN
# =========================

def main():
    ERROR_LOG.write_text("", encoding="utf-8")
    log("DK_05 START")
    log(f"Run timestamp (UTC): {datetime.utcnow().isoformat()}")

    for norm_path in NORMALIZED_DIR.glob("dk_*_*.csv"):
        process_file(norm_path)

    log("DK_05 END")

if __name__ == "__main__":
    main()
