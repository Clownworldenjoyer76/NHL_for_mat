#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import sys
import re

# =========================
# CONFIG
# =========================

INPUT_PATTERNS = [
    "docs/win/nba/moneyline/*.csv",
    "docs/win/nba/spreads/*.csv",
    "docs/win/nba/totals/*.csv",
    "docs/win/ncaab/moneyline/*.csv",
    "docs/win/ncaab/spreads/*.csv",
    "docs/win/ncaab/totals/*.csv",
]

EXPLICIT_COLUMNS = {
    "away_odds",
    "home_odds",
    "over_odds",
    "under_odds",
    "away_spread_acceptable_american_odds",
    "home_spread_acceptable_american_odds",
    "over_acceptable_american_odds",
    "under_acceptable_american_odds",
    "away_ml_fair_american_odds",
    "away_ml_acceptable_american_odds",
    "home_ml_fair_american_odds",
    "home_ml_acceptable_american_odds",
}

ERROR_DIR = Path("docs/win/errors/07_juice")
ERROR_LOG = ERROR_DIR / "juice_pre_clean_summary.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def normalize_american(val):
    """
    Convert '+130', '-110', '130', -110 → int
    Leave blanks / NaN untouched
    """
    if pd.isna(val):
        return val

    s = str(val).strip()
    if s == "":
        return val

    # strip leading +
    s = re.sub(r"^\+", "", s)

    try:
        return int(float(s))
    except ValueError:
        return val

def should_normalize(col):
    return (
        "american_odds" in col
        or col in EXPLICIT_COLUMNS
    )

# =========================
# MAIN
# =========================

def main():
    total_files = 0
    total_columns = 0
    total_values = 0

    for pattern in INPUT_PATTERNS:
        for path in Path().glob(pattern):
            if not path.exists():
                continue

            df = pd.read_csv(path)
            modified = False

            for col in df.columns:
                if not should_normalize(col):
                    continue

                total_columns += 1
                before = df[col].copy()

                df[col] = df[col].apply(normalize_american)

                changed = (before != df[col]).sum()
                if changed > 0:
                    total_values += int(changed)
                    modified = True

            if modified:
                df.to_csv(path, index=False)

            total_files += 1

    summary = (
        "JUICE PRE-CLEAN SUMMARY\n"
        "======================\n"
        f"Files processed: {total_files}\n"
        f"Columns normalized: {total_columns}\n"
        f"Values normalized: {total_values}\n"
    )

    print("\n" + summary)
    ERROR_LOG.write_text(summary, encoding="utf-8")

# =========================
# ENTRY
# =========================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        msg = f"JUICE PRE-CLEAN FAILED\n{e}\n"
        print(msg, file=sys.stderr)
        ERROR_LOG.write_text(msg, encoding="utf-8")
        sys.exit(1)
