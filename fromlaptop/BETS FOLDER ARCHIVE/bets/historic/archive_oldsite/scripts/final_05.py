import pandas as pd
import glob
from pathlib import Path
import math

BASE = Path("docs/win/final/step_2")

INVALID_THRESHOLD = 0.20

FILES_PROCESSED = 0
TOTAL_ROWS = 0
TOTAL_INVALID = 0
TOTAL_CELLS_ATTEMPTED = 0


def american_to_decimal(a):
    try:
        a = float(a)

        if not math.isfinite(a) or a == 0:
            return pd.NA

        return 1 + (a / 100 if a > 0 else 100 / abs(a))

    except Exception:
        return pd.NA


def apply_decimal_columns(files, mapping):
    global FILES_PROCESSED, TOTAL_ROWS, TOTAL_INVALID, TOTAL_CELLS_ATTEMPTED

    for f in files:
        df = pd.read_csv(f)

        FILES_PROCESSED += 1
        rows = len(df)
        TOTAL_ROWS += rows

        file_invalid = 0

        for src_col, out_col in mapping.items():
            if src_col not in df.columns:
                continue

            df[out_col] = df[src_col].apply(american_to_decimal)

            invalid = df[out_col].isna().sum()
            file_invalid += invalid
            TOTAL_CELLS_ATTEMPTED += rows

        TOTAL_INVALID += file_invalid

        df.to_csv(f, index=False)

        print(
            f"Updated {f} | rows={rows} | invalid_conversions={file_invalid}"
        )


def run():
    apply_decimal_columns(
        glob.glob(str(BASE / "*/ml/juice_*_ml_*.csv")),
        {
            "home_ml_juice_odds": "deci_home_ml_juice_odds",
            "away_ml_juice_odds": "deci_away_ml_juice_odds",
            "dk_away_odds": "deci_dk_away_odds",
            "dk_home_odds": "deci_dk_home_odds",
        },
    )

    apply_decimal_columns(
        glob.glob(str(BASE / "*/spreads/juice_*_spreads_*.csv")),
        {
            "away_spread_juice_odds": "deci_away_spread_juice_odds",
            "home_spread_juice_odds": "deci_home_spread_juice_odds",
            "dk_away_odds": "deci_dk_away_odds",
            "dk_home_odds": "deci_dk_home_odds",
        },
    )

    apply_decimal_columns(
        glob.glob(str(BASE / "*/totals/juice_*_totals_*.csv")),
        {
            "over_juice_odds": "deci_over_juice_odds",
            "under_juice_odds": "deci_under_juice_odds",
            "over_odds": "deci_dk_over_odds",
            "under_odds": "deci_dk_under_odds",
        },
    )

    invalid_rate = (
        TOTAL_INVALID / TOTAL_CELLS_ATTEMPTED
        if TOTAL_CELLS_ATTEMPTED
        else 0
    )

    print("\n=== FINAL_05 SUMMARY ===")
    print(f"Files processed: {FILES_PROCESSED}")
    print(f"Total rows: {TOTAL_ROWS}")
    print(f"Total invalid conversions: {TOTAL_INVALID}")
    print(f"Total cells attempted: {TOTAL_CELLS_ATTEMPTED}")
    print(f"Invalid rate: {invalid_rate:.2%}")

    if FILES_PROCESSED == 0:
        raise RuntimeError("final_05: 0 files processed")

    if TOTAL_ROWS == 0:
        raise RuntimeError("final_05: 0 rows processed")

    if invalid_rate > INVALID_THRESHOLD:
        raise RuntimeError(
            f"final_05: Invalid conversion rate too high ({invalid_rate:.2%})"
        )


if __name__ == "__main__":
    run()
