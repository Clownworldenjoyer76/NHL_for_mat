#!/usr/bin/env python3

import glob
import traceback
from datetime import datetime
from pathlib import Path
import sys
import math

import pandas as pd

INPUT_DIR = Path("docs/win/hockey/01_merge")
OUTPUT_DIR = Path("docs/win/hockey/02_juice")
JUICE_FILE = Path("config/hockey/nhl/nhl_puck_line_juice.csv")

ERROR_DIR = Path("docs/win/hockey/errors/02_juice")
LOG_FILE = ERROR_DIR / "apply_puck_line_juice.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


def _now():
    return datetime.utcnow().isoformat() + "Z"


def _log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")


def find_band_row(juice_df: pd.DataFrame, puck_line: float, venue: str):
    band_low = juice_df[["band_min", "band_max"]].min(axis=1)
    band_high = juice_df[["band_min", "band_max"]].max(axis=1)

    mask = (band_low <= puck_line) & (puck_line <= band_high) & (juice_df["venue"] == venue)
    band = juice_df[mask]

    if band.empty:
        return None

    if len(band) > 1:
        _log(f"[WARN] Multiple juice rows matched puck_line={puck_line}, venue={venue}. Using first.")

    return float(band.iloc[0]["extra_juice"])


def process_side(df: pd.DataFrame, juice_df: pd.DataFrame, side: str):
    puck_col = f"{side}_puck_line"
    fair_col = f"{side}_fair_puck_line_decimal"

    juiced_decimal_col = f"{side}_juiced_decimal_puck_line"
    juiced_prob_col = f"{side}_juiced_prob_puck_line"

    df[juiced_decimal_col] = pd.NA
    df[juiced_prob_col] = pd.NA

    required_cols = [puck_col, fair_col]
    for c in required_cols:
        if c not in df.columns:
            raise KeyError(f"Missing required column in input: {c}")

    applied = 0
    skipped_no_band = 0
    skipped_bad_row = 0

    for idx, row in df.iterrows():
        try:
            puck_line = round(float(row[puck_col]), 4)
            fair_decimal = float(row[fair_col])
        except Exception:
            skipped_bad_row += 1
            continue

        if not math.isfinite(fair_decimal) or fair_decimal <= 1:
            skipped_bad_row += 1
            continue

        extra = find_band_row(juice_df, puck_line, side)
        if extra is None:
            skipped_no_band += 1
            continue

        # ✅ Corrected: Probability-based adjustment
        try:
            fair_prob = 1 / fair_decimal
            juiced_prob = fair_prob * (1 - extra)

            if not math.isfinite(juiced_prob) or juiced_prob <= 0:
                juiced_prob = 1e-6

            juiced_decimal = 1 / juiced_prob
        except Exception:
            skipped_bad_row += 1
            continue

        if not math.isfinite(juiced_decimal) or juiced_decimal <= 1:
            juiced_decimal = 1.0001

        df.at[idx, juiced_decimal_col] = juiced_decimal
        df.at[idx, juiced_prob_col] = juiced_prob
        applied += 1

    return df, applied, skipped_no_band, skipped_bad_row


def main():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== APPLY PUCK LINE JUICE START {_now()} ===\n")

    try:
        _log(f"[INFO] JUICE_FILE: {JUICE_FILE}")
        _log(f"[INFO] INPUT_DIR: {INPUT_DIR}")
        _log(f"[INFO] OUTPUT_DIR: {OUTPUT_DIR}")

        juice_df = pd.read_csv(JUICE_FILE)

        _log(f"[INFO] Juice columns: {list(juice_df.columns)}")
        _log(f"[INFO] Juice rows: {len(juice_df)}")

        expected = ["band", "band_min", "band_max", "venue", "extra_juice"]
        missing = [c for c in expected if c not in juice_df.columns]
        if missing:
            raise KeyError(f"JUICE_FILE missing columns: {missing}")

        juice_df["band_min"] = juice_df["band_min"].astype(float)
        juice_df["band_max"] = juice_df["band_max"].astype(float)
        juice_df["venue"] = juice_df["venue"].astype(str).str.strip()
        juice_df["extra_juice"] = juice_df["extra_juice"].astype(float)

        _log("[INFO] Juice config (normalized) preview:")
        _log(juice_df.to_string(index=False))

        pattern = str(INPUT_DIR / "*_NHL_puck_line.csv")
        files = sorted(glob.glob(pattern))
        _log(f"[INFO] Glob pattern: {pattern}")
        _log(f"[INFO] Files found: {len(files)}")
        for fp in files:
            _log(f"  - {fp}")

        if not files:
            raise ValueError(f"No input files matched pattern: {pattern}")

        for file_path in files:
            in_path = Path(file_path)
            out_path = OUTPUT_DIR / in_path.name

            _log(f"\n=== FILE START {_now()} ===")
            _log(f"[INFO] Input file: {in_path}")
            _log(f"[INFO] Output file: {out_path}")

            df = pd.read_csv(in_path)

            _log(f"[INFO] Input rows: {len(df)}")
            _log(f"[INFO] Input columns: {list(df.columns)}")

            for side in ["home", "away"]:
                col = f"{side}_puck_line"
                if col in df.columns:
                    vals = df[col].dropna().astype(float).round(4).value_counts().to_dict()
                    _log(f"[INFO] {col} value_counts: {vals}")

            df, home_applied, home_no_band, home_bad = process_side(df, juice_df, "home")
            df, away_applied, away_no_band, away_bad = process_side(df, juice_df, "away")

            _log(f"[INFO] Home applied={home_applied} no_band={home_no_band} bad_rows={home_bad}")
            _log(f"[INFO] Away applied={away_applied} no_band={away_no_band} bad_rows={away_bad}")

            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            df.to_csv(out_path, index=False)

            df2 = pd.read_csv(out_path)
            _log(f"[INFO] Wrote output rows: {len(df2)}")
            _log(f"[INFO] Output columns now include: "
                 f"home_juiced_decimal_puck_line={'home_juiced_decimal_puck_line' in df2.columns}, "
                 f"away_juiced_decimal_puck_line={'away_juiced_decimal_puck_line' in df2.columns}")

            sample_cols = [c for c in [
                "game_id",
                "home_puck_line", "away_puck_line",
                "home_juiced_decimal_puck_line", "home_juiced_prob_puck_line",
                "away_juiced_decimal_puck_line", "away_juiced_prob_puck_line",
            ] if c in df2.columns]

            if sample_cols:
                _log("[INFO] Output sample (first 10):")
                _log(df2[sample_cols].head(10).to_string(index=False))

            _log(f"=== FILE END {_now()} ===")

        _log(f"\n=== APPLY PUCK LINE JUICE END {_now()} ===")

    except Exception as e:
        _log("\n=== ERROR ===")
        _log(str(e))
        _log(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
