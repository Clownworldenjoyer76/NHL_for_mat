# scripts/dk_01.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import traceback

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/manual/first")
OUTPUT_DIR = Path("docs/win/manual/cleaned")

ERROR_DIR = Path("docs/win/errors/02_dk_prep")
ERROR_LOG = ERROR_DIR / "dk_01.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def log(msg: str):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def american_to_decimal(odds: float) -> float:
    return 1 + odds / 100 if odds > 0 else 1 + 100 / abs(odds)

# =========================
# CORE
# =========================

def process_file(path: Path):
    try:
        df = pd.read_csv(path)
        rows_in = len(df)

        parts = path.stem.split("_")
        if len(parts) < 4:
            raise ValueError(f"Invalid filename: {path.name}")

        _, league, market, *_ = parts
        df["league"] = f"{league}_{market}"

        df["odds"] = (
            df["odds"].astype(str)
            .str.replace("−", "-", regex=False)
            .str.lstrip("+")
        )
        df["decimal_odds"] = df["odds"].astype(float).apply(american_to_decimal)

        for col in ("handle_pct", "bets_pct"):
            if col in df.columns:
                df[col] = df[col].astype(float) / 100.0

        df["game_id"] = ""

        out_path = OUTPUT_DIR / path.name
        df.to_csv(out_path, index=False)

        log(f"{path.name} | rows_in={rows_in} rows_out={len(df)}")

    except Exception as e:
        log(f"FILE ERROR: {path.name}")
        log(str(e))
        log(traceback.format_exc())
        log("-" * 80)

# =========================
# MAIN
# =========================

def main():
    # Overwrite log file at start of run
    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("")

    log("DK_01 START")
    for file in INPUT_DIR.glob("dk_*_*.csv"):
        process_file(file)
    log("DK_01 END\n")

if __name__ == "__main__":
    main()
