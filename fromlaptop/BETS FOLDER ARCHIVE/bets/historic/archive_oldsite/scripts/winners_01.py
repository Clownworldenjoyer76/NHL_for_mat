import shutil
import glob
from pathlib import Path
from datetime import datetime

# =========================
# PATHS
# =========================

STEP3_BASE = Path("docs/win/final/step_3")
OUTPUT_BASE = Path("docs/win/winners/step_01")
ERROR_DIR = Path("docs/win/errors/09_winners")

OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = ERROR_DIR / "winners_01.txt"

FILES_PROCESSED = 0
FILES_WRITTEN = 0

# =========================
# HELPERS
# =========================

def build_output_path(input_path: Path) -> Path:
    """
    Convert:
    docs/win/final/step_3/nba/ml/juice_nba_ml_2026_02_10.csv

    To:
    docs/win/winners/step_01/winners_nba_ml_2026_02_10.csv
    """
    name = input_path.name.replace("juice_", "winners_")
    return OUTPUT_BASE / name


def copy_files(pattern: str):
    global FILES_PROCESSED, FILES_WRITTEN

    for file_path in glob.glob(pattern):
        FILES_PROCESSED += 1
        src = Path(file_path)

        if not src.exists():
            raise RuntimeError(f"Missing input file: {src}")

        dest = build_output_path(src)

        shutil.copy2(src, dest)
        FILES_WRITTEN += 1

        print(f"Copied {src} -> {dest}")


# =========================
# MAIN
# =========================

def run():

    patterns = [
        STEP3_BASE / "nba/ml/juice_nba_ml_*.csv",
        STEP3_BASE / "nba/spreads/juice_nba_spreads_*.csv",
        STEP3_BASE / "nba/totals/juice_nba_totals_*.csv",
        STEP3_BASE / "ncaab/ml/juice_ncaab_ml_*.csv",
        STEP3_BASE / "ncaab/spreads/juice_ncaab_spreads_*.csv",
        STEP3_BASE / "ncaab/totals/juice_ncaab_totals_*.csv",
    ]

    for pattern in patterns:
        copy_files(str(pattern))

    print("\n=== WINNERS_01 SUMMARY ===")
    print(f"Files processed: {FILES_PROCESSED}")
    print(f"Files written: {FILES_WRITTEN}")

    if FILES_PROCESSED == 0:
        raise RuntimeError("winners_01: 0 files processed")

    if FILES_WRITTEN != FILES_PROCESSED:
        raise RuntimeError("winners_01: mismatch between processed and written files")


if __name__ == "__main__":
    run()
