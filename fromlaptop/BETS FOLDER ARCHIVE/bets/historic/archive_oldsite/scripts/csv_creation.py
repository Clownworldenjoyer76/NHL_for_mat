#scripts/csv_creation.py

#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
from datetime import datetime

# =========================
# PATHS
# =========================

INPUT_DIR = Path("testing")
OUTPUT_DIR = INPUT_DIR / "csvs"

DUMP_INPUT_DIR = Path("docs/win/dump")
DUMP_OUTPUT_DIR = DUMP_INPUT_DIR / "csvs"

ERROR_DIR = Path("docs/win/errors/00_ConvertXLSX")
ERROR_LOG = ERROR_DIR / "csv_creation.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DUMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# CORE LOGIC
# =========================

def convert_directory(input_dir: Path, output_dir: Path, log_lines: list):
    xlsx_files = list(input_dir.glob("*.xlsx"))

    if not xlsx_files:
        log_lines.append(f"No .xlsx files found in {input_dir}/")
        return 0, 0, 0

    found = len(xlsx_files)
    converted = 0
    failed = 0

    for xlsx_path in xlsx_files:
        try:
            df = pd.read_excel(xlsx_path, engine="openpyxl")
            output_path = output_dir / f"{xlsx_path.stem}.csv"
            df.to_csv(output_path, index=False)
            log_lines.append(f"Converted: {xlsx_path} -> {output_path}")
            converted += 1
        except Exception as e:
            log_lines.append(f"FAILED: {xlsx_path.name} | {e}")
            failed += 1

    return found, converted, failed


def convert_xlsx_to_csv():
    log_lines = []
    timestamp = datetime.utcnow().isoformat()

    log_lines.append("CSV CREATION RUN")
    log_lines.append("================")
    log_lines.append(f"Timestamp (UTC): {timestamp}\n")

    total_found = total_converted = total_failed = 0

    f, c, fa = convert_directory(INPUT_DIR, OUTPUT_DIR, log_lines)
    total_found += f
    total_converted += c
    total_failed += fa

    f, c, fa = convert_directory(DUMP_INPUT_DIR, DUMP_OUTPUT_DIR, log_lines)
    total_found += f
    total_converted += c
    total_failed += fa

    log_lines.append("\nSUMMARY")
    log_lines.append("-------")
    log_lines.append(f"Files found: {total_found}")
    log_lines.append(f"Files converted: {total_converted}")
    log_lines.append(f"Files failed: {total_failed}")

    ERROR_LOG.write_text("\n".join(log_lines), encoding="utf-8")


if __name__ == "__main__":
    convert_xlsx_to_csv()
