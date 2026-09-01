#!/usr/bin/env python3
# docs/win/final_scores/scripts/00_parsing/dedupe.py

import csv
from pathlib import Path
import traceback

BASE_DIR = Path("docs/win/final_scores")
ERROR_DIR = BASE_DIR / "errors"
ERROR_DIR.mkdir(parents=True, exist_ok=True)
ERROR_LOG = ERROR_DIR / "dedupe.txt"


def main():
    with open(ERROR_LOG, "w") as log:
        try:
            files = sorted(BASE_DIR.glob("*_final_scores_*.csv"))

            if not files:
                log.write("No final score files found.\n")
                return

            for path in files:

                with path.open("r", newline="", encoding="utf-8") as f:
                    reader = list(csv.DictReader(f))

                if not reader:
                    log.write(f"{path.name}: empty file\n")
                    continue

                seen = set()
                deduped = []

                for row in reader:
                    key = (
                        row.get("game_date"),
                        row.get("market"),
                        row.get("away_team"),
                        row.get("home_team"),
                    )

                    if key not in seen:
                        seen.add(key)
                        deduped.append(row)

                with path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=reader[0].keys())
                    writer.writeheader()
                    writer.writerows(deduped)

                log.write(f"{path.name}: {len(reader)} -> {len(deduped)} rows\n")

        except Exception as e:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    main()
