#!/usr/bin/env python3

import csv
from pathlib import Path

INPUT_DIR = Path("docs/win/edge")
OUTPUT_DIR = Path("docs/win/final")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Updated personal juice rules (Version 1.3 – soccer ML)
# (min_probability, max_probability, american_odds_addition)
PERSONAL_JUICE_RULES = [
    (0.65, 1.01, 5),     # p ≥ 0.65
    (0.55, 0.65, 5),     # 0.55 ≤ p < 0.65
    (0.50, 0.55, 10),    # 0.50 ≤ p < 0.55
    (0.45, 0.50, 15),    # 0.45 ≤ p < 0.50
    (0.40, 0.45, 30),    # 0.40 ≤ p < 0.45
    (0.35, 0.40, 60),    # 0.35 ≤ p < 0.40
    (0.25, 0.35, 100),   # 0.25 ≤ p < 0.35
    (0.00, 0.25, 160),   # p < 0.25
]


def get_personal_juice(probability: float) -> int:
    for min_p, max_p, juice in PERSONAL_JUICE_RULES:
        if min_p <= probability < max_p:
            return juice
    return 0


def main():
    edge_files = sorted(INPUT_DIR.glob("edge_soc_*.csv"))
    if not edge_files:
        raise RuntimeError("No edge_soc_*.csv files found")

    for edge_path in edge_files:
        out_name = edge_path.name.replace("edge_", "final_")
        out_path = OUTPUT_DIR / out_name

        with edge_path.open(newline="", encoding="utf-8") as infile, \
             out_path.open("w", newline="", encoding="utf-8") as outfile:

            reader = csv.DictReader(infile)

            required = {
                "bet_type",
                "acceptable_american_odds",
                "win_probability",
                "draw_probability",
            }
            missing = required - set(reader.fieldnames)
            if missing:
                raise ValueError(f"Missing required columns: {missing}")

            fieldnames = list(reader.fieldnames) + [
                "personally_acceptable_american_odds"
            ]

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                bet_type = row["bet_type"]

                if bet_type == "win":
                    p = float(row["win_probability"])
                elif bet_type == "draw":
                    p = float(row["draw_probability"])
                else:
                    raise ValueError(f"Unknown bet_type: {bet_type}")

                base_acceptable = int(row["acceptable_american_odds"])
                personal_juice = get_personal_juice(p)

                row["personally_acceptable_american_odds"] = (
                    base_acceptable + personal_juice
                )

                writer.writerow(row)


if __name__ == "__main__":
    main()
