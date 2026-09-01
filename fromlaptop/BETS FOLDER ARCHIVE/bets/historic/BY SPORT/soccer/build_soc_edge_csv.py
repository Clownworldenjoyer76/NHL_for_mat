#!/usr/bin/env python3

import csv
from pathlib import Path

EDGE_MULTIPLIER = 1.05

INPUT_GLOB = "docs/win/clean/win_prob__clean_soc_*"
OUTPUT_DIR = Path("docs/win/edge")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def decimal_to_american(decimal):
    if decimal >= 2:
        return int(round((decimal - 1) * 100))
    else:
        return int(round(-100 / (decimal - 1)))


for input_path in Path(".").glob(INPUT_GLOB):
    date_part = input_path.name.split("_")[-1].replace("-", "_").replace(".csv", "")
    output_path = OUTPUT_DIR / f"edge_soc_{date_part}.csv"

    with input_path.open(newline="", encoding="utf-8") as infile, \
         output_path.open("w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)

        if "bet_type" not in reader.fieldnames:
            raise ValueError("Missing required column: bet_type")

        fieldnames = list(reader.fieldnames) + [
            "fair_decimal_odds",
            "fair_american_odds",
            "acceptable_decimal_odds",
            "acceptable_american_odds",
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        last_game_id = None

        for row in reader:
            bet_type = row["bet_type"]

            if bet_type == "win":
                last_game_id = row.get("game_id")

                p = float(row["win_probability"])

            elif bet_type == "draw":
                if last_game_id is not None:
                    row["game_id"] = last_game_id

                p = float(row["draw_probability"])

            else:
                raise ValueError(f"Unknown bet_type: {bet_type}")

            fair_decimal = 1 / p
            acceptable_decimal = fair_decimal * EDGE_MULTIPLIER

            row["fair_decimal_odds"] = fair_decimal
            row["fair_american_odds"] = decimal_to_american(fair_decimal)
            row["acceptable_decimal_odds"] = acceptable_decimal
            row["acceptable_american_odds"] = decimal_to_american(acceptable_decimal)

            writer.writerow(row)
