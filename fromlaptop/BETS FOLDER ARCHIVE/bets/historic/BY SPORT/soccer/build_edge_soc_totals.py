#!/usr/bin/env python3
"""
Build Soccer Totals Evaluation File (Model-Anchored)

Rules:
- No live market odds assumed
- Use best_ou as the comparison total
- OVER if model total > best_ou
- UNDER if model total < best_ou
- NO PLAY if best_ou missing/invalid or totals equal
- Price every valid game
"""

import csv
import glob
from math import exp, factorial
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# -----------------------------
# PARAMETERS (UPDATED JUICE)
# -----------------------------
EDGE_BUFFER_TOTALS = 0.045  # increased from 0.035

HOME_MULT = 1.10
AWAY_MULT = 0.90

MIN_LAM = 0.5
MAX_LAM = 6.0

INPUT_DIR = Path("docs/win/clean")
OUTPUT_DIR = Path("docs/win/soc")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# MATH HELPERS
# -----------------------------
def poisson_cdf(k: int, lam: float) -> float:
    return sum((lam ** i) * exp(-lam) / factorial(i) for i in range(k + 1))


def fair_decimal(p: float) -> float:
    return 1.0 / p


def decimal_to_american(d: float) -> int:
    if d >= 2.0:
        return int(round((d - 1) * 100))
    return int(round(-100 / (d - 1)))


def acceptable_decimal(p: float, extra_buffer: float = 0.0) -> float:
    buffer = EDGE_BUFFER_TOTALS + extra_buffer
    return 1.0 / max(p - buffer, 0.0001)

# -----------------------------
# MAIN
# -----------------------------
def main():
    input_files = sorted(
        glob.glob(str(INPUT_DIR / "win_prob__clean_soc_*.csv"))
    )
    if not input_files:
        raise FileNotFoundError("No clean soccer win probability files found")

    latest_file = input_files[-1]

    today = datetime.utcnow()
    out_path = OUTPUT_DIR / f"edge_soc_totals_{today.year}_{today.month:02d}_{today.day:02d}.csv"

    games = defaultdict(list)

    with open(latest_file, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row.get("game_id"):
                games[row["game_id"]].append(row)

    with open(out_path, "w", newline="", encoding="utf-8") as outfile:
        fieldnames = [
            "game_id",
            "date",
            "time",
            "team_1",
            "team_2",
            "market_total",
            "side",
            "model_probability",
            "fair_decimal_odds",
            "fair_american_odds",
            "acceptable_decimal_odds",
            "acceptable_american_odds",
            "league",
        ]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for game_id, rows in games.items():

            if len(rows) != 2:
                writer.writerow({
                    "game_id": game_id,
                    "date": rows[0].get("date", ""),
                    "time": rows[0].get("time", ""),
                    "team_1": rows[0].get("team", ""),
                    "team_2": "",
                    "market_total": "",
                    "side": "NO PLAY",
                    "model_probability": "",
                    "fair_decimal_odds": "",
                    "fair_american_odds": "",
                    "acceptable_decimal_odds": "",
                    "acceptable_american_odds": "",
                    "league": "soc_ou",
                })
                continue

            team_1 = rows[0]["team"]
            team_2 = rows[1]["team"]
            date = rows[0]["date"]
            time = rows[0]["time"]

            market_total_raw = rows[0].get("best_ou", "")
            side = "NO PLAY"
            p_selected = None

            try:
                market_total = float(market_total_raw)
            except (ValueError, TypeError):
                market_total = ""

            try:
                lam_total = (
                    float(rows[0]["goals"]) * AWAY_MULT +
                    float(rows[1]["goals"]) * HOME_MULT
                )
            except (ValueError, TypeError):
                lam_total = None

            if (
                lam_total is not None
                and market_total != ""
                and MIN_LAM <= lam_total <= MAX_LAM
            ):
                cutoff = int(market_total - 0.5)
                p_under = poisson_cdf(cutoff, lam_total)
                p_over = 1.0 - p_under

                if lam_total > market_total:
                    side = "OVER"
                    p_selected = p_over
                elif lam_total < market_total:
                    side = "UNDER"
                    p_selected = p_under

            if p_selected is not None:
                fair_d = fair_decimal(p_selected)
                fair_a = decimal_to_american(fair_d)

                # ---- JUICE ADJUSTMENT (soc_ou calibrated) ----
                extra_buffer = 0.0

                if market_total >= 3.5:
                    extra_buffer += 0.02

                if p_selected < 0.58:
                    extra_buffer += 0.01
                # ---------------------------------------------

                acc_d = acceptable_decimal(p_selected, extra_buffer)
                acc_a = decimal_to_american(acc_d)

                writer.writerow({
                    "game_id": game_id,
                    "date": date,
                    "time": time,
                    "team_1": team_1,
                    "team_2": team_2,
                    "market_total": market_total,
                    "side": side,
                    "model_probability": round(p_selected, 4),
                    "fair_decimal_odds": round(fair_d, 4),
                    "fair_american_odds": fair_a,
                    "acceptable_decimal_odds": round(acc_d, 4),
                    "acceptable_american_odds": acc_a,
                    "league": "soc_ou",
                })
            else:
                writer.writerow({
                    "game_id": game_id,
                    "date": date,
                    "time": time,
                    "team_1": team_1,
                    "team_2": team_2,
                    "market_total": market_total,
                    "side": "NO PLAY",
                    "model_probability": "",
                    "fair_decimal_odds": "",
                    "fair_american_odds": "",
                    "acceptable_decimal_odds": "",
                    "acceptable_american_odds": "",
                    "league": "soc_ou",
                })

    print(f"Created {out_path}")


if __name__ == "__main__":
    main()
