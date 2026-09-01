#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/generate_summary.py

import pandas as pd
import glob
import os
from pathlib import Path
from datetime import datetime

OUTPUT_FILE = Path("docs/win/final_scores/errors/generate_summary_edges.txt")
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)


# =========================
# EDGE EXTRACTION
# =========================

def extract_edge(row):

    try:

        side = str(row.get("bet_side", "")).lower()
        market = str(row.get("market_type", "")).lower()

        if market == "moneyline":

            if side == "home":
                return row.get("home_ml_edge_decimal")

            if side == "away":
                return row.get("away_ml_edge_decimal")

        # UPDATED: include puck_line
        if market in ["spread", "puck_line"]:

            if side == "home":
                return row.get("home_spread_edge_decimal")

            if side == "away":
                return row.get("away_spread_edge_decimal")

        if market in ["total", "totals"]:

            if side == "over":
                return row.get("over_edge_decimal")

            if side == "under":
                return row.get("under_edge_decimal")

    except Exception:
        pass

    return None

# =========================
# SPORTS CONFIG
# =========================
SPORTS = [
    {"name": "nhl", "suffix": "NHL"},
    {"name": "mlb", "suffix": "MLB"},
]


# =========================
# MAIN REPORT
# =========================

def generate_reports():

    skipped_rows = []

    total_all_rows = 0
    total_all_skipped = 0

    total_win_edges = []
    total_loss_edges = []

    output_lines = []

    output_lines.append(
        f"Edge Summary Report | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )

    for sport in SPORTS:

        sport_name = sport["name"]
        suffix = sport["suffix"]

        results_dir = f"docs/win/final_scores/results/{sport_name}/graded"

        files = glob.glob(os.path.join(results_dir, f"*_results_{suffix}.csv"))

        if not files:
            continue

        sport_rows = 0
        sport_skipped = 0

        win_edges = []
        loss_edges = []

        for file in files:

            try:
                df = pd.read_csv(file)
            except Exception as e:

                skipped_rows.append(
                    (file, "FILE_READ_ERROR", str(e))
                )
                continue

            for idx, row in df.iterrows():

                sport_rows += 1
                total_all_rows += 1

                edge = extract_edge(row)

                if edge is None or pd.isna(edge):

                    sport_skipped += 1
                    total_all_skipped += 1

                    skipped_rows.append(
                        (
                            file,
                            row.get("game_id", "unknown_game"),
                            "Missing edge value"
                        )
                    )
                    continue

                result = row.get("bet_result")

                if result == "Win":
                    win_edges.append(edge)
                    total_win_edges.append(edge)

                elif result == "Loss":
                    loss_edges.append(edge)
                    total_loss_edges.append(edge)

                else:

                    sport_skipped += 1
                    total_all_skipped += 1

                    skipped_rows.append(
                        (
                            file,
                            row.get("game_id", "unknown_game"),
                            f"Invalid result: {result}"
                        )
                    )

        win_avg = sum(win_edges) / len(win_edges) if win_edges else 0
        loss_avg = sum(loss_edges) / len(loss_edges) if loss_edges else 0

        direction = "CORRECT" if win_avg > loss_avg else "INVERTED"

        output_lines.append(f"\n{suffix}")
        output_lines.append(f"Average win edge: {win_avg:.4f}")
        output_lines.append(f"Average loss edge: {loss_avg:.4f}")
        output_lines.append(f"Edge signal direction: {direction}")
        output_lines.append(f"Total Graded Bets: {sport_rows}")
        output_lines.append(f"Total Graded Bets Skip: {sport_skipped}")

    # =========================
    # TOTAL SECTION
    # =========================

    total_win_avg = (
        sum(total_win_edges) / len(total_win_edges)
        if total_win_edges else 0
    )

    total_loss_avg = (
        sum(total_loss_edges) / len(total_loss_edges)
        if total_loss_edges else 0
    )

    total_direction = (
        "CORRECT" if total_win_avg > total_loss_avg else "INVERTED"
    )

    output_lines.append("\nTotal [all sports combined]")
    output_lines.append(f"Average win edge: {total_win_avg:.4f}")
    output_lines.append(f"Average loss edge: {total_loss_avg:.4f}")
    output_lines.append(f"Edge signal direction: {total_direction}")
    output_lines.append(f"Total Graded Bets: {total_all_rows}")
    output_lines.append(f"Total Graded Bets Skip: {total_all_skipped}")

    # =========================
    # SKIPPED SECTION
    # =========================

    output_lines.append("\nSkipped")

    if skipped_rows:

        for file, game_id, reason in skipped_rows:

            fname = os.path.basename(file)

            output_lines.append(
                f"{game_id} | {fname} | {reason}"
            )

    else:

        output_lines.append("None")

    # =========================
    # WRITE REPORT
    # =========================

    with open(OUTPUT_FILE, "w") as f:

        for line in output_lines:
            f.write(line + "\n")


if __name__ == "__main__":
    generate_reports()
