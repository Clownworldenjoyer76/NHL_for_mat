#!/usr/bin/env python3
# docs/win/final_scores/scripts/00_parsing/scores.py

import sys
import csv
from pathlib import Path
from datetime import datetime
import traceback

OUT_DIR = Path("docs/win/final_scores")
ERR_DIR = OUT_DIR / "errors"

OUT_DIR.mkdir(parents=True, exist_ok=True)
ERR_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = [
    "game_date",
    "league",
    "market",
    "away_team",
    "home_team",
    "away_score",
    "home_score",
    "total",
    "away_spread",
    "home_spread",
    "away_puck_line",
    "home_puck_line",
]


def normalize_market(market: str) -> str:
    m = (market or "").strip().upper()
    if m in {"NBA", "NCAAB", "NCAAM"}:
        return "NCAAB" if m in {"NCAAB", "NCAAM"} else "NBA"
    if m == "NHL":
        return "NHL"
    raise ValueError("market must be NBA, NCAAB, or NHL")


def league_from_market(market: str) -> str:
    return "Basketball" if market in {"NBA", "NCAAB"} else "Hockey"


def is_date_line(s: str) -> bool:
    try:
        datetime.strptime(s.strip(), "%m/%d/%Y")
        return True
    except:
        return False


def to_output_date(s: str) -> str:
    dt = datetime.strptime(s.strip(), "%m/%d/%Y")
    return dt.strftime("%Y_%m_%d")


def first_field(line: str) -> str:
    return line.split("\t")[0].strip()


def parse_games(lines, market):
    rows = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        if not is_date_line(line):
            i += 1
            continue

        game_date = to_output_date(line)

        i += 1
        if i >= len(lines):
            break

        away_parts = lines[i].split("\t")
        if len(away_parts) < 2:
            i += 1
            continue

        away_team = away_parts[1].strip()

        i += 1
        if i >= len(lines):
            break

        home_team = first_field(lines[i])

        i += 1
        while i < len(lines) and not first_field(lines[i]).isdigit():
            i += 1
        if i >= len(lines):
            break

        away_score = int(first_field(lines[i]))

        i += 1
        while i < len(lines) and not first_field(lines[i]).isdigit():
            i += 1
        if i >= len(lines):
            break

        home_score = int(first_field(lines[i]))

        total = away_score + home_score

        away_spread = ""
        home_spread = ""
        away_puck_line = ""
        home_puck_line = ""

        # Spread logic
        if market in {"NBA", "NCAAB"}:
            away_spread = str(home_score - away_score)
            home_spread = str(away_score - home_score)

        # Puck line uses SAME logic
        if market == "NHL":
            away_puck_line = str(home_score - away_score)
            home_puck_line = str(away_score - home_score)

        rows.append({
            "game_date": game_date,
            "league": league_from_market(market),
            "market": market,
            "away_team": away_team,
            "home_team": home_team,
            "away_score": str(away_score),
            "home_score": str(home_score),
            "total": str(total),
            "away_spread": away_spread,
            "home_spread": home_spread,
            "away_puck_line": away_puck_line,
            "home_puck_line": home_puck_line,
        })

        i += 1

    if not rows:
        raise ValueError("No games parsed from input.")

    return rows


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    if len(sys.argv) != 3:
        print("Usage: scores.py <market: NBA|NCAAB|NHL> <input_text_file>")
        return 2

    market = normalize_market(sys.argv[1])
    input_path = Path(sys.argv[2])
    err_path = ERR_DIR / f"scores_{market}.txt"

    try:
        lines = input_path.read_text(encoding="utf-8", errors="replace").splitlines()
        rows = parse_games(lines, market)

        game_date = rows[0]["game_date"]
        out_path = OUT_DIR / f"{game_date}_final_scores_{market}.csv"

        write_csv(out_path, rows)
        print(f"Wrote {out_path} | rows={len(rows)}")
        return 0

    except Exception as e:
        with err_path.open("w", encoding="utf-8") as f:
            f.write(str(e) + "\n\n")
            f.write(traceback.format_exc())
        print(f"ERROR: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
