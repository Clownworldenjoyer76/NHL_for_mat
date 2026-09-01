# scripts/dk_text_to_csv.py
#!/usr/bin/env python3

import csv
import sys
import os
import re
from datetime import datetime

LEAGUE_INPUT = sys.argv[1] if len(sys.argv) > 1 else "ncaab"
DATE = datetime.now().strftime("%Y_%m_%d")

IS_DK = LEAGUE_INPUT.endswith("_dk")
BASE_LEAGUE = LEAGUE_INPUT.replace("_dk", "")
OUTPUT_LEAGUE = BASE_LEAGUE

OUT_DIR = "docs/win/manual/first"
ERROR_DIR = "docs/win/errors/dk_input"

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(ERROR_DIR, exist_ok=True)

OUT_ML = f"{OUT_DIR}/dk_{BASE_LEAGUE}_moneyline_{DATE}.csv"
OUT_SP = f"{OUT_DIR}/dk_{BASE_LEAGUE}_spreads_{DATE}.csv"
OUT_OU = f"{OUT_DIR}/dk_{BASE_LEAGUE}_totals_{DATE}.csv"

ERROR_LOG = f"{ERROR_DIR}/dk_input_{DATE}.txt"

ml_rows, sp_rows, ou_rows = [], [], []
games_seen = 0
errors = []

# ======================
# HELPERS
# ======================

def clean(line: str) -> str:
    return (
        line.replace("opens in a new tab", "")
            .replace("−", "-")
            .replace("-logo", "")
            .strip()
    )

def is_american_odds(s):
    return re.fullmatch(r"[+-]\d+", s or "") is not None

def is_spread(s):
    return re.fullmatch(r"[+-]\d+(\.\d+)?", s or "") is not None

def is_total_number(s):
    return re.fullmatch(r"\d+(\.\d+)?", s or "") is not None

def parse_time_line(line):
    m = re.search(r"(\d{1,2}:\d{2}\s?(AM|PM))", line)
    return m.group(1) if m else ""

def is_game_boundary(lines, idx):
    if idx + 1 >= len(lines):
        return False
    return lines[idx + 1].lower() == "at"

def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        if rows:
            w.writerows(rows)

def write_summary():
    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("DK INPUT PARSE SUMMARY\n")
        f.write("======================\n\n")
        f.write(f"League Input: {LEAGUE_INPUT}\n")
        f.write(f"League Output: {OUTPUT_LEAGUE}\n")
        f.write(f"Date: {DATE}\n")
        f.write(f"Games detected: {games_seen}\n")
        f.write(f"Moneyline rows: {len(ml_rows)}\n")
        f.write(f"Spread rows: {len(sp_rows)}\n")
        f.write(f"Total rows: {len(ou_rows)}\n\n")
        f.write(f"Total errors: {len(errors)}\n")

# ======================
# MAIN
# ======================

try:
    with open("raw.txt", encoding="utf-8") as f:
        lines = [clean(l) for l in f if clean(l)]

    # Only DK branch supports this parser; enforce ncaab_dk specifically
    if LEAGUE_INPUT != "ncaab_dk":
        raise RuntimeError("This parser is specialized for ncaab_dk only.")

    i = 0
    while i < len(lines):

        if lines[i].lower() == "at" and i > 0 and i + 1 < len(lines):

            away = lines[i - 1]
            home = lines[i + 1]

            # Skip ranking lines (e.g., "21" before team name)
            if away.isdigit():
                i += 1
                continue

            games_seen += 1

            j = i + 2
            spread_away = spread_home = None
            spread_away_odds = spread_home_odds = None
            total_number = None
            total_over_odds = total_under_odds = None
            ml_away = ml_home = None
            game_time = ""

            while j < len(lines):

                if is_game_boundary(lines, j):
                    break

                # Spread detection (two occurrences: away then home)
                if is_spread(lines[j]) and j + 1 < len(lines) and is_american_odds(lines[j + 1]):
                    if spread_away is None:
                        spread_away = lines[j]
                        spread_away_odds = lines[j + 1]
                    else:
                        spread_home = lines[j]
                        spread_home_odds = lines[j + 1]
                    j += 2
                    continue

                # Totals detection + embedded ML capture
                # Layout:
                #   O, total, over_odds, AWAY_ML
                #   U, total, under_odds, HOME_ML
                if lines[j] in ("O", "U") and j + 2 < len(lines):
                    side = lines[j]
                    total = lines[j + 1]
                    odds = lines[j + 2]

                    if is_total_number(total) and is_american_odds(odds):
                        total_number = total
                        if side == "O":
                            total_over_odds = odds
                        else:
                            total_under_odds = odds

                        j += 3

                        # Embedded ML immediately after total odds
                        if j < len(lines) and is_american_odds(lines[j]):
                            if side == "O" and ml_away is None:
                                ml_away = lines[j]
                                j += 1
                            elif side == "U" and ml_home is None:
                                ml_home = lines[j]
                                j += 1

                        continue

                # Time detection
                t = parse_time_line(lines[j])
                if t:
                    game_time = t

                j += 1

            # =========================
            # WRITE OUTPUT
            # =========================

            if spread_away and spread_home:
                sp_rows.append([
                    DATE.replace("_", "-"),
                    game_time,
                    away,
                    home,
                    spread_away,
                    spread_away_odds,
                    "",
                    "",
                    OUTPUT_LEAGUE
                ])
                sp_rows.append([
                    DATE.replace("_", "-"),
                    game_time,
                    home,
                    away,
                    spread_home,
                    spread_home_odds,
                    "",
                    "",
                    OUTPUT_LEAGUE
                ])

            if ml_away and ml_home:
                ml_rows.append([
                    DATE.replace("_", "-"),
                    game_time,
                    away,
                    home,
                    ml_away,
                    "",
                    "",
                    OUTPUT_LEAGUE
                ])
                ml_rows.append([
                    DATE.replace("_", "-"),
                    game_time,
                    home,
                    away,
                    ml_home,
                    "",
                    "",
                    OUTPUT_LEAGUE
                ])

            if total_number and total_over_odds and total_under_odds:
                for team1, team2 in [(away, home), (home, away)]:
                    ou_rows.append([
                        DATE.replace("_", "-"),
                        game_time,
                        team1,
                        team2,
                        "Over",
                        total_number,
                        total_over_odds,
                        "",
                        "",
                        OUTPUT_LEAGUE
                    ])
                    ou_rows.append([
                        DATE.replace("_", "-"),
                        game_time,
                        team1,
                        team2,
                        "Under",
                        total_number,
                        total_under_odds,
                        "",
                        "",
                        OUTPUT_LEAGUE
                    ])

            i = j
        else:
            i += 1

    write_csv(
        OUT_ML,
        ["date","time","team","opponent","odds","handle_pct","bets_pct","league"],
        ml_rows
    )

    write_csv(
        OUT_SP,
        ["date","time","team","opponent","spread","odds","handle_pct","bets_pct","league"],
        sp_rows
    )

    write_csv(
        OUT_OU,
        ["date","time","team","opponent","side","total","odds","handle_pct","bets_pct","league"],
        ou_rows
    )

    write_summary()

except Exception as e:
    errors.append(str(e))
    write_summary()
    raise
