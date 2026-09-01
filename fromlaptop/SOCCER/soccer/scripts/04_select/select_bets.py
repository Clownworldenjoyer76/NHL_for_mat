#!/usr/bin/env python3
# docs/win/soccer/scripts/04_select/select_bets.py

import pandas as pd
from pathlib import Path
from datetime import datetime
import traceback
import re
import yaml

# =========================
# PATHS
# =========================
INPUT_DIR = Path("docs/win/soccer/03_edges")
OUTPUT_DIR = Path("docs/win/soccer/04_select")
ERROR_DIR = Path("docs/win/soccer/errors/04_select")
ERROR_LOG = ERROR_DIR / "select_bets.txt"

CONFIG_PATH = Path("docs/win/soccer/config/markets.yaml")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# LOAD CONFIG
# =========================
with open(CONFIG_PATH, "r") as f:
    MARKET_CONFIG = yaml.safe_load(f)["markets"]

# =========================
# CONFIG (fallbacks only)
# =========================
KELLY_FRACTION = 0.25
MIN_EDGE_FALLBACK = 0.00001

# =========================
# HELPERS
# =========================
def parse_match_time(time_str):

    if pd.isna(time_str):
        return None

    time_str = str(time_str).strip()

    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            return datetime.strptime(time_str, fmt)
        except Exception:
            pass

    return None


def calculate_kelly(prob, odds, fraction=0.25):

    if pd.isna(prob) or pd.isna(odds):
        return 0

    if odds <= 1 or prob <= 0:
        return 0

    f_star = (odds * prob - 1) / (odds - 1)

    return max(0, f_star * fraction)


def decimal_to_american(decimal):

    if pd.isna(decimal):
        return None

    try:
        decimal = float(decimal)

        if decimal <= 1:
            return None

        if decimal >= 2:
            return round((decimal - 1) * 100)

        return round(-100 / (decimal - 1))

    except Exception:
        return None


def get_market_min_edge(row, market_type):

    market = row.get("market")
    market_cfg = MARKET_CONFIG.get(market, {})
    type_cfg = market_cfg.get(market_type, {})

    return type_cfg.get("min_edge", MIN_EDGE_FALLBACK)


def get_result_prob(row, side):
    return row.get(f"{side}_prob")


def get_total_prob(row, side):

    if side == "over25":
        return row.get("over25_prob")

    if side == "under25":
        over_prob = row.get("over25_prob")
        return None if pd.isna(over_prob) else 1 - over_prob

    if side == "over35":
        return row.get("over35_prob")

    if side == "under35":
        over_prob = row.get("over35_prob")
        return None if pd.isna(over_prob) else 1 - over_prob

    return None


def get_btts_prob(row, side):

    if side == "btts_yes":
        return row.get("btts_prob")

    if side == "btts_no":
        prob = row.get("btts_prob")
        return None if pd.isna(prob) else 1 - prob

    return None


# =========================
# BUILD SELECTION
# =========================
def build_selection(row, market_name, take_bet, edge_pct, prob, odds_decimal=None, odds_american=None):

    if odds_decimal is None:
        odds_decimal = row.get(f"{take_bet}_dk_decimal")

    if odds_american is None:
        odds_american = row.get(f"dk_{take_bet}_american")

    stake = calculate_kelly(prob, odds_decimal, KELLY_FRACTION)

    return {
        "league": "Soccer",
        "market": row.get("market"),
        "game_date": row.get("match_date"),
        "match_time": row.get("match_time"),
        "home_team": row.get("home_team"),
        "away_team": row.get("away_team"),
        "game_id": row.get("game_id"),
        "market_type": market_name,
        "take_bet": take_bet,
        "odds_american": odds_american,
        "odds_decimal": odds_decimal,
        "edge_pct": edge_pct,
        "kelly_stake_pct": round(stake * 100, 2),
        "expected_goals": row.get("expected_total_goals", "")
    }


# =========================
# RESULT
# =========================
def select_best_result_side(row, columns):

    min_edge = get_market_min_edge(row, "result")
    candidates = {}

    for side in ["home", "draw", "away"]:

        edge = row.get(f"{side}_edge_pct")

        if pd.isna(edge) or edge <= min_edge:
            continue

        candidates[side] = edge

    if not candidates:
        return None

    best = max(candidates, key=candidates.get)
    prob = get_result_prob(row, best)

    return build_selection(row, "result", best, candidates[best], prob)


# =========================
# TOTAL
# =========================
def select_best_total(row, columns):

    min_edge = get_market_min_edge(row, "total")
    candidates = {}

    has_25 = not pd.isna(row.get("over25_prob"))
    has_35 = not pd.isna(row.get("over35_prob"))

    if has_25:
        sides = ["over25", "under25"]
    elif has_35:
        sides = ["over35", "under35"]
    else:
        return None

    for side in sides:

        edge = row.get(f"{side}_edge_pct")
        prob = get_total_prob(row, side)

        if pd.isna(edge) or edge <= min_edge or prob is None:
            continue

        candidates[side] = edge

    if not candidates:
        return None

    best = max(candidates, key=candidates.get)
    prob = get_total_prob(row, best)

    return build_selection(row, "total", best, candidates[best], prob)


# =========================
# BTTS
# =========================
def select_best_btts(row, columns):

    min_edge = get_market_min_edge(row, "btts")

    prob_yes = row.get("btts_prob")

    if pd.isna(prob_yes):
        return None

    prob_no = 1 - prob_yes

    yes_edge = row.get("btts_yes_edge_pct")
    no_edge = row.get("btts_no_edge_pct")

    candidates = {}

    if not pd.isna(yes_edge) and yes_edge > min_edge:
        candidates["btts_yes"] = (yes_edge, prob_yes)

    if not pd.isna(no_edge) and no_edge > min_edge:
        candidates["btts_no"] = (no_edge, prob_no)

    if not candidates:
        return None

    best = max(candidates, key=lambda x: candidates[x][0])
    edge, prob = candidates[best]

    odds_decimal = row.get(f"{best}_adjusted_decimal")

    if pd.isna(odds_decimal):
        return None

    odds_american = decimal_to_american(odds_decimal)

    return build_selection(
        row=row,
        market_name="btts",
        take_bet=best,
        edge_pct=edge,
        prob=prob,
        odds_decimal=odds_decimal,
        odds_american=odds_american
    )


# =========================
# MAIN
# =========================
def main():

    with open(ERROR_LOG, "a") as log:

        log.write(f"=== SELECT BETS RUN: {datetime.utcnow().isoformat()}Z ===\n")

        try:

            input_files = sorted(INPUT_DIR.glob("soccer_*.csv"))

            if not input_files:
                log.write("No input files found.\n")
                return

            for input_path in input_files:

                df = pd.read_csv(input_path)
                columns = set(df.columns)

                selections = []

                for _, row in df.iterrows():

                    r = select_best_result_side(row, columns)
                    if r:
                        selections.append(r)

                    t = select_best_total(row, columns)
                    if t:
                        selections.append(t)

                    b = select_best_btts(row, columns)
                    if b:
                        selections.append(b)

                if not selections:
                    log.write(f"No plays for {input_path.name}\n")
                    continue

                sel_df = pd.DataFrame(selections)

                sel_df["_sort_time"] = sel_df["match_time"].apply(parse_match_time)

                sel_df = sel_df.sort_values(
                    by=["game_date", "_sort_time", "home_team", "away_team", "market_type"],
                    na_position="last"
                ).drop(columns=["_sort_time"])

                output_path = OUTPUT_DIR / input_path.name

                sel_df.to_csv(output_path, index=False)

                log.write(f"Wrote {len(sel_df)} rows to {output_path}\n")

        except Exception as e:

            log.write(f"\nCRITICAL ERROR: {str(e)}\n{traceback.format_exc()}\n")


        # =========================
        # DAILY COMBINED
        # =========================
        try:

            date_groups = {}

            for csv_file in OUTPUT_DIR.glob("soccer_*.csv"):

                match = re.search(r"(\d{4}_\d{2}_\d{2})", csv_file.name)

                if not match:
                    continue

                date_str = match.group(1)
                date_groups.setdefault(date_str, []).append(csv_file)

            for date_str, files in date_groups.items():

                dfs = []

                for f in files:
                    try:
                        df = pd.read_csv(f)
                        if not df.empty:
                            dfs.append(df)
                    except:
                        continue

                if not dfs:
                    continue

                combined = pd.concat(dfs, ignore_index=True)

                combined = combined.drop_duplicates(
                    subset=["game_date", "home_team", "away_team", "market_type"]
                )

                out_path = OUTPUT_DIR / f"{date_str}_soccer.csv"

                combined.to_csv(out_path, index=False)

                log.write(f"Created {out_path}\n")

        except Exception as e:

            log.write(f"\nERROR BUILDING DAILY: {str(e)}\n{traceback.format_exc()}\n")


if __name__ == "__main__":
    main()
