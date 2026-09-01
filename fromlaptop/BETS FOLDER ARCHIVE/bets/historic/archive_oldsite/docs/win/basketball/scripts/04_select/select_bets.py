#!/usr/bin/env python3
# docs/win/basketball/scripts/04_select/select_bets.py

import pandas as pd
from pathlib import Path
from datetime import datetime
import traceback

INPUT_DIR = Path("docs/win/basketball/03_edges")
OUTPUT_DIR = Path("docs/win/basketball/04_select")
ERROR_DIR = Path("docs/win/basketball/errors/04_select")
ERROR_LOG = ERROR_DIR / "select_bets.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

MIN_EDGE_DECIMAL = 0.05
MIN_EDGE_PCT = 0.02

MIN_TOTAL_EDGE_DECIMAL = 0.12
MIN_TOTAL_EDGE_PCT = 0.06

MIN_TOTAL_ODDS = -150

REQUIRED_GAME_COLS = ["game_date", "away_team", "home_team"]


def valid_edge(edge_dec, edge_pct):
    return (
        pd.notna(edge_dec)
        and pd.notna(edge_pct)
        and edge_dec >= MIN_EDGE_DECIMAL
        and edge_pct >= MIN_EDGE_PCT
    )


def valid_total_edge(edge_dec, edge_pct):
    return (
        pd.notna(edge_dec)
        and pd.notna(edge_pct)
        and edge_dec >= MIN_TOTAL_EDGE_DECIMAL
        and edge_pct >= MIN_TOTAL_EDGE_PCT
    )


def valid_total_odds(odds):
    odds_num = pd.to_numeric(odds, errors="coerce")
    return pd.notna(odds_num) and odds_num >= MIN_TOTAL_ODDS


def infer_market_from_filename(filename: str):
    name = filename.lower()
    if "moneyline" in name:
        return "moneyline"
    if "spread" in name:
        return "spread"
    if "total" in name:
        return "total"
    return None


def assert_required_cols(df: pd.DataFrame, df_name: str, log) -> bool:
    missing = [c for c in REQUIRED_GAME_COLS if c not in df.columns]
    if missing:
        log.write(f"ERROR: {df_name} missing required columns: {missing}\n")
        return False
    return True


def main():

    with open(ERROR_LOG, "w") as log:

        log.write("=== BASKETBALL SELECT BETS RUN ===\n")
        log.write(f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n")

        try:

            input_files = sorted(INPUT_DIR.glob("*.csv"))

            if not input_files:
                log.write("No input files found.\n")
                return

            for input_path in input_files:

                df = pd.read_csv(input_path)

                market = infer_market_from_filename(input_path.name)
                if market is None:
                    log.write(f"Skipping {input_path.name} (cannot infer market)\n")
                    continue

                # Hard requirement: no parsing from game_id
                if not assert_required_cols(df, input_path.name, log):
                    log.write(f"Skipping {input_path.name} due to missing required columns.\n\n")
                    continue

                selections = []

                for _, row in df.iterrows():

                    game_id = row.get("game_id")
                    league = row.get("league")

                    game_date = row.get("game_date")
                    away_team = row.get("away_team")
                    home_team = row.get("home_team")

                    # Normalize to strings (avoid nan)
                    game_date = "" if pd.isna(game_date) else str(game_date)
                    away_team = "" if pd.isna(away_team) else str(away_team)
                    home_team = "" if pd.isna(home_team) else str(home_team)

                    # =========================
                    # MONEYLINE
                    # =========================
                    if market == "moneyline":

                        for side in ["home", "away"]:

                            edge_dec = row.get(f"{side}_edge_decimal")
                            edge_pct = row.get(f"{side}_edge_pct")
                            win_prob = row.get(f"{side}_prob")
                            american_odds = row.get(f"{side}_juice_odds")

                            if pd.isna(edge_dec) or pd.isna(edge_pct) or pd.isna(win_prob):
                                continue

                            if str(league).upper() == "NBA":

                                odds = pd.to_numeric(american_odds, errors="coerce")
                                if pd.isna(odds):
                                    continue

                                if 100 <= odds <= 149:
                                    if not (edge_dec >= 0.05 and win_prob >= 0.42):
                                        continue
                                elif 150 <= odds <= 199:
                                    if not (edge_dec >= 0.06 and win_prob >= 0.38):
                                        continue
                                elif 200 <= odds <= 299:
                                    if not (edge_dec >= 0.07 and win_prob >= 0.33):
                                        continue
                                elif odds >= 300:
                                    if not (edge_dec >= 0.15 and win_prob >= 0.33):
                                        continue
                                elif -149 <= odds <= -100:
                                    if not (edge_dec >= 0.05 and win_prob >= 0.58):
                                        continue
                                elif -249 <= odds <= -150:
                                    if not (edge_dec >= 0.06 and win_prob >= 0.62):
                                        continue
                                elif odds <= -250:
                                    if not (edge_dec >= 0.07 and win_prob >= 0.70):
                                        continue
                                else:
                                    continue

                            else:
                                if not valid_edge(edge_dec, edge_pct):
                                    continue

                            selections.append({
                                "game_date": game_date,
                                "league": league,
                                "away_team": away_team,
                                "home_team": home_team,
                                "market_type": "moneyline",
                                "bet_side": side,
                                "line": "",

                                "game_id": game_id,
                                "market": market,
                                "take_bet": f"{side}_ml",
                                "take_odds": american_odds,
                                "take_team": side,  # Option A label only
                                "value": win_prob,
                                "take_bet_edge_decimal": edge_dec,
                                "take_bet_edge_pct": edge_pct,
                            })

                    # =========================
                    # SPREAD
                    # =========================
                    elif market == "spread":

                        for side in ["home", "away"]:

                            edge_dec = row.get(f"{side}_edge_decimal")
                            edge_pct = row.get(f"{side}_edge_pct")
                            spread_val = row.get(f"{side}_spread")

                            if valid_edge(edge_dec, edge_pct):

                                selections.append({
                                    "game_date": game_date,
                                    "league": league,
                                    "away_team": away_team,
                                    "home_team": home_team,
                                    "market_type": "spread",
                                    "bet_side": side,
                                    "line": spread_val,

                                    "game_id": game_id,
                                    "market": market,
                                    "take_bet": f"{side}_spread",
                                    "take_odds": row.get(f"{side}_spread_juice_odds"),
                                    "take_team": side,  # Option A label only
                                    "value": spread_val,
                                    "take_bet_edge_decimal": edge_dec,
                                    "take_bet_edge_pct": edge_pct,
                                })

                    # =========================
                    # TOTAL
                    # =========================
                    elif market == "total":

                        total_value = row.get("total")

                        over_dec = row.get("over_edge_decimal")
                        over_pct = row.get("over_edge_pct")
                        odds_over = row.get("total_over_juice_odds")

                        under_dec = row.get("under_edge_decimal")
                        under_pct = row.get("under_edge_pct")
                        odds_under = row.get("total_under_juice_odds")

                        if valid_total_edge(over_dec, over_pct) and valid_total_odds(odds_over):

                            selections.append({
                                "game_date": game_date,
                                "league": league,
                                "away_team": away_team,
                                "home_team": home_team,
                                "market_type": "total",
                                "bet_side": "over",
                                "line": total_value,

                                "game_id": game_id,
                                "market": market,
                                "take_bet": "over_bet",
                                "take_odds": odds_over,
                                "take_team": "over",
                                "value": total_value,
                                "take_bet_edge_decimal": over_dec,
                                "take_bet_edge_pct": over_pct,
                            })

                        if valid_total_edge(under_dec, under_pct) and valid_total_odds(odds_under):

                            selections.append({
                                "game_date": game_date,
                                "league": league,
                                "away_team": away_team,
                                "home_team": home_team,
                                "market_type": "total",
                                "bet_side": "under",
                                "line": total_value,

                                "game_id": game_id,
                                "market": market,
                                "take_bet": "under_bet",
                                "take_odds": odds_under,
                                "take_team": "under",
                                "value": total_value,
                                "take_bet_edge_decimal": under_dec,
                                "take_bet_edge_pct": under_pct,
                            })

                sel_df = pd.DataFrame(selections)

                if not sel_df.empty:
                    sel_df = sel_df.drop_duplicates(
                        subset=["game_date", "league", "away_team", "home_team", "market_type", "bet_side", "line", "take_bet"]
                    )

                output_path = OUTPUT_DIR / input_path.name
                sel_df.to_csv(output_path, index=False)

                log.write(f"Wrote {output_path} | rows={len(sel_df)}\n")

        except Exception as e:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    main()
