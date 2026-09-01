#!/usr/bin/env python3
# docs/win/hockey/scripts/04_select/select_bets.py

import pandas as pd
from pathlib import Path
from datetime import datetime
import traceback

INPUT_DIR = Path("docs/win/hockey/03_edges")
OUTPUT_DIR = Path("docs/win/hockey/04_select")
ERROR_DIR = Path("docs/win/hockey/errors/04_select")
ERROR_LOG = ERROR_DIR / "select_bets.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

TOTAL_MIN_EDGE_PCT = 0.03
TOTAL_MIN_PROB = 0.45

LEAGUE_CODE = "NHL"

REQUIRED_GAME_COLS = ["game_date", "away_team", "home_team"]


def valid_edge(edge_pct, threshold):
    return pd.notna(edge_pct) and edge_pct >= threshold


def assert_required_cols(df: pd.DataFrame, df_name: str, log) -> bool:
    if df is None:
        return True
    missing = [c for c in REQUIRED_GAME_COLS if c not in df.columns]
    if missing:
        log.write(f"ERROR: {df_name} missing required columns: {missing}\n")
        return False
    return True


def build_matchups(df: pd.DataFrame) -> pd.DataFrame:
    # unique matchups by authoritative columns only
    return df[REQUIRED_GAME_COLS].dropna().drop_duplicates()


def main():
    with open(ERROR_LOG, "w") as log:

        log.write("=== NHL SELECT BETS RUN ===\n")
        log.write(f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n")

        try:

            moneyline_files = sorted(INPUT_DIR.glob("*_NHL_moneyline.csv"))
            puckline_files = sorted(INPUT_DIR.glob("*_NHL_puck_line.csv"))
            total_files = sorted(INPUT_DIR.glob("*_NHL_total.csv"))

            all_files = moneyline_files + puckline_files + total_files
            slates = {}

            for f in all_files:
                slate_key = (
                    f.name.replace("_NHL_moneyline.csv", "")
                    .replace("_NHL_puck_line.csv", "")
                    .replace("_NHL_total.csv", "")
                )
                slates.setdefault(slate_key, []).append(f)

            for slate_key in slates.keys():

                final_rows = []

                ml_count = 0
                puck_count = 0
                total_count = 0

                ml_path = INPUT_DIR / f"{slate_key}_NHL_moneyline.csv"
                pl_path = INPUT_DIR / f"{slate_key}_NHL_puck_line.csv"
                total_path = INPUT_DIR / f"{slate_key}_NHL_total.csv"

                ml_df = pd.read_csv(ml_path) if ml_path.exists() else None
                pl_df = pd.read_csv(pl_path) if pl_path.exists() else None
                total_df = pd.read_csv(total_path) if total_path.exists() else None

                # Hard requirement: no parsing from game_id. If inputs lack columns, skip slate.
                ok = True
                ok = ok and assert_required_cols(ml_df, ml_path.name, log)
                ok = ok and assert_required_cols(pl_df, pl_path.name, log)
                ok = ok and assert_required_cols(total_df, total_path.name, log)

                if not ok:
                    log.write(f"Skipping slate {slate_key} due to missing required columns.\n\n")
                    continue

                # Build canonical matchup list from authoritative columns
                matchup_frames = []
                if ml_df is not None and not ml_df.empty:
                    matchup_frames.append(build_matchups(ml_df))
                if pl_df is not None and not pl_df.empty:
                    matchup_frames.append(build_matchups(pl_df))
                if total_df is not None and not total_df.empty:
                    matchup_frames.append(build_matchups(total_df))

                if not matchup_frames:
                    output_path = OUTPUT_DIR / f"{slate_key}_NHL.csv"
                    pd.DataFrame([]).to_csv(output_path, index=False)
                    log.write(f"Wrote {output_path} | rows=0\n")
                    log.write("Moneyline bets: 0\nPuck line bets: 0\nTotal bets: 0\n\n")
                    continue

                matchups = pd.concat(matchup_frames, ignore_index=True).drop_duplicates()

                for _, m in matchups.iterrows():
                    game_date = str(m["game_date"])
                    away_team = str(m["away_team"])
                    home_team = str(m["home_team"])

                    # =====================
                    # PUCK LINE
                    # =====================
                    if pl_df is not None and not pl_df.empty:
                        game_pl = pl_df[
                            (pl_df["game_date"].astype(str) == game_date)
                            & (pl_df["away_team"].astype(str) == away_team)
                            & (pl_df["home_team"].astype(str) == home_team)
                        ]

                        best_row = None
                        best_edge = -float("inf")

                        for _, row in game_pl.iterrows():
                            for side in ["home", "away"]:
                                puck_line = row.get(f"{side}_puck_line")
                                edge_pct = row.get(f"{side}_edge_pct")

                                if pd.isna(puck_line) or puck_line <= 0:
                                    continue
                                if pd.isna(edge_pct) or edge_pct <= 0:
                                    continue

                                if edge_pct > best_edge:
                                    best_edge = edge_pct
                                    best_row = (row, side)

                        if best_row:
                            row, side = best_row
                            line_val = row.get(f"{side}_puck_line")

                            final_rows.append({
                                "game_date": game_date,
                                "league": LEAGUE_CODE,
                                "away_team": away_team,
                                "home_team": home_team,
                                "market_type": "puck_line",
                                "bet_side": side,
                                "line": line_val,

                                "game_id": row.get("game_id"),
                                "take_bet": f"{side}_puck_line",
                                "take_bet_prob": row.get(f"{side}_juiced_prob_puck_line"),
                                "take_bet_edge_decimal": row.get(f"{side}_edge_decimal"),
                                "take_bet_edge_pct": row.get(f"{side}_edge_pct"),
                                "take_team": side,  # Option A label only
                                "take_odds": row.get(f"{side}_dk_puck_line_american"),
                                "value": line_val,
                            })
                            puck_count += 1

                    # =====================
                    # MONEYLINE
                    # =====================
                    if ml_df is not None and not ml_df.empty:
                        game_ml = ml_df[
                            (ml_df["game_date"].astype(str) == game_date)
                            & (ml_df["away_team"].astype(str) == away_team)
                            & (ml_df["home_team"].astype(str) == home_team)
                        ]

                        best_row = None
                        best_edge = -float("inf")

                        for _, row in game_ml.iterrows():
                            for side in ["home", "away"]:
                                edge_pct = row.get(f"{side}_edge_pct")
                                prob = row.get(f"{side}_prob")
                                american_odds = row.get(f"{side}_dk_moneyline_american")

                                if pd.isna(edge_pct) or pd.isna(prob) or pd.isna(american_odds):
                                    continue

                                if 200 <= american_odds <= 225:
                                    if not (edge_pct >= 0.05 and prob >= 0.35):
                                        continue
                                elif 1 <= american_odds <= 199:
                                    if not (edge_pct >= 0.05 and prob >= 0.38):
                                        continue
                                elif -1 >= american_odds >= -999:
                                    if not (edge_pct >= 0.04 and prob >= 0.55):
                                        continue
                                else:
                                    continue

                                if edge_pct > best_edge:
                                    best_edge = edge_pct
                                    best_row = (row, side)

                        if best_row:
                            row, side = best_row

                            final_rows.append({
                                "game_date": game_date,
                                "league": LEAGUE_CODE,
                                "away_team": away_team,
                                "home_team": home_team,
                                "market_type": "moneyline",
                                "bet_side": side,
                                "line": "",

                                "game_id": row.get("game_id"),
                                "take_bet": f"{side}_moneyline",
                                "take_bet_prob": row.get(f"{side}_prob"),
                                "take_bet_edge_decimal": row.get(f"{side}_edge_decimal"),
                                "take_bet_edge_pct": row.get(f"{side}_edge_pct"),
                                "take_team": side,  # Option A label only
                                "take_odds": row.get(f"{side}_dk_moneyline_american"),
                                "value": row.get(f"{side}_prob"),
                            })
                            ml_count += 1

                    # =====================
                    # TOTAL
                    # =====================
                    if total_df is not None and not total_df.empty:
                        game_total = total_df[
                            (total_df["game_date"].astype(str) == game_date)
                            & (total_df["away_team"].astype(str) == away_team)
                            & (total_df["home_team"].astype(str) == home_team)
                        ]

                        best_row = None
                        best_edge = -float("inf")

                        for _, row in game_total.iterrows():
                            for side in ["over", "under"]:
                                edge_pct = row.get(f"{side}_edge_pct")
                                prob = row.get(f"juiced_total_{side}_prob")

                                if not valid_edge(edge_pct, TOTAL_MIN_EDGE_PCT):
                                    continue
                                if pd.isna(prob) or prob < TOTAL_MIN_PROB:
                                    continue

                                if edge_pct > best_edge:
                                    best_edge = edge_pct
                                    best_row = (row, side)

                        if best_row:
                            row, side = best_row
                            line_val = row.get("total")

                            final_rows.append({
                                "game_date": game_date,
                                "league": LEAGUE_CODE,
                                "away_team": away_team,
                                "home_team": home_team,
                                "market_type": "total",
                                "bet_side": side,
                                "line": line_val,

                                "game_id": row.get("game_id"),
                                "take_bet": f"{side}_total",
                                "take_bet_prob": row.get(f"juiced_total_{side}_prob"),
                                "take_bet_edge_decimal": row.get(f"{side}_edge_decimal"),
                                "take_bet_edge_pct": row.get(f"{side}_edge_pct"),
                                "take_team": side,  # Option A label only
                                "take_odds": row.get(f"dk_total_{side}_american"),
                                "value": line_val,
                            })
                            total_count += 1

                out_df = pd.DataFrame(final_rows)

                if not out_df.empty:
                    out_df = out_df.sort_values(by="take_bet_edge_pct", ascending=False)

                    # Final safety: prevent mirrored duplicates by matchup columns (no game_id parsing)
                    out_df = out_df.drop_duplicates(
                        subset=["game_date", "league", "away_team", "home_team", "market_type", "bet_side", "line"]
                    )

                output_path = OUTPUT_DIR / f"{slate_key}_NHL.csv"
                out_df.to_csv(output_path, index=False)

                log.write(f"Wrote {output_path} | rows={len(out_df)}\n")
                log.write(f"Moneyline bets: {ml_count}\n")
                log.write(f"Puck line bets: {puck_count}\n")
                log.write(f"Total bets: {total_count}\n\n")

        except Exception as e:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    main()
