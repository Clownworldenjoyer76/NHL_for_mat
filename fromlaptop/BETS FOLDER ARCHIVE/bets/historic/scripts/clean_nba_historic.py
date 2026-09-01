#!/usr/bin/env python3

import pandas as pd
from pathlib import Path

INPUT_FILE = Path("bets/historic/archive/nba_data.csv")
OUTPUT_DIR = Path("bets/historic/clean")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def american_profit(odds):
    if odds > 0:
        return odds / 100
    else:
        return 100 / abs(odds)

def clean_games(df):

    df["date"] = df["date"].astype(str).str.replace(".0","",regex=False)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

    numeric_cols = [
        "home_final","away_final",
        "home_close_ml","away_close_ml",
        "home_close_spread","away_close_spread",
        "close_over_under"
    ]

    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["margin"] = df["home_final"] - df["away_final"]
    df["total_points"] = df["home_final"] + df["away_final"]

    df = df[(df["close_over_under"] > 100) & (df["close_over_under"] < 300)]
    df = df[(df["home_close_spread"].abs() <= 25)]

    return df

def create_ml_bets(df):

    rows = []

    for _, r in df.iterrows():

        margin = r["margin"]

        home_win = margin > 0

        rows.append({
            "date": r["date"],
            "team": r["home_team"],
            "opponent": r["away_team"],
            "venue": "home",
            "fav_ud": "favorite" if r["home_close_ml"] < 0 else "underdog",
            "odds": r["home_close_ml"],
            "result": "win" if home_win else "loss",
            "profit": american_profit(r["home_close_ml"]) if home_win else -1
        })

        rows.append({
            "date": r["date"],
            "team": r["away_team"],
            "opponent": r["home_team"],
            "venue": "away",
            "fav_ud": "favorite" if r["away_close_ml"] < 0 else "underdog",
            "odds": r["away_close_ml"],
            "result": "win" if not home_win else "loss",
            "profit": american_profit(r["away_close_ml"]) if not home_win else -1
        })

    return pd.DataFrame(rows)

def create_spread_bets(df):

    rows = []

    for _, r in df.iterrows():

        margin = r["margin"]

        home_spread = r["home_close_spread"]
        away_spread = r["away_close_spread"]

        def result(margin, spread):
            if margin + spread > 0:
                return "win"
            elif margin + spread < 0:
                return "loss"
            else:
                return "push"

        home_result = result(margin, home_spread)
        away_result = result(-margin, away_spread)

        rows.append({
            "date": r["date"],
            "team": r["home_team"],
            "opponent": r["away_team"],
            "venue": "home",
            "spread": home_spread,
            "result": home_result
        })

        rows.append({
            "date": r["date"],
            "team": r["away_team"],
            "opponent": r["home_team"],
            "venue": "away",
            "spread": away_spread,
            "result": away_result
        })

    return pd.DataFrame(rows)

def create_total_bets(df):

    rows = []

    for _, r in df.iterrows():

        line = r["close_over_under"]
        total = r["total_points"]

        if total > line:
            over_result = "win"
            under_result = "loss"
        elif total < line:
            over_result = "loss"
            under_result = "win"
        else:
            over_result = "push"
            under_result = "push"

        rows.append({
            "date": r["date"],
            "side": "over",
            "line": line,
            "total_points": total,
            "result": over_result
        })

        rows.append({
            "date": r["date"],
            "side": "under",
            "line": line,
            "total_points": total,
            "result": under_result
        })

    return pd.DataFrame(rows)

def main():

    df = pd.read_csv(INPUT_FILE)

    df = clean_games(df)

    df.to_csv(OUTPUT_DIR / "nba_games_clean.csv", index=False)

    ml = create_ml_bets(df)
    ml.to_csv(OUTPUT_DIR / "nba_moneyline_bets.csv", index=False)

    sp = create_spread_bets(df)
    sp.to_csv(OUTPUT_DIR / "nba_spread_bets.csv", index=False)

    totals = create_total_bets(df)
    totals.to_csv(OUTPUT_DIR / "nba_total_bets.csv", index=False)

if __name__ == "__main__":
    main()
