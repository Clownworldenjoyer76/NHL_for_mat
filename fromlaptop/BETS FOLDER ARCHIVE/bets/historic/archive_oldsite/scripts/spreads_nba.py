import pandas as pd
import glob
from pathlib import Path
from scipy.stats import norm
from datetime import datetime

CLEANED_DIR = Path("docs/win/dump/csvs/cleaned")
NORMALIZED_DIR = Path("docs/win/manual/normalized")
OUTPUT_DIR = Path("docs/win/nba/spreads")
ERROR_DIR = Path("docs/win/errors/06_spreads")
ERROR_LOG = ERROR_DIR / "spreads_nba_errors.txt"

EDGE = 0.05
NBA_STD_DEV = 12

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

def log_error(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

def to_american(dec):
    if pd.isna(dec) or dec <= 1:
        return ""
    if dec >= 2:
        return f"+{int((dec - 1) * 100)}"
    return f"-{int(100 / (dec - 1))}"

def process_spreads():
    with open(ERROR_LOG, "w", encoding="utf-8"):
        pass
        
    projection_files = glob.glob(str(CLEANED_DIR / "nba_*.csv"))

    if not projection_files:
        log_error("No NBA projection files found")
        return

    for proj_path in projection_files:
        try:
            date_suffix = "_".join(Path(proj_path).stem.split("_")[1:])
            dk_path = NORMALIZED_DIR / f"dk_nba_spreads_{date_suffix}.csv"

            if not dk_path.exists():
                log_error(f"Missing DK file: {dk_path}")
                continue

            df_proj = pd.read_csv(proj_path)
            df_dk = pd.read_csv(dk_path)

            df_proj = df_proj[
                [
                    "game_id",
                    "away_team_projected_points",
                    "home_team_projected_points",
                    "game_projected_points",
                ]
            ]

            merged = pd.merge(df_dk, df_proj, on="game_id", how="inner")
            if merged.empty:
                log_error(f"No merge rows for {proj_path}")
                continue

            merged["proj_home_margin"] = (
                merged["home_team_projected_points"]
                - merged["away_team_projected_points"]
            )

            merged["home_spread_probability"] = merged.apply(
                lambda x: 1 - norm.cdf(-x["home_spread"], x["proj_home_margin"], NBA_STD_DEV),
                axis=1,
            )
            merged["away_spread_probability"] = 1 - merged["home_spread_probability"]

            merged["home_spread_acceptable_decimal_odds"] = (
                (1 / merged["home_spread_probability"]) * (1 + EDGE)
            )
            merged["away_spread_acceptable_decimal_odds"] = (
                (1 / merged["away_spread_probability"]) * (1 + EDGE)
            )

            merged["home_spread_acceptable_american_odds"] = merged[
                "home_spread_acceptable_decimal_odds"
            ].apply(to_american)
            merged["away_spread_acceptable_american_odds"] = merged[
                "away_spread_acceptable_decimal_odds"
            ].apply(to_american)

            cols = [
                "game_id", "league", "date", "time",
                "away_team", "home_team",
                "away_team_projected_points", "home_team_projected_points",
                "game_projected_points",
                "away_spread", "home_spread",
                "away_handle_pct", "home_handle_pct",
                "away_bets_pct", "home_bets_pct",
                "away_spread_probability", "home_spread_probability",
                "away_spread_acceptable_decimal_odds", "away_spread_acceptable_american_odds",
                "home_spread_acceptable_decimal_odds", "home_spread_acceptable_american_odds",
            ]

            merged[cols].to_csv(
                OUTPUT_DIR / f"spreads_nba_{date_suffix}.csv",
                index=False,
            )

        except Exception as e:
            log_error(f"{proj_path} failed: {e}")

if __name__ == "__main__":
    process_spreads()
