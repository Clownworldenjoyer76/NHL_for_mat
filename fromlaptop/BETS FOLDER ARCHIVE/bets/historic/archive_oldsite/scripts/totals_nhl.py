import pandas as pd
import glob
from pathlib import Path
from scipy.stats import poisson
from datetime import datetime

CLEANED_DIR = Path("docs/win/dump/csvs/cleaned")
NORMALIZED_DIR = Path("docs/win/manual/normalized")
OUTPUT_DIR = Path("docs/win/nhl/totals")
ERROR_DIR = Path("docs/win/errors/05_totals")
ERROR_LOG = ERROR_DIR / "totals_nhl_errors.txt"

EDGE = 0.05

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

def log_error(msg):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

def to_american(decimal_odds):
    if pd.isna(decimal_odds) or decimal_odds <= 1:
        return ""
    if decimal_odds >= 2.0:
        return f"+{int((decimal_odds - 1) * 100)}"
    return f"-{int(100 / (decimal_odds - 1))}"

def normalize_shared_columns(df):
    for col in ["date", "time", "away_team", "home_team"]:
        if f"{col}_y" in df.columns:
            df[col] = df[f"{col}_y"]
        elif f"{col}_x" in df.columns:
            df[col] = df[f"{col}_x"]
        elif col not in df.columns:
            df[col] = ""
    return df.drop(columns=[c for c in df.columns if c.endswith("_x") or c.endswith("_y")], errors="ignore")

def process_totals():
    with open(ERROR_LOG, "w", encoding="utf-8"):
        pass
        
    projection_files = glob.glob(str(CLEANED_DIR / "nhl_*.csv"))

    if not projection_files:
        log_error("No NHL projection files found")
        return

    for proj_path in projection_files:
        try:
            date_suffix = "_".join(Path(proj_path).stem.split("_")[1:])
            dk_path = NORMALIZED_DIR / f"dk_nhl_totals_{date_suffix}.csv"

            if not dk_path.exists():
                log_error(f"Missing DK file: {dk_path}")
                continue

            df_proj = pd.read_csv(proj_path)
            df_dk = pd.read_csv(dk_path)

            df_proj = df_proj.drop(columns=["date", "time", "away_team", "home_team", "league"], errors="ignore")

            merged = pd.merge(df_proj, df_dk, on="game_id", how="inner")
            if merged.empty:
                log_error(f"No merge rows for {proj_path}")
                continue

            merged = normalize_shared_columns(merged)

            merged["under_probability"] = merged.apply(
                lambda x: poisson.cdf(x["total"] - 0.5, x["game_projected_goals"]),
                axis=1,
            )
            merged["over_probability"] = 1 - merged["under_probability"]

            merged["over_acceptable_decimal_odds"] = (1 / merged["over_probability"]) * (1 + EDGE)
            merged["under_acceptable_decimal_odds"] = (1 / merged["under_probability"]) * (1 + EDGE)

            merged["over_acceptable_american_odds"] = merged["over_acceptable_decimal_odds"].apply(to_american)
            merged["under_acceptable_american_odds"] = merged["under_acceptable_decimal_odds"].apply(to_american)

            cols = [
                "game_id", "date", "time", "away_team", "home_team",
                "away_team_projected_goals", "home_team_projected_goals",
                "away_handle_pct", "home_handle_pct",
                "away_bets_pct", "home_bets_pct",
                "game_projected_goals", "over_odds", "under_odds", "total",
                "over_probability", "under_probability",
                "over_acceptable_decimal_odds", "over_acceptable_american_odds",
                "under_acceptable_decimal_odds", "under_acceptable_american_odds",
            ]

            output_df = merged[cols].copy()
            output_df.insert(1, "league", "nhl_totals")

            out_path = OUTPUT_DIR / f"totals_nhl_{date_suffix}.csv"
            output_df.to_csv(out_path, index=False)

        except Exception as e:
            log_error(f"{proj_path} failed: {e}")

if __name__ == "__main__":
    process_totals()
