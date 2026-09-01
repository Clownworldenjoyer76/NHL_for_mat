import pandas as pd
import glob
from pathlib import Path

BASE_IN = Path("docs/win/juice")
BASE_OUT = Path("docs/win/final/step_1")
DK_NORMALIZED = Path("docs/win/manual/normalized")

ERROR_DIR = Path("docs/win/errors/final_01")
ERROR_LOG = ERROR_DIR / "final_01_missing_dk_merge.txt"

FILES_PROCESSED = 0
ROWS_IN = 0
ROWS_OUT = 0


def log_error(message: str):
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    with open(ERROR_LOG, "a") as f:
        f.write(message + "\n")


def run():
    global FILES_PROCESSED, ROWS_IN, ROWS_OUT

    for subdir in [
        "nba/ml",
        "nba/spreads",
        "nba/totals",
        "ncaab/ml",
        "ncaab/spreads",
        "ncaab/totals",
    ]:
        in_dir = BASE_IN / subdir
        out_dir = BASE_OUT / subdir
        out_dir.mkdir(parents=True, exist_ok=True)

        is_ml = subdir.endswith("/ml")
        league = subdir.split("/")[0]

        for f in glob.glob(str(in_dir / "*.csv")):
            df = pd.read_csv(f)
            FILES_PROCESSED += 1
            ROWS_IN += len(df)

            if is_ml:
                if "time" not in df.columns:
                    df["time"] = "00:00"

                if "game_id" not in df.columns:
                    log_error(f"{f} missing game_id — skipping DK merge")
                else:
                    df["game_id"] = df["game_id"].astype(str)

                    stem = Path(f).stem
                    parts = stem.split("_")
                    if len(parts) >= 4:
                        date_suffix = "_".join(parts[-3:])
                        dk_pattern = DK_NORMALIZED / f"dk_{league}_moneyline_{date_suffix}.csv"
                        dk_files = glob.glob(str(dk_pattern))

                        if dk_files:
                            dk_df = pd.read_csv(dk_files[0])

                            if {"game_id", "away_odds", "home_odds"}.issubset(dk_df.columns):
                                dk_df["game_id"] = dk_df["game_id"].astype(str)

                                dk_df = dk_df[["game_id", "away_odds", "home_odds"]].rename(
                                    columns={
                                        "away_odds": "dk_away_odds",
                                        "home_odds": "dk_home_odds",
                                    }
                                )

                                df = df.merge(dk_df, on="game_id", how="left")

                                missing_mask = df["dk_away_odds"].isna() | df["dk_home_odds"].isna()
                                if missing_mask.any():
                                    missing_ids = df.loc[missing_mask, "game_id"].tolist()
                                    log_error(
                                        f"{f} — missing DK match for game_ids: {missing_ids}"
                                    )
                            else:
                                log_error(f"{dk_files[0]} missing required DK columns")
                        else:
                            log_error(f"Missing DK normalized file: {dk_pattern}")
                    else:
                        log_error(f"Unable to parse date from filename: {f}")

            out_path = out_dir / Path(f).name
            df.to_csv(out_path, index=False)

            ROWS_OUT += len(df)
            print(f"Wrote {out_path} | rows={len(df)}")

    print("\n=== FINAL_01 SUMMARY ===")
    print(f"Files processed: {FILES_PROCESSED}")
    print(f"Rows in: {ROWS_IN}")
    print(f"Rows out: {ROWS_OUT}")

    if FILES_PROCESSED == 0:
        raise RuntimeError("final_01: 0 files processed")

    if ROWS_OUT == 0:
        raise RuntimeError("final_01: 0 rows written")


if __name__ == "__main__":
    run()
