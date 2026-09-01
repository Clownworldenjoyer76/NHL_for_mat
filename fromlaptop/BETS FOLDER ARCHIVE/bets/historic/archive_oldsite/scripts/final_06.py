import pandas as pd
import glob
from pathlib import Path

STEP1 = Path("docs/win/final/step_1")
STEP2 = Path("docs/win/final/step_2")
STEP3 = Path("docs/win/final/step_3")

FILES = 0
ROWS_IN = 0
ROWS_OUT = 0


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def write_filtered(step2_pattern, step1_pattern, juice_dk_pairs, keep_cols, extra_merge_cols=None):
    global FILES, ROWS_IN, ROWS_OUT

    step1_map = {}

    if step1_pattern:
        for f in glob.glob(str(step1_pattern)):
            df = pd.read_csv(f)
            df["game_id"] = df["game_id"].astype(str)
            step1_map[Path(f).name] = df

    for f in glob.glob(str(step2_pattern)):
        df = pd.read_csv(f)
        FILES += 1
        ROWS_IN += len(df)

        if "game_id" not in df.columns:
            raise RuntimeError(f"{f} missing game_id")

        df["game_id"] = df["game_id"].astype(str)

        mask = pd.Series(False, index=df.index)

        for juice_col, comp_col in juice_dk_pairs:
            if juice_col in df.columns and comp_col in df.columns:
                valid = df[juice_col].notna() & df[comp_col].notna()
                mask |= valid & (df[comp_col] > df[juice_col])

        filtered = df[mask].copy()

        if filtered.empty:
            continue

        if extra_merge_cols:
            step1_file = Path(f).name
            if step1_file in step1_map:
                step1_df = step1_map[step1_file][["game_id"] + extra_merge_cols]
                filtered = filtered.merge(step1_df, on="game_id", how="left")

        missing = [c for c in keep_cols if c not in filtered.columns]
        if missing:
            raise RuntimeError(f"{f} missing required columns: {missing}")

        out_df = filtered[keep_cols]

        out_path = STEP3 / Path(f).relative_to(STEP2)
        ensure_dir(out_path.parent)
        out_df.to_csv(out_path, index=False)

        ROWS_OUT += len(out_df)

        print(f"Wrote {out_path} | kept={len(out_df)} of {len(df)}")


def run():
    # ---------------- MONEYLINE ----------------
    write_filtered(
        STEP2 / "*/ml/juice_*_ml_*.csv",
        None,
        [
            ("deci_home_ml_juice_odds", "deci_dk_home_odds"),
            ("deci_away_ml_juice_odds", "deci_dk_away_odds"),
        ],
        [
            "date", "time", "away_team", "home_team", "league", "game_id",
            "deci_home_ml_juice_odds", "deci_away_ml_juice_odds",
            "deci_dk_home_odds", "deci_dk_away_odds",
        ],
    )

    # ---------------- SPREADS ----------------
    write_filtered(
        STEP2 / "*/spreads/juice_*_spreads_*.csv",
        None,
        [
            ("deci_home_spread_juice_odds", "deci_dk_home_odds"),
            ("deci_away_spread_juice_odds", "deci_dk_away_odds"),
        ],
        [
            "date", "time", "away_team", "home_team", "league", "game_id",
            "away_spread", "home_spread",
            "deci_home_spread_juice_odds", "deci_away_spread_juice_odds",
            "deci_dk_away_odds", "deci_dk_home_odds",
        ],
    )

    # ---------------- TOTALS ----------------
    write_filtered(
        STEP2 / "*/totals/juice_*_totals_*.csv",
        None,
        [
            ("deci_over_juice_odds", "deci_dk_over_odds"),
            ("deci_under_juice_odds", "deci_dk_under_odds"),
        ],
        [
            "date", "time", "away_team", "home_team", "league", "game_id",
            "total",
            "deci_over_juice_odds", "deci_under_juice_odds",
            "deci_dk_over_odds", "deci_dk_under_odds",
        ],
    )

    print("\n=== FINAL_06 SUMMARY ===")
    print(f"Files processed: {FILES}")
    print(f"Rows in: {ROWS_IN}")
    print(f"Rows out (value edges): {ROWS_OUT}")

    if FILES == 0:
        raise RuntimeError("final_06: 0 files processed")

    if ROWS_OUT == 0:
        raise RuntimeError("final_06: 0 value rows produced")

    if ROWS_OUT > ROWS_IN:
        raise RuntimeError("Integrity error: rows_out > rows_in")


if __name__ == "__main__":
    run()
