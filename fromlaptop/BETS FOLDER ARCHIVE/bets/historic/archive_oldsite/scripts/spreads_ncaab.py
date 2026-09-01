# scripts/spreads_ncaab.py

import os
import glob
from pathlib import Path
from datetime import datetime

import pandas as pd
from scipy.stats import norm

EDGE = 0.05
NCAAB_STD_DEV = 15  # raised from 11 to 15


def utc_ts() -> str:
    return datetime.utcnow().isoformat() + "Z"


def resolve_repo_root() -> Path:
    # scripts/spreads_ncaab.py -> scripts -> repo root
    return Path(__file__).resolve().parents[1]


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_line(fp: Path, line: str) -> None:
    fp.parent.mkdir(parents=True, exist_ok=True)
    with fp.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def to_american(dec):
    if pd.isna(dec) or dec <= 1:
        return ""
    if dec >= 2:
        return f"+{int((dec - 1) * 100)}"
    return f"-{int(100 / (dec - 1))}"


def clamp_probability(p):
    return min(max(p, 0.05), 0.95)


def process_spreads():
    repo_root = resolve_repo_root()

    cleaned_dir = repo_root / "docs/win/dump/csvs/cleaned"
    normalized_dir = repo_root / "docs/win/manual/normalized"
    output_dir = repo_root / "docs/win/ncaab/spreads"
    error_dir = repo_root / "docs/win/errors/06_spreads"
    error_log = error_dir / "spreads_ncaab_errors.txt"

    safe_mkdir(output_dir)
    safe_mkdir(error_dir)

    # Always overwrite log at start so each run is unmistakable
    error_log.write_text("", encoding="utf-8")

    def log(msg: str) -> None:
        line = f"{utc_ts()} | {msg}"
        write_line(error_log, line)
        print(line, flush=True)

    log("=== NCAAB SPREADS RUN START ===")
    log(f"repo_root={repo_root}")
    log(f"cwd={Path.cwd()}")
    log(f"cleaned_dir={cleaned_dir} exists={cleaned_dir.exists()}")
    log(f"normalized_dir={normalized_dir} exists={normalized_dir.exists()}")
    log(f"output_dir={output_dir} exists={output_dir.exists()}")
    log(f"error_log={error_log} exists={error_log.exists()}")

    projection_files = sorted(glob.glob(str(cleaned_dir / "ncaab_*.csv")))
    log(f"projection_files_count={len(projection_files)}")

    if not projection_files:
        log("ERROR | No NCAAB projection files found (glob returned 0)")
        log("=== SUMMARY | files_written=0 files_skipped=0 merge_empty=0 missing_dk=0 missing_cols=0 exceptions=0 ===")
        log("=== NCAAB SPREADS RUN END ===")
        return

    files_written = 0
    files_skipped = 0
    merge_empty = 0
    missing_dk = 0
    missing_cols = 0
    exceptions = 0
    total_merged_rows = 0

    dk_required_cols = [
        "game_id", "league", "date", "time",
        "away_team", "home_team",
        "away_spread", "home_spread",
        "away_handle_pct", "home_handle_pct",
        "away_bets_pct", "home_bets_pct",
    ]

    for proj_path in projection_files:
        try:
            proj_path = str(proj_path)
            stem = Path(proj_path).stem
            date_suffix = "_".join(stem.split("_")[1:])
            dk_path = normalized_dir / f"dk_ncaab_spreads_{date_suffix}.csv"
            out_path = output_dir / f"spreads_ncaab_{date_suffix}.csv"

            log(f"file={proj_path} date_suffix={date_suffix}")
            log(f"dk_path={dk_path} exists={dk_path.exists()} out_path={out_path}")

            if not dk_path.exists():
                missing_dk += 1
                files_skipped += 1
                log(f"ERROR | Missing DK file: {dk_path}")
                continue

            df_proj = pd.read_csv(proj_path)
            df_dk = pd.read_csv(dk_path)

            # Column check
            miss = [c for c in dk_required_cols if c not in df_dk.columns]
            if miss:
                missing_cols += 1
                files_skipped += 1
                log(f"ERROR | Missing DK columns: {miss}")
                continue

            df_dk_subset = df_dk[dk_required_cols]

            if "game_id" not in df_proj.columns:
                exceptions += 1
                files_skipped += 1
                log("ERROR | Projection missing game_id column")
                continue

            needed_proj_cols = [
                "game_id",
                "away_team_projected_points",
                "home_team_projected_points",
                "game_projected_points",
            ]
            missp = [c for c in needed_proj_cols if c not in df_proj.columns]
            if missp:
                exceptions += 1
                files_skipped += 1
                log(f"ERROR | Projection missing columns: {missp}")
                continue

            df_proj = df_proj[needed_proj_cols]

            merged = pd.merge(df_dk_subset, df_proj, on="game_id", how="inner")
            log(f"merged_rows={len(merged)} dk_rows={len(df_dk_subset)} proj_rows={len(df_proj)}")

            if merged.empty:
                merge_empty += 1
                files_skipped += 1
                log(f"ERROR | No merge rows for {proj_path}")
                continue

            total_merged_rows += len(merged)

            merged["proj_home_margin"] = (
                merged["home_team_projected_points"] - merged["away_team_projected_points"]
            )

            merged["home_spread_probability"] = merged.apply(
                lambda x: 1 - norm.cdf(
                    -x["home_spread"],
                    x["proj_home_margin"],
                    NCAAB_STD_DEV,
                ),
                axis=1,
            )

            merged["home_spread_probability"] = merged["home_spread_probability"].apply(clamp_probability)
            merged["away_spread_probability"] = 1 - merged["home_spread_probability"]

            merged["home_spread_acceptable_decimal_odds"] = (1 / merged["home_spread_probability"]) * (1 + EDGE)
            merged["away_spread_acceptable_decimal_odds"] = (1 / merged["away_spread_probability"]) * (1 + EDGE)

            merged["home_spread_acceptable_american_odds"] = merged["home_spread_acceptable_decimal_odds"].apply(to_american)
            merged["away_spread_acceptable_american_odds"] = merged["away_spread_acceptable_decimal_odds"].apply(to_american)

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

            merged[cols].to_csv(out_path, index=False)
            files_written += 1
            log(f"WROTE | {out_path} rows={len(merged)}")

        except Exception as e:
            exceptions += 1
            files_skipped += 1
            log(f"ERROR | {proj_path} failed: {repr(e)}")

    log(
        "=== SUMMARY | "
        f"files_written={files_written} "
        f"files_skipped={files_skipped} "
        f"merge_empty={merge_empty} "
        f"missing_dk={missing_dk} "
        f"missing_cols={missing_cols} "
        f"exceptions={exceptions} "
        f"total_merged_rows={total_merged_rows} "
        "==="
    )
    log("=== NCAAB SPREADS RUN END ===")


if __name__ == "__main__":
    process_spreads()
