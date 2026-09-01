#!/usr/bin/env python3
# docs/win/basketball/scripts/03_edges/compute_edges.py

import pandas as pd
from pathlib import Path
from datetime import datetime
import traceback
import re

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/basketball/02_juice")
OUTPUT_DIR = Path("docs/win/basketball/03_edges")
ERROR_DIR = Path("docs/win/basketball/errors/03_edges")
ERROR_LOG = ERROR_DIR / "compute_edges.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def validate_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def edge_decimal(dk: pd.Series, juice_decimal: pd.Series) -> pd.Series:
    dk = pd.to_numeric(dk, errors="coerce")
    j = pd.to_numeric(juice_decimal, errors="coerce")
    return dk - j


def edge_pct(dk: pd.Series, juice_decimal: pd.Series) -> pd.Series:
    dk = pd.to_numeric(dk, errors="coerce")
    j = pd.to_numeric(juice_decimal, errors="coerce")
    out = (dk - j) / j
    return out.where(j > 0)


def atomic_write_csv(df: pd.DataFrame, output_path: Path) -> None:
    tmp = output_path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(output_path)


def extract_date_from_filename(filename: str) -> str:
    match = re.search(r"\d{4}_\d{2}_\d{2}", filename)
    if not match:
        raise ValueError(f"No date found in filename: {filename}")
    return match.group(0)

# =========================
# EDGE COMPUTATION
# =========================

def compute_moneyline_edges(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "home_dk_decimal_moneyline",
        "away_dk_decimal_moneyline",
        "home_juice_decimal_moneyline",
        "away_juice_decimal_moneyline",
    ]
    validate_columns(df, required)

    df["home_edge_decimal"] = edge_decimal(
        df["home_dk_decimal_moneyline"],
        df["home_juice_decimal_moneyline"],
    )
    df["home_edge_pct"] = edge_pct(
        df["home_dk_decimal_moneyline"],
        df["home_juice_decimal_moneyline"],
    )
    df["home_play"] = df["home_edge_decimal"] > 0

    df["away_edge_decimal"] = edge_decimal(
        df["away_dk_decimal_moneyline"],
        df["away_juice_decimal_moneyline"],
    )
    df["away_edge_pct"] = edge_pct(
        df["away_dk_decimal_moneyline"],
        df["away_juice_decimal_moneyline"],
    )
    df["away_play"] = df["away_edge_decimal"] > 0

    return df


def compute_spread_edges(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "home_dk_spread_decimal",
        "away_dk_spread_decimal",
        "home_spread_juice_decimal",
        "away_spread_juice_decimal",
    ]
    validate_columns(df, required)

    df["home_edge_decimal"] = edge_decimal(
        df["home_dk_spread_decimal"],
        df["home_spread_juice_decimal"],
    )
    df["home_edge_pct"] = edge_pct(
        df["home_dk_spread_decimal"],
        df["home_spread_juice_decimal"],
    )
    df["home_play"] = df["home_edge_decimal"] > 0

    df["away_edge_decimal"] = edge_decimal(
        df["away_dk_spread_decimal"],
        df["away_spread_juice_decimal"],
    )
    df["away_edge_pct"] = edge_pct(
        df["away_dk_spread_decimal"],
        df["away_spread_juice_decimal"],
    )
    df["away_play"] = df["away_edge_decimal"] > 0

    return df


def compute_total_edges(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "dk_total_over_decimal",
        "dk_total_under_decimal",
        "total_over_juice_decimal",
        "total_under_juice_decimal",
    ]
    validate_columns(df, required)

    df["over_edge_decimal"] = edge_decimal(
        df["dk_total_over_decimal"],
        df["total_over_juice_decimal"],
    )
    df["over_edge_pct"] = edge_pct(
        df["dk_total_over_decimal"],
        df["total_over_juice_decimal"],
    )
    df["over_play"] = df["over_edge_decimal"] > 0

    df["under_edge_decimal"] = edge_decimal(
        df["dk_total_under_decimal"],
        df["total_under_juice_decimal"],
    )
    df["under_edge_pct"] = edge_pct(
        df["dk_total_under_decimal"],
        df["total_under_juice_decimal"],
    )
    df["under_play"] = df["under_edge_decimal"] > 0

    return df

# =========================
# PROCESSING
# =========================

def process_market_files(files, compute_fn, league: str, market: str):
    for f in files:
        df = pd.read_csv(f)
        df = compute_fn(df)
        date = extract_date_from_filename(f.name)

        output_name = f"{date}_basketball_{league}_{market}.csv"
        output_path = OUTPUT_DIR / output_name
        atomic_write_csv(df, output_path)


def process_league(league: str):
    process_market_files(
        sorted(INPUT_DIR.glob(f"*_{league}_moneyline.csv")),
        compute_moneyline_edges,
        league,
        "moneyline"
    )
    process_market_files(
        sorted(INPUT_DIR.glob(f"*_{league}_spread.csv")),
        compute_spread_edges,
        league,
        "spread"
    )
    process_market_files(
        sorted(INPUT_DIR.glob(f"*_{league}_total.csv")),
        compute_total_edges,
        league,
        "total"
    )


def main():
    with open(ERROR_LOG, "w") as log:
        log.write("=== BASKETBALL COMPUTE EDGES RUN ===\n")
        log.write(f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n")
        try:
            process_league("NBA")
            process_league("NCAAB")
        except Exception as e:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    main()
