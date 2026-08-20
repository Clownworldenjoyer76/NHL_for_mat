#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/05_final_scores/03_nhl_results_reports.py

from datetime import datetime, UTC
from pathlib import Path

import numpy as np
import pandas as pd


###############################################################
######################## PATH CONFIG ##########################
###############################################################

NHL_ROOT = Path("docs/win/hockey/nhl")
FINAL_ROOT = NHL_ROOT / "05_final_scores"

INPUT_FILE = FINAL_ROOT / "intermediate" / "work_nhl.csv"

REPORT_ROOT = FINAL_ROOT / "reports"
MONEYLINE_DIR = REPORT_ROOT / "moneyline"
PUCKLINE_DIR = REPORT_ROOT / "puckline"
TOTAL_DIR = REPORT_ROOT / "total"

TALLY_FILE = FINAL_ROOT / "nhl_market_tally.csv"

ERROR_DIR = FINAL_ROOT / "errors"
ERROR_DIR.mkdir(parents=True, exist_ok=True)

REPORT_ROOT.mkdir(parents=True, exist_ok=True)
MONEYLINE_DIR.mkdir(parents=True, exist_ok=True)
PUCKLINE_DIR.mkdir(parents=True, exist_ok=True)
TOTAL_DIR.mkdir(parents=True, exist_ok=True)

ERROR_LOG = ERROR_DIR / "03_nhl_results_reports_errors.txt"
SUMMARY_LOG = ERROR_DIR / "03_nhl_results_reports_summary.txt"


###############################################################
######################## REPORT COLUMNS #######################
###############################################################

REPORT_COLUMNS = [
    "league",
    "market_type",
    "side_group",
    "variable",
    "Win",
    "Loss",
    "Push",
    "Total",
    "bets_excluding_pushes",
    "bets_including_pushes",
    "Win_Pct",
    "units",
    "roi",
    "avg_odds",
    "avg_ev",
    "avg_kelly",
    "avg_win_prob",
]


###############################################################
######################## BUCKET CONFIG ########################
###############################################################

EV_BANDS = [
    (0.00, 0.01),
    (0.01, 0.02),
    (0.02, 0.03),
    (0.03, 0.04),
    (0.04, 0.05),
    (0.05, 0.075),
    (0.075, 0.10),
    (0.10, 999),
]

KELLY_BANDS = [
    (0.00, 0.01),
    (0.01, 0.02),
    (0.02, 0.05),
    (0.05, 0.10),
    (0.10, 0.20),
    (0.20, 0.50),
    (0.50, 999),
]

ODDS_BANDS = [
    (-999, -200),
    (-199, -150),
    (-149, -125),
    (-124, -110),
    (-109, 100),
    (101, 125),
    (126, 150),
    (151, 200),
    (201, 999),
]

WIN_PROB_BANDS = {
    "moneyline": [
        (0.00, 0.45),
        (0.45, 0.50),
        (0.50, 0.55),
        (0.55, 0.60),
        (0.60, 0.65),
        (0.65, 0.70),
        (0.70, 1.00),
    ],
    "puck_line": [
        (0.00, 0.45),
        (0.45, 0.50),
        (0.50, 0.55),
        (0.55, 0.60),
        (0.60, 0.65),
        (0.65, 0.70),
        (0.70, 1.00),
    ],
    "total": [
        (0.00, 0.45),
        (0.45, 0.50),
        (0.50, 0.55),
        (0.55, 0.60),
        (0.60, 0.65),
        (0.65, 0.70),
        (0.70, 1.00),
    ],
}

TOTAL_RANGE_BANDS = [
    (0.0, 5.5),
    (5.5, 6.0),
    (6.0, 6.5),
    (6.5, 7.0),
    (7.0, 7.5),
    (7.5, 8.0),
    (8.0, 999),
]


###############################################################
######################## LOGGING ##############################
###############################################################

def reset_logs() -> None:
    ERROR_LOG.write_text("", encoding="utf-8")
    SUMMARY_LOG.write_text("", encoding="utf-8")


def log_error(msg: str) -> None:
    with ERROR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now(UTC).isoformat()}] {msg}\n")


def log_summary(msg: str) -> None:
    with SUMMARY_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now(UTC).isoformat()}] {msg}\n")


###############################################################
######################## HELPERS ##############################
###############################################################

def safe_read(path: Path) -> pd.DataFrame:
    try:
        path = Path(path)

        if not path.exists():
            log_error(f"MISSING FILE | {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df.empty:
            log_error(f"EMPTY FILE | {path}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"READ ERROR | {path} | {e}")
        return pd.DataFrame()


def normalize_market(value) -> str:
    value = str(value).strip().lower()

    if value in {"moneyline", "ml"}:
        return "moneyline"

    if value in {"puck_line", "puckline", "spread"}:
        return "puck_line"

    if value in {"total", "totals"}:
        return "total"

    return value


def normalize_side(value) -> str:
    return str(value).strip().lower()


def side_group(row) -> str:
    market = normalize_market(row.get("market_type", ""))
    side = normalize_side(row.get("bet_side", ""))

    if market in {"moneyline", "puck_line"}:
        if side == "home":
            return "HOME"
        if side == "away":
            return "AWAY"

    if market == "total":
        if side == "over":
            return "OVER"
        if side == "under":
            return "UNDER"

    return side.upper()


def band_label(low: float, high: float) -> str:
    def fmt(v: float) -> str:
        if abs(v) >= 100:
            return str(int(v))
        if float(v).is_integer():
            return f"{v:.1f}" if abs(v) < 10 else str(int(v))
        return str(v).rstrip("0").rstrip(".")

    return f"{fmt(low)}_to_{fmt(high)}"


def bucket_value(value, bands: list[tuple[float, float]]) -> str:
    if pd.isna(value):
        return "missing"

    try:
        v = float(value)
    except Exception:
        return "missing"

    for low, high in bands:
        if low <= v <= high:
            return band_label(low, high)

    return "out_of_range"


def win_prob_bucket(row) -> str:
    market = normalize_market(row.get("market_type", ""))
    bands = WIN_PROB_BANDS.get(market, [])
    return bucket_value(row.get("model_prob"), bands)


def american_to_profit_per_unit(odds) -> float:
    if pd.isna(odds):
        return np.nan

    try:
        odds = float(odds)
    except Exception:
        return np.nan

    if odds > 0:
        return odds / 100.0

    if odds < 0:
        return 100.0 / abs(odds)

    return np.nan


def grade_to_units(row) -> float:
    result = str(row.get("bet_result", "")).strip().lower()
    odds = row.get("dk_odds_american", np.nan)
    profit_per_unit = american_to_profit_per_unit(odds)

    if result == "win":
        return profit_per_unit if not pd.isna(profit_per_unit) else np.nan

    if result == "loss":
        return -1.0

    if result == "push":
        return 0.0

    return np.nan


def empty_report_df() -> pd.DataFrame:
    return pd.DataFrame(columns=REPORT_COLUMNS)


def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["league"] = "nhl"
    df["market_type"] = df["market_type"].map(normalize_market)
    df["bet_side"] = df["bet_side"].map(normalize_side)
    df["side_group"] = df.apply(side_group, axis=1)

    numeric_cols = [
        "line",
        "dk_odds_american",
        "dk_odds_decimal",
        "model_prob",
        "edge",
        "ev",
        "kelly",
        "away_score",
        "home_score",
        "total_score",
        "away_puck_line_result",
        "home_puck_line_result",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    result_clean = df["bet_result"].astype(str).str.strip().str.lower()

    df["is_win"] = (result_clean == "win").astype(int)
    df["is_loss"] = (result_clean == "loss").astype(int)
    df["is_push"] = (result_clean == "push").astype(int)
    df["units"] = df.apply(grade_to_units, axis=1)

    df["ev_bucket"] = df["ev"].apply(lambda v: bucket_value(v, EV_BANDS))
    df["kelly_bucket"] = df["kelly"].apply(lambda v: bucket_value(v, KELLY_BANDS))
    df["odds_bucket"] = df["dk_odds_american"].apply(lambda v: bucket_value(v, ODDS_BANDS))
    df["win_prob_bucket"] = df.apply(win_prob_bucket, axis=1)
    df["side_bucket"] = df["side_group"]
    df["total_range_bucket"] = df["line"].apply(lambda v: bucket_value(v, TOTAL_RANGE_BANDS))

    return df


###############################################################
######################## SUMMARIES ############################
###############################################################

def summarize(df: pd.DataFrame, market_type: str, variable_col: str, include_side: bool) -> pd.DataFrame:
    if df.empty:
        return empty_report_df()

    group_cols = ["league", "market_type", variable_col]

    if include_side:
        group_cols.insert(2, "side_group")

    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            Win=("is_win", "sum"),
            Loss=("is_loss", "sum"),
            Push=("is_push", "sum"),
            units=("units", "sum"),
            avg_odds=("dk_odds_american", "mean"),
            avg_ev=("ev", "mean"),
            avg_kelly=("kelly", "mean"),
            avg_win_prob=("model_prob", "mean"),
        )
        .reset_index()
    )

    grouped["Total"] = grouped["Win"] + grouped["Loss"] + grouped["Push"]
    grouped["bets_excluding_pushes"] = grouped["Win"] + grouped["Loss"]
    grouped["bets_including_pushes"] = grouped["Total"]

    grouped["Win_Pct"] = np.where(
        grouped["bets_excluding_pushes"] > 0,
        grouped["Win"] / grouped["bets_excluding_pushes"],
        np.nan,
    )

    grouped["roi"] = np.where(
        grouped["bets_including_pushes"] > 0,
        grouped["units"] / grouped["bets_including_pushes"],
        np.nan,
    )

    grouped = grouped.rename(columns={variable_col: "variable"})

    if not include_side:
        grouped["side_group"] = "ALL"

    grouped["market_type"] = market_type

    grouped = grouped[REPORT_COLUMNS]

    grouped = grouped.sort_values(
        ["league", "market_type", "side_group", "variable"],
        kind="stable",
    ).reset_index(drop=True)

    return grouped


def write_report(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log_summary(f"WROTE REPORT | {path} | rows={len(df)}")


def write_pair(
    market_df: pd.DataFrame,
    market_type: str,
    out_dir: Path,
    prefix: str,
    report_name: str,
    variable_col: str,
) -> None:
    base = summarize(market_df, market_type, variable_col, include_side=False)
    side = summarize(market_df, market_type, variable_col, include_side=True)

    write_report(base, out_dir / f"{prefix}_by_{report_name}.csv")
    write_report(side, out_dir / f"{prefix}_by_{report_name}_home_away_summary.csv")


def write_market_tally(df: pd.DataFrame) -> None:
    rows = []

    for market_type in ["moneyline", "puck_line", "total"]:
        market_df = df[df["market_type"] == market_type].copy()

        if market_df.empty:
            row = {
                "league": "nhl",
                "market_type": market_type,
                "side_group": "ALL",
                "variable": "ALL",
                "Win": 0,
                "Loss": 0,
                "Push": 0,
                "Total": 0,
                "bets_excluding_pushes": 0,
                "bets_including_pushes": 0,
                "Win_Pct": np.nan,
                "units": np.nan,
                "roi": np.nan,
                "avg_odds": np.nan,
                "avg_ev": np.nan,
                "avg_kelly": np.nan,
                "avg_win_prob": np.nan,
            }
            rows.append(row)
            continue

        summary = summarize(market_df, market_type, "all_bucket", include_side=False)
        rows.extend(summary.to_dict("records"))

    tally = pd.DataFrame(rows, columns=REPORT_COLUMNS)
    tally.to_csv(TALLY_FILE, index=False)
    log_summary(f"WROTE MARKET TALLY | {TALLY_FILE} | rows={len(tally)}")


###############################################################
######################## MAIN #################################
###############################################################

def main() -> None:
    reset_logs()

    log_summary("START 03_nhl_results_reports.py")
    log_summary(f"INPUT_FILE={INPUT_FILE}")
    log_summary(f"REPORT_ROOT={REPORT_ROOT}")

    df = safe_read(INPUT_FILE)

    if df.empty:
        log_error("STOPPING: work file missing or empty")
        print("NHL reports failed: work file missing or empty.")
        return

    required = [
        "league",
        "game_date",
        "game_id",
        "away_team",
        "home_team",
        "market_type",
        "bet_side",
        "line",
        "dk_odds_american",
        "model_prob",
        "ev",
        "kelly",
        "bet_result",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        log_error(f"STOPPING: missing required columns {missing}")
        print(f"NHL reports failed: missing columns {missing}")
        return

    df = prepare_df(df)
    df["all_bucket"] = "ALL"

    moneyline_df = df[df["market_type"] == "moneyline"].copy()
    puckline_df = df[df["market_type"] == "puck_line"].copy()
    total_df = df[df["market_type"] == "total"].copy()

    ###########################################################
    # MONEYLINE
    ###########################################################

    write_pair(
        moneyline_df,
        "moneyline",
        MONEYLINE_DIR,
        "nhl_moneyline",
        "ev",
        "ev_bucket",
    )

    write_pair(
        moneyline_df,
        "moneyline",
        MONEYLINE_DIR,
        "nhl_moneyline",
        "kelly",
        "kelly_bucket",
    )

    write_pair(
        moneyline_df,
        "moneyline",
        MONEYLINE_DIR,
        "nhl_moneyline",
        "odds",
        "odds_bucket",
    )

    write_pair(
        moneyline_df,
        "moneyline",
        MONEYLINE_DIR,
        "nhl_moneyline",
        "win_prob",
        "win_prob_bucket",
    )

    ###########################################################
    # PUCK LINE
    ###########################################################

    write_pair(
        puckline_df,
        "puck_line",
        PUCKLINE_DIR,
        "nhl_puck_line",
        "ev",
        "ev_bucket",
    )

    write_pair(
        puckline_df,
        "puck_line",
        PUCKLINE_DIR,
        "nhl_puck_line",
        "kelly",
        "kelly_bucket",
    )

    write_pair(
        puckline_df,
        "puck_line",
        PUCKLINE_DIR,
        "nhl_puck_line",
        "odds",
        "odds_bucket",
    )

    write_pair(
        puckline_df,
        "puck_line",
        PUCKLINE_DIR,
        "nhl_puck_line",
        "side",
        "side_bucket",
    )

    write_pair(
        puckline_df,
        "puck_line",
        PUCKLINE_DIR,
        "nhl_puck_line",
        "win_prob",
        "win_prob_bucket",
    )

    ###########################################################
    # TOTAL
    ###########################################################

    write_pair(
        total_df,
        "total",
        TOTAL_DIR,
        "nhl_total",
        "ev",
        "ev_bucket",
    )

    write_pair(
        total_df,
        "total",
        TOTAL_DIR,
        "nhl_total",
        "kelly",
        "kelly_bucket",
    )

    write_pair(
        total_df,
        "total",
        TOTAL_DIR,
        "nhl_total",
        "odds",
        "odds_bucket",
    )

    write_pair(
        total_df,
        "total",
        TOTAL_DIR,
        "nhl_total",
        "side",
        "side_bucket",
    )

    write_pair(
        total_df,
        "total",
        TOTAL_DIR,
        "nhl_total",
        "total_range",
        "total_range_bucket",
    )

    write_pair(
        total_df,
        "total",
        TOTAL_DIR,
        "nhl_total",
        "win_prob",
        "win_prob_bucket",
    )

    write_market_tally(df)

    log_summary("END 03_nhl_results_reports.py")
    print("NHL reports complete.")


if __name__ == "__main__":
    main()