#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/01_merge/build_juice_files.py

import math
import sys
import traceback
from pathlib import Path
from datetime import datetime, UTC

import pandas as pd
from scipy.stats import poisson, skellam

BASE_DIR = Path("docs/win/hockey/nhl")
INPUT_DIR = BASE_DIR / "01_merge"
OUTPUT_DIR = INPUT_DIR / "01_merguiced"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR = BASE_DIR / "errors" / "01_merge"
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "build_juice_files.txt"

FATIGUE_FEATURE_COLUMNS = [
    "home_days_rest","away_days_rest","home_back_to_back","away_back_to_back",
    "home_games_in_4_days","away_games_in_4_days","home_three_in_four","away_three_in_four",
    "home_games_in_6_days","away_games_in_6_days","home_four_in_six","away_four_in_six",
    "home_games_in_7_days","away_games_in_7_days","rest_differential",
]

TEAM_STRENGTH_FEATURE_COLUMNS = [
    "home_adj_xgf","away_adj_xgf","adj_xgf_differential",
    "home_adj_xga","away_adj_xga","adj_xga_differential",
    "home_adj_xg_net","away_adj_xg_net","adj_xg_net_differential",
    "home_adj_gf","away_adj_gf","adj_gf_differential",
    "home_adj_ga","away_adj_ga","adj_ga_differential",
    "home_off_rank","away_off_rank","off_rank_differential",
    "home_def_rank","away_def_rank","def_rank_differential",
    "home_net_rank","away_net_rank","net_rank_differential",
    "home_net_z","away_net_z","net_z_differential",
]

LINEUP_NUMERIC_FEATURE_COLUMNS = [
    "home_skater_rapm","away_skater_rapm","skater_rapm_differential",
    "home_skater_war","away_skater_war","skater_war_differential",
    "home_pp_value","away_pp_value","pp_value_differential",
    "home_pk_value","away_pk_value","pk_value_differential",
    "home_forward_line_strength","away_forward_line_strength","forward_line_strength_differential",
    "home_defense_pair_strength","away_defense_pair_strength","defense_pair_strength_differential",
]

LINEUP_METADATA_COLUMNS = [
    "home_lineup_status","away_lineup_status",
    "home_lineup_observed_at","away_lineup_observed_at",
    "home_lineup_source","away_lineup_source",
]

LINEUP_FEATURE_COLUMNS = [
    *LINEUP_NUMERIC_FEATURE_COLUMNS,*LINEUP_METADATA_COLUMNS,
]

GOALIE_FEATURE_COLUMNS = [
    "home_expected_starter",
    "away_expected_starter",
    "home_starter_gsax",
    "away_starter_gsax",
    "home_backup_gsax",
    "away_backup_gsax",
    "starter_gsax_differential",
    "home_goalie_status",
    "away_goalie_status",
    "home_goalie_status_observed_at",
    "away_goalie_status_observed_at",
    "home_goalie_status_source",
    "away_goalie_status_source",
]

GOALIE_NUMERIC_FEATURE_COLUMNS = [
    "home_starter_gsax",
    "away_starter_gsax",
    "home_backup_gsax",
    "away_backup_gsax",
    "starter_gsax_differential",
]

BASE_COLUMNS = [
    "sport","league","game_date","game_time","game_id","away_team","home_team",
    *FATIGUE_FEATURE_COLUMNS,*TEAM_STRENGTH_FEATURE_COLUMNS,*GOALIE_FEATURE_COLUMNS,*LINEUP_FEATURE_COLUMNS,
]

ODDS_PROVENANCE_COLUMNS = [
    "odds_source",
    "pulled_at",
]

MONEYLINE_PROVENANCE_COLUMNS = [
    *ODDS_PROVENANCE_COLUMNS,
    "moneyline_provider_id",
    "moneyline_provider_name",
]

PUCK_LINE_PROVENANCE_COLUMNS = [
    *ODDS_PROVENANCE_COLUMNS,
    "puck_line_provider_id",
    "puck_line_provider_name",
]

TOTAL_PROVENANCE_COLUMNS = [
    *ODDS_PROVENANCE_COLUMNS,
    "total_provider_id",
    "total_provider_name",
]

ALL_ODDS_PROVENANCE_COLUMNS = [
    "odds_source",
    "moneyline_provider_id",
    "moneyline_provider_name",
    "puck_line_provider_id",
    "puck_line_provider_name",
    "total_provider_id",
    "total_provider_name",
    "pulled_at",
]

MERGED_REQUIRED_COLUMNS = BASE_COLUMNS + [
    "away_prob_moneyline","home_prob_moneyline","away_projected_goals","home_projected_goals",
    "total_projected_goals","away_puck_line","home_puck_line","total",
    "away_dk_moneyline_american","home_dk_moneyline_american",
    "away_dk_moneyline_decimal","home_dk_moneyline_decimal",
    "away_dk_puck_line_american","home_dk_puck_line_american",
    "away_dk_puck_line_decimal","home_dk_puck_line_decimal",
    "dk_total_over_american","dk_total_under_american",
    "dk_total_over_decimal","dk_total_under_decimal",
    *ALL_ODDS_PROVENANCE_COLUMNS,
]

MONEYLINE_COLUMNS = BASE_COLUMNS + [
    "away_prob_moneyline","home_prob_moneyline",
    "away_fair_decimal_moneyline","home_fair_decimal_moneyline",
    "away_dk_moneyline_american","home_dk_moneyline_american",
    "away_dk_moneyline_decimal","home_dk_moneyline_decimal",
    *MONEYLINE_PROVENANCE_COLUMNS,
]

PUCK_LINE_COLUMNS = BASE_COLUMNS + [
    "away_puck_line","home_puck_line","away_prob_puck_line","home_prob_puck_line",
    "away_fair_decimal_puck_line","home_fair_decimal_puck_line",
    "away_dk_puck_line_american","home_dk_puck_line_american",
    "away_dk_puck_line_decimal","home_dk_puck_line_decimal",
    *PUCK_LINE_PROVENANCE_COLUMNS,
]

TOTAL_COLUMNS = BASE_COLUMNS + [
    "total","total_projected_goals","over_prob_total","under_prob_total",
    "over_fair_decimal_total","under_fair_decimal_total",
    "dk_total_over_american","dk_total_under_american",
    "dk_total_over_decimal","dk_total_under_decimal",
    *TOTAL_PROVENANCE_COLUMNS,
]

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== build_juice_files RUN {datetime.now(UTC).isoformat()} ===\n")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(UTC).isoformat()} | {msg}\n")

def wipe_output_dir() -> None:
    removed = 0
    for path in OUTPUT_DIR.glob("*.csv"):
        path.unlink()
        removed += 1
    log(f"Wiped pre-juice CSV outputs: {removed}")

def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def fair_decimal(prob):
    if pd.isna(prob) or prob <= 0:
        return None
    return 1 / prob

def calculate_home_puck_probability(home_line, home_projected_goals, away_projected_goals):
    if any(pd.isna(v) for v in (home_line, home_projected_goals, away_projected_goals)):
        return None
    if home_projected_goals <= 0 or away_projected_goals <= 0:
        return None
    threshold = math.floor(-home_line)
    probability = 1 - skellam.cdf(threshold, home_projected_goals, away_projected_goals)
    if pd.isna(probability):
        return None
    return min(max(probability, 0.01), 0.99)

def calculate_away_puck_probability(away_line, away_projected_goals, home_projected_goals):
    if any(pd.isna(v) for v in (away_line, away_projected_goals, home_projected_goals)):
        return None
    if away_projected_goals <= 0 or home_projected_goals <= 0:
        return None
    threshold = math.floor(-away_line)
    probability = 1 - skellam.cdf(threshold, away_projected_goals, home_projected_goals)
    if pd.isna(probability):
        return None
    return min(max(probability, 0.01), 0.99)

def calculate_total_probabilities(total_line, total_projected_goals):
    if pd.isna(total_line) or pd.isna(total_projected_goals) or total_projected_goals <= 0:
        return None, None
    total_line = float(total_line)
    if total_line.is_integer():
        push_total = int(total_line)
        under_prob = poisson.cdf(push_total - 1, total_projected_goals)
        over_prob = 1 - poisson.cdf(push_total, total_projected_goals)
        no_push_prob = under_prob + over_prob
        if pd.isna(no_push_prob) or no_push_prob <= 0:
            return None, None
        under_prob /= no_push_prob
        over_prob /= no_push_prob
    else:
        cutoff = math.floor(total_line)
        under_prob = poisson.cdf(cutoff, total_projected_goals)
        over_prob = 1 - under_prob
    if pd.isna(over_prob) or pd.isna(under_prob):
        return None, None
    return min(max(over_prob, 0.01), 0.99), min(max(under_prob, 0.01), 0.99)

def validate_schema(path: Path, df: pd.DataFrame) -> list[str]:
    return [col for col in MERGED_REQUIRED_COLUMNS if col not in df.columns]

def build_moneyline(df: pd.DataFrame, output_path: Path) -> int:
    moneyline = df.copy()
    moneyline["away_fair_decimal_moneyline"] = moneyline["away_prob_moneyline"].apply(fair_decimal)
    moneyline["home_fair_decimal_moneyline"] = moneyline["home_prob_moneyline"].apply(fair_decimal)
    moneyline = moneyline[MONEYLINE_COLUMNS]
    moneyline.to_csv(output_path, index=False)
    log(f"WROTE {output_path} ({len(moneyline)} rows)")
    return len(moneyline)

def build_puck_line(df: pd.DataFrame, output_path: Path) -> int:
    puck_line = df.copy()
    away_probs, home_probs, away_fair, home_fair = [], [], [], []
    for idx, row in puck_line.iterrows():
        hp = calculate_home_puck_probability(row["home_puck_line"], row["home_projected_goals"], row["away_projected_goals"])
        ap = calculate_away_puck_probability(row["away_puck_line"], row["away_projected_goals"], row["home_projected_goals"])
        if hp is None or ap is None:
            log(f"ROW ISSUE: puck-line probability unavailable idx={idx} game_id={row.get('game_id','')}")
        home_probs.append(hp); away_probs.append(ap)
        home_fair.append(fair_decimal(hp) if hp is not None else None)
        away_fair.append(fair_decimal(ap) if ap is not None else None)
    puck_line["away_prob_puck_line"] = away_probs
    puck_line["home_prob_puck_line"] = home_probs
    puck_line["away_fair_decimal_puck_line"] = away_fair
    puck_line["home_fair_decimal_puck_line"] = home_fair
    puck_line = puck_line[PUCK_LINE_COLUMNS]
    puck_line.to_csv(output_path, index=False)
    log(f"WROTE {output_path} ({len(puck_line)} rows)")
    return len(puck_line)

def build_total(df: pd.DataFrame, output_path: Path) -> int:
    total = df.copy()
    over_probs, under_probs, over_fair, under_fair = [], [], [], []
    for idx, row in total.iterrows():
        op, up = calculate_total_probabilities(row["total"], row["total_projected_goals"])
        if op is None or up is None:
            log(f"ROW ISSUE: total probability unavailable idx={idx} game_id={row.get('game_id','')}")
        over_probs.append(op); under_probs.append(up)
        over_fair.append(fair_decimal(op) if op is not None else None)
        under_fair.append(fair_decimal(up) if up is not None else None)
    total["over_prob_total"] = over_probs
    total["under_prob_total"] = under_probs
    total["over_fair_decimal_total"] = over_fair
    total["under_fair_decimal_total"] = under_fair
    total = total[TOTAL_COLUMNS]
    total.to_csv(output_path, index=False)
    log(f"WROTE {output_path} ({len(total)} rows)")
    return len(total)

def process_file(path: Path) -> list[tuple[str, int]]:
    df = pd.read_csv(path)
    if df.empty:
        log(f"EMPTY: {path} — skipping")
        return []
    missing = validate_schema(path, df)
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    numeric_columns = [
        *FATIGUE_FEATURE_COLUMNS,*TEAM_STRENGTH_FEATURE_COLUMNS,*GOALIE_NUMERIC_FEATURE_COLUMNS,*LINEUP_NUMERIC_FEATURE_COLUMNS,
        "away_prob_moneyline","home_prob_moneyline","away_projected_goals","home_projected_goals",
        "total_projected_goals","away_puck_line","home_puck_line","total",
        "away_dk_moneyline_american","home_dk_moneyline_american",
        "away_dk_moneyline_decimal","home_dk_moneyline_decimal",
        "away_dk_puck_line_american","home_dk_puck_line_american",
        "away_dk_puck_line_decimal","home_dk_puck_line_decimal",
        "dk_total_over_american","dk_total_under_american",
        "dk_total_over_decimal","dk_total_under_decimal",
    ]
    for col in numeric_columns:
        df[col] = to_numeric(df[col])
    slate_date = path.name.replace("_NHL_merged.csv", "")
    outputs = [
        (OUTPUT_DIR / f"{slate_date}_NHL_moneyline.csv", build_moneyline),
        (OUTPUT_DIR / f"{slate_date}_NHL_puck_line.csv", build_puck_line),
        (OUTPUT_DIR / f"{slate_date}_NHL_total.csv", build_total),
    ]
    written = []
    for output_path, fn in outputs:
        written.append((str(output_path), fn(df, output_path)))
    return written

def main() -> None:
    try:
        wipe_output_dir()
        input_files = sorted(INPUT_DIR.glob("*_NHL_merged.csv"))
        log(f"Input files found: {len(input_files)}")
        if not input_files:
            raise FileNotFoundError(f"No merged input files found in {INPUT_DIR}")
        files_written = []
        for path in input_files:
            files_written.extend(process_file(path))
        log(f"Files written: {len(files_written)}")
        log("STATUS: SUCCESS")
    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
