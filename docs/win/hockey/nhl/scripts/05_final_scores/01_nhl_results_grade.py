#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/05_final_scores/01_nhl_results_grade.py

from datetime import datetime, UTC
from pathlib import Path

import pandas as pd


###############################################################
######################## PATH CONFIG ##########################
###############################################################

NHL_ROOT = Path("docs/win/hockey/nhl")
FINAL_ROOT = NHL_ROOT / "05_final_scores"

SELECT_DIR = NHL_ROOT / "04_select"
SCORE_DIR = FINAL_ROOT / "final_scores"
GRADED_DIR = FINAL_ROOT / "graded"

INTERMEDIATE_DIR = FINAL_ROOT / "intermediate"
ERROR_DIR = FINAL_ROOT / "errors"

GRADED_DIR.mkdir(parents=True, exist_ok=True)
INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

GRADE_ERROR_LOG = ERROR_DIR / "01_nhl_results_grade_errors.txt"
GRADE_SUMMARY_LOG = ERROR_DIR / "01_nhl_results_grade_summary.txt"

SELECT_PATTERN = "*_NHL.csv"
SCORE_PATTERN = "*_NHL_final_scores.csv"

MASTER_FILE = GRADED_DIR / "NHL_final.csv"


###############################################################
######################## LOGGING ##############################
###############################################################

def reset_logs() -> None:
    GRADE_ERROR_LOG.write_text("", encoding="utf-8")
    GRADE_SUMMARY_LOG.write_text("", encoding="utf-8")


def log_error(msg: str) -> None:
    with GRADE_ERROR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now(UTC).isoformat()}] {msg}\n")


def log_summary(msg: str) -> None:
    with GRADE_SUMMARY_LOG.open("a", encoding="utf-8") as f:
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

        df = pd.read_csv(path, dtype=str)

        if df is None or df.empty:
            log_error(f"EMPTY FILE | {path}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"READ ERROR | {path} | {e}")
        return pd.DataFrame()


def normalize_date(value) -> str:
    return str(value).strip().replace("-", "_")


def normalize_team(value) -> str:
    return str(value).strip()


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


def to_float(value):
    try:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def require_columns(df: pd.DataFrame, required: list[str], label: str) -> bool:
    missing = [c for c in required if c not in df.columns]
    if missing:
        log_error(f"MISSING COLUMNS | {label} | {missing}")
        return False
    return True


def clean_old_outputs() -> None:
    for path in GRADED_DIR.glob("*_results_NHL.csv"):
        path.unlink(missing_ok=True)

    MASTER_FILE.unlink(missing_ok=True)

    log_summary("CLEARED OLD NHL GRADED OUTPUTS")


###############################################################
####################### LOAD INPUTS ###########################
###############################################################

def load_select_rows() -> pd.DataFrame:
    select_files = sorted(SELECT_DIR.glob(SELECT_PATTERN))

    if not select_files:
        log_error(f"NO SELECT FILES FOUND | {SELECT_DIR} | pattern={SELECT_PATTERN}")
        return pd.DataFrame()

    required = [
        "sport",
        "league",
        "game_date",
        "game_time",
        "game_id",
        "away_team",
        "home_team",
        "market_type",
        "bet_side",
        "line",
        "take_bet",
        "dk_odds_american",
        "dk_odds_decimal",
        "model_prob",
        "edge",
        "ev",
        "kelly",
    ]

    parts = []

    for path in select_files:
        df = safe_read(path)
        if df.empty:
            continue

        if not require_columns(df, required, str(path)):
            continue

        df = df.copy()
        df["source_select_file"] = path.name
        df["game_date"] = df["game_date"].map(normalize_date)
        df["away_team"] = df["away_team"].map(normalize_team)
        df["home_team"] = df["home_team"].map(normalize_team)
        df["game_id"] = df["game_id"].astype(str).str.strip()
        df["market_type"] = df["market_type"].map(normalize_market)
        df["bet_side"] = df["bet_side"].map(normalize_side)

        parts.append(df)

    if not parts:
        log_error("NO VALID SELECT ROWS FOUND")
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    log_summary(f"SELECT ROWS LOADED | files={len(select_files)} | rows={len(out)}")
    return out


def load_score_rows() -> pd.DataFrame:
    score_files = sorted(SCORE_DIR.glob(SCORE_PATTERN))

    if not score_files:
        log_error(f"NO FINAL SCORE FILES FOUND | {SCORE_DIR} | pattern={SCORE_PATTERN}")
        return pd.DataFrame()

    required = [
        "sport",
        "league",
        "game_date",
        "game_id",
        "away_team",
        "home_team",
        "away_score",
        "home_score",
        "total_score",
        "away_puck_line_result",
        "home_puck_line_result",
    ]

    parts = []

    for path in score_files:
        df = safe_read(path)
        if df.empty:
            continue

        if not require_columns(df, required, str(path)):
            continue

        df = df.copy()
        df["source_score_file"] = path.name
        df["game_date"] = df["game_date"].map(normalize_date)
        df["away_team"] = df["away_team"].map(normalize_team)
        df["home_team"] = df["home_team"].map(normalize_team)
        df["game_id"] = df["game_id"].astype(str).str.strip()

        parts.append(df)

    if not parts:
        log_error("NO VALID FINAL SCORE ROWS FOUND")
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    log_summary(f"FINAL SCORE ROWS LOADED | files={len(score_files)} | rows={len(out)}")
    return out


###############################################################
###################### SCORE LOOKUPS ##########################
###############################################################

def team_key(row) -> str:
    return (
        f"{normalize_date(row.get('game_date', ''))}|"
        f"{normalize_team(row.get('away_team', ''))}|"
        f"{normalize_team(row.get('home_team', ''))}"
    )


def build_score_indexes(scores: pd.DataFrame) -> tuple[dict[str, dict], dict[str, dict]]:
    by_game_id: dict[str, dict] = {}
    by_team_key: dict[str, dict] = {}

    for _, row in scores.iterrows():
        rec = row.to_dict()

        game_id = str(rec.get("game_id", "")).strip()
        if game_id:
            if game_id in by_game_id:
                log_error(f"DUPLICATE FINAL SCORE game_id | {game_id}")
            else:
                by_game_id[game_id] = rec

        key = team_key(rec)
        if key in by_team_key:
            log_error(f"DUPLICATE FINAL SCORE team/date key | {key}")
        else:
            by_team_key[key] = rec

    return by_game_id, by_team_key


def attach_score_to_bet(bet: dict, by_game_id: dict[str, dict], by_team_key: dict[str, dict]) -> dict | None:
    game_id = str(bet.get("game_id", "")).strip()

    if game_id and game_id in by_game_id:
        return by_game_id[game_id]

    key = team_key(bet)
    if key in by_team_key:
        return by_team_key[key]

    return None


###############################################################
######################## OUTCOME LOGIC ########################
###############################################################

def determine_outcome(row: dict) -> str:
    try:
        market = normalize_market(row.get("market_type", ""))
        side = normalize_side(row.get("bet_side", ""))

        away_score = to_float(row.get("away_score"))
        home_score = to_float(row.get("home_score"))
        line = to_float(row.get("line"))

        if away_score is None or home_score is None:
            return "Unknown"

        if market == "moneyline":
            if away_score == home_score:
                return "Push"

            if side == "home":
                return "Win" if home_score > away_score else "Loss"

            if side == "away":
                return "Win" if away_score > home_score else "Loss"

            return "Unknown"

        if market == "puck_line":
            if line is None:
                return "Unknown"

            if side == "home":
                home_result = to_float(row.get("home_puck_line_result"))
                if home_result is None:
                    home_result = home_score - away_score

                diff = home_result + line

            elif side == "away":
                away_result = to_float(row.get("away_puck_line_result"))
                if away_result is None:
                    away_result = away_score - home_score

                diff = away_result + line

            else:
                return "Unknown"

            if abs(diff) < 1e-9:
                return "Push"

            return "Win" if diff > 0 else "Loss"

        if market == "total":
            if line is None:
                return "Unknown"

            total_score = to_float(row.get("total_score"))
            if total_score is None:
                total_score = away_score + home_score

            diff = total_score - line

            if abs(diff) < 1e-9:
                return "Push"

            if side == "over":
                return "Win" if diff > 0 else "Loss"

            if side == "under":
                return "Win" if diff < 0 else "Loss"

            return "Unknown"

    except Exception as e:
        log_error(f"DETERMINE OUTCOME ERROR | {e} | row={row}")

    return "Unknown"


###############################################################
######################## GRADING ##############################
###############################################################

def grade_rows(bets: pd.DataFrame, scores: pd.DataFrame) -> pd.DataFrame:
    by_game_id, by_team_key = build_score_indexes(scores)

    graded_rows = []
    unmatched_rows = 0

    score_cols = [
        "away_score",
        "home_score",
        "total_score",
        "away_puck_line_result",
        "home_puck_line_result",
        "source_score_file",
    ]

    for _, bet_row in bets.iterrows():
        bet = bet_row.to_dict()
        score = attach_score_to_bet(bet, by_game_id, by_team_key)

        if score is None:
            unmatched_rows += 1
            log_error(
                "NO FINAL SCORE MATCH | "
                f"game_date={bet.get('game_date', '')} | "
                f"game_id={bet.get('game_id', '')} | "
                f"{bet.get('away_team', '')} at {bet.get('home_team', '')} | "
                f"market={bet.get('market_type', '')} | side={bet.get('bet_side', '')}"
            )
            continue

        combined = dict(bet)

        score_game_id = str(score.get("game_id", "")).strip()
        bet_game_id = str(combined.get("game_id", "")).strip()
        if not bet_game_id and score_game_id:
            combined["game_id"] = score_game_id

        for col in score_cols:
            combined[col] = score.get(col, "")

        combined["bet_result"] = determine_outcome(combined)
        graded_rows.append(combined)

    log_summary(
        f"GRADED ROWS BUILT | input_bets={len(bets)} | graded={len(graded_rows)} | unmatched={unmatched_rows}"
    )

    return pd.DataFrame(graded_rows)


def write_outputs(df: pd.DataFrame) -> None:
    if df.empty:
        log_error("NO GRADED ROWS TO WRITE")
        return

    df = df.copy()
    df["game_date"] = df["game_date"].map(normalize_date)

    sort_cols = [
        c for c in [
            "game_date",
            "game_time",
            "game_id",
            "away_team",
            "home_team",
            "market_type",
            "bet_side",
            "line",
        ]
        if c in df.columns
    ]

    if sort_cols:
        df = df.sort_values(sort_cols, kind="mergesort")

    dedupe_cols = [
        c for c in [
            "game_date",
            "game_id",
            "away_team",
            "home_team",
            "market_type",
            "bet_side",
            "line",
            "take_bet",
        ]
        if c in df.columns
    ]

    if dedupe_cols:
        before = len(df)
        df = df.drop_duplicates(subset=dedupe_cols, keep="last")
        after = len(df)
        log_summary(f"DEDUPED GRADED ROWS | before={before} | after={after}")

    for game_date, date_df in df.groupby("game_date", dropna=False):
        out_path = GRADED_DIR / f"{game_date}_results_NHL.csv"
        date_df.to_csv(out_path, index=False)

        counts = date_df["bet_result"].astype(str).value_counts().to_dict()
        log_summary(f"WROTE DAILY GRADED | {out_path} | rows={len(date_df)} | results={counts}")

    df.to_csv(MASTER_FILE, index=False)
    log_summary(f"WROTE MASTER GRADED | {MASTER_FILE} | rows={len(df)}")


###############################################################
######################## MAIN #################################
###############################################################

def main() -> None:
    reset_logs()

    log_summary("START 01_nhl_results_grade.py")
    log_summary(f"SELECT_DIR={SELECT_DIR}")
    log_summary(f"SCORE_DIR={SCORE_DIR}")
    log_summary(f"GRADED_DIR={GRADED_DIR}")

    clean_old_outputs()

    bets = load_select_rows()
    if bets.empty:
        log_error("STOPPING: no select rows")
        print("NHL grading failed: no select rows.")
        return

    scores = load_score_rows()
    if scores.empty:
        log_error("STOPPING: no final score rows")
        print("NHL grading failed: no final score rows.")
        return

    graded = grade_rows(bets, scores)
    write_outputs(graded)

    log_summary("END 01_nhl_results_grade.py")
    print("NHL grading complete.")


if __name__ == "__main__":
    main()