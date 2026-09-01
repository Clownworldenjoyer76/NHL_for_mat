#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/results_sorted.py

from pathlib import Path
from datetime import datetime
import pandas as pd


# =========================
# PATHS
# =========================

INPUTS = {
    "NBA": Path("docs/win/final_scores/results/nba/graded/NBA_final.csv"),
    "NCAAB": Path("docs/win/final_scores/results/ncaab/graded/NCAAB_final.csv"),
    "NHL": Path("docs/win/final_scores/results/nhl/graded/NHL_final.csv"),
    "SOCCER": Path("docs/win/final_scores/results/soccer/graded/SOCCER_final.csv"),
}

OUTPUTS = {
    "NBA": Path("docs/win/final_scores/results/nba/graded/NBA_final_sorted.csv"),
    "NCAAB": Path("docs/win/final_scores/results/ncaab/graded/ncaab_final_sorted.csv"),
    "NHL": Path("docs/win/final_scores/results/nhl/graded/nhl_final_sorted.csv"),
    "SOCCER": Path("docs/win/final_scores/results/soccer/graded/soccer_final_sorted.csv"),
}

MARKET_TALLY_INPUTS = {
    "NBA": Path("docs/win/final_scores/results/nba/graded/NBA_final_sorted.csv"),
    "NCAAB": Path("docs/win/final_scores/results/ncaab/graded/ncaab_final_sorted.csv"),
    "NHL": Path("docs/win/final_scores/results/nhl/graded/nhl_final_sorted.csv"),
    "SOCCER": Path("docs/win/final_scores/results/soccer/graded/soccer_final_sorted.csv"),
}

MARKET_TALLY_OUTPUTS = {
    "NBA": Path("docs/win/final_scores/results/market_tally_NBA.csv"),
    "NCAAB": Path("docs/win/final_scores/results/market_tally_NCAAB.csv"),
    "NHL": Path("docs/win/final_scores/results/market_tally_NHL.csv"),
    "SOCCER": Path("docs/win/final_scores/results/market_tally_SOCCER.csv"),
}

DEEP_SUMMARY_DIR = Path("docs/win/final_scores/deeper_summaries")

ERROR_LOG = Path("docs/win/final_scores/errors/results_sorted_errors.txt")


# =========================
# LOGGING
# =========================

def log(msg: str) -> None:
    ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


# =========================
# FILE IO
# =========================

def safe_read(path: Path) -> pd.DataFrame:
    try:
        if not path.exists():
            log(f"Missing input file: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df is None or df.empty:
            log(f"Empty input file: {path}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log(f"ERROR reading {path}: {e}")
        return pd.DataFrame()


# =========================
# BASIC CLEANERS
# =========================

def normalize_result(df: pd.DataFrame) -> pd.DataFrame:
    if "bet_result" in df.columns:
        df["bet_result"] = df["bet_result"].astype(str).str.strip().str.title()
    return df


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def to_float(value):
    return pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]


# =========================
# WIN / LOSS SUMMARY
# =========================

def summarize_wl(df: pd.DataFrame):
    if df is None or df.empty or "bet_result" not in df.columns:
        return 0, 0, 0, 0, 0.0

    wins = int((df["bet_result"] == "Win").sum())
    losses = int((df["bet_result"] == "Loss").sum())
    pushes = int((df["bet_result"] == "Push").sum())

    total = wins + losses + pushes
    denom = wins + losses
    win_pct = float(wins / denom) if denom > 0 else 0.0

    return wins, losses, pushes, total, win_pct


def aggregate_results(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    rows = []

    grouped = df.groupby(group_cols, dropna=False)

    for keys, sub in grouped:
        wins, losses, pushes, total, win_pct = summarize_wl(sub)

        if not isinstance(keys, tuple):
            keys = (keys,)

        row = {}
        for i, col in enumerate(group_cols):
            row[col] = keys[i]

        row["Win"] = wins
        row["Loss"] = losses
        row["Push"] = pushes
        row["Total"] = total
        row["Win_Pct"] = round(win_pct, 4)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


# =========================
# CURRENT SUMMARY OUTPUTS
# =========================

def generic_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    rows = []

    if "market_type" not in df.columns:
        log(f"{market_name}: missing market_type column")
        return pd.DataFrame()

    for m in sorted(df["market_type"].dropna().astype(str).unique()):
        sub = df[df["market_type"].astype(str) == m]

        wins, losses, pushes, total, win_pct = summarize_wl(sub)

        rows.append({
            "market": market_name,
            "market_type": m,
            "Win": wins,
            "Loss": losses,
            "Push": pushes,
            "Total": total,
            "Win_Pct": round(win_pct, 4)
        })

    return pd.DataFrame(rows)


def soccer_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for m in ["result", "total"]:
        sub = df[df["market_type"].astype(str) == m] if "market_type" in df.columns else pd.DataFrame()

        wins, losses, pushes, total, win_pct = summarize_wl(sub)

        rows.append({
            "market": "SOCCER",
            "market_type": m,
            "Win": wins,
            "Loss": losses,
            "Push": pushes,
            "Total": total,
            "Win_Pct": round(win_pct, 4)
        })

    return pd.DataFrame(rows)


def build_sorted_output(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    df = normalize_result(df)

    if market_name == "SOCCER":
        return soccer_summary(df)

    return generic_summary(df, market_name)


# =========================
# PICK-LEVEL DERIVATIONS
# =========================

def get_side_group(row) -> str:
    market_name = normalize_text(row.get("market"))
    market_type = normalize_text(row.get("market_type"))

    if market_name == "soccer":
        take_bet = normalize_text(row.get("take_bet"))

        if market_type == "result":
            if take_bet == "home":
                return "Home"
            if take_bet == "away":
                return "Away"
            return "Non_Home_Away"

        return "Non_Home_Away"

    bet_side = normalize_text(row.get("bet_side"))
    take_bet = normalize_text(row.get("take_bet"))
    home_team = normalize_text(row.get("home_team"))
    away_team = normalize_text(row.get("away_team"))

    if bet_side == "home" or take_bet == "home":
        return "Home"

    if bet_side == "away" or take_bet == "away":
        return "Away"

    if bet_side and home_team and bet_side == home_team:
        return "Home"

    if bet_side and away_team and bet_side == away_team:
        return "Away"

    if take_bet and home_team and take_bet == home_team:
        return "Home"

    if take_bet and away_team and take_bet == away_team:
        return "Away"

    return "Non_Home_Away"


def get_total_side(row) -> str:
    market_name = normalize_text(row.get("market"))
    market_type = normalize_text(row.get("market_type"))

    if market_name == "soccer":
        take_bet = normalize_text(row.get("take_bet"))

        if market_type == "total":
            if take_bet == "over25":
                return "Over"
            if take_bet == "under25":
                return "Under"

        return ""

    bet_side = normalize_text(row.get("bet_side"))
    take_bet = normalize_text(row.get("take_bet"))

    if bet_side == "over" or take_bet == "over":
        return "Over"
    if bet_side == "under" or take_bet == "under":
        return "Under"

    return ""


def get_selected_edge(row):
    market_name = normalize_text(row.get("market"))
    market_type = normalize_text(row.get("market_type"))

    if market_name == "soccer":
        return to_float(row.get("edge_pct"))

    side_group = row.get("side_group", "")
    total_side = row.get("total_side", "")

    if market_type == "moneyline":
        if side_group == "Home":
            return to_float(row.get("home_ml_edge_decimal"))
        if side_group == "Away":
            return to_float(row.get("away_ml_edge_decimal"))
        return to_float(row.get("take_bet_edge_pct"))

    if market_type in {"spread", "puck_line"}:
        if side_group == "Home":
            return to_float(row.get("home_spread_edge_decimal"))
        if side_group == "Away":
            return to_float(row.get("away_spread_edge_decimal"))
        return to_float(row.get("take_bet_edge_pct"))

    if market_type == "total":
        if total_side == "Over":
            over_edge = to_float(row.get("over_edge_decimal"))
            if pd.notna(over_edge):
                return over_edge
        if total_side == "Under":
            under_edge = to_float(row.get("under_edge_decimal"))
            if pd.notna(under_edge):
                return under_edge
        return to_float(row.get("take_bet_edge_pct"))

    return pd.NA


def get_selected_american_odds(row):
    market_name = normalize_text(row.get("market"))
    market_type = normalize_text(row.get("market_type"))

    if market_name == "soccer":
        return to_float(row.get("odds_american"))

    side_group = row.get("side_group", "")
    total_side = row.get("total_side", "")

    if market_type == "moneyline":
        if side_group == "Home":
            odds = to_float(row.get("home_dk_moneyline_american"))
            if pd.notna(odds):
                return odds
        if side_group == "Away":
            odds = to_float(row.get("away_dk_moneyline_american"))
            if pd.notna(odds):
                return odds

        take_odds = to_float(row.get("take_odds"))
        if pd.notna(take_odds):
            return take_odds

    if market_type == "spread":
        if side_group == "Home":
            odds = to_float(row.get("home_dk_spread_american"))
            if pd.notna(odds):
                return odds
        if side_group == "Away":
            odds = to_float(row.get("away_dk_spread_american"))
            if pd.notna(odds):
                return odds

        take_odds = to_float(row.get("take_odds"))
        if pd.notna(take_odds):
            return take_odds

    if market_type == "puck_line":
        if side_group == "Home":
            odds = to_float(row.get("dk_home_puck_line"))
            if pd.notna(odds):
                return odds
        if side_group == "Away":
            odds = to_float(row.get("dk_away_puck_line"))
            if pd.notna(odds):
                return odds

        take_odds = to_float(row.get("take_odds"))
        if pd.notna(take_odds):
            return take_odds

    if market_type == "total":
        if total_side == "Over":
            odds = to_float(row.get("dk_total_over_american"))
            if pd.notna(odds):
                return odds
        if total_side == "Under":
            odds = to_float(row.get("dk_total_under_american"))
            if pd.notna(odds):
                return odds

        take_odds = to_float(row.get("take_odds"))
        if pd.notna(take_odds):
            return take_odds

    return pd.NA


def get_selected_spread_like_line(row):
    market_type = normalize_text(row.get("market_type"))
    side_group = row.get("side_group", "")

    if market_type == "spread":
        if side_group == "Home":
            return to_float(row.get("home_spread"))
        if side_group == "Away":
            return to_float(row.get("away_spread"))

    if market_type == "puck_line":
        if side_group == "Home":
            return to_float(row.get("home_puck_line"))
        if side_group == "Away":
            return to_float(row.get("away_puck_line"))

    return pd.NA


def get_favorite_dog_bucket(row) -> str:
    market_name = normalize_text(row.get("market"))
    market_type = normalize_text(row.get("market_type"))

    if market_name == "soccer":
        return ""

    if market_type == "moneyline":
        odds = to_float(row.get("selected_american_odds"))

        if pd.isna(odds):
            return ""

        if odds < 0:
            return "Favorite"
        if odds > 0:
            return "Dog"
        return "Pick"

    if market_type in {"spread", "puck_line"}:
        line_val = to_float(row.get("selected_line_value"))

        if pd.isna(line_val):
            return ""

        if line_val < 0:
            return "Favorite"
        if line_val > 0:
            return "Dog"
        return "Pick"

    return ""


# =========================
# BUCKETS
# =========================

def edge_bucket(value) -> str:
    val = to_float(value)

    if pd.isna(val):
        return ""

    if val < 0:
        return "<0"
    if val < 0.01:
        return "0.00_to_0.0099"
    if val < 0.02:
        return "0.01_to_0.0199"
    if val < 0.03:
        return "0.02_to_0.0299"
    if val < 0.04:
        return "0.03_to_0.0399"
    if val < 0.05:
        return "0.04_to_0.0499"
    if val < 0.06:
        return "0.05_to_0.0599"
    if val < 0.075:
        return "0.06_to_0.0749"
    if val < 0.10:
        return "0.075_to_0.0999"
    return "0.10_plus"


def odds_bucket(value) -> str:
    val = to_float(value)

    if pd.isna(val):
        return ""

    if val <= -200:
        return "minus_200_or_lower"
    if val <= -150:
        return "minus_199_to_minus_150"
    if val <= -125:
        return "minus_149_to_minus_125"
    if val <= -110:
        return "minus_124_to_minus_110"
    if val <= -101:
        return "minus_109_to_minus_101"
    if val <= 100:
        return "minus_100_to_plus_100"
    if val <= 125:
        return "plus_101_to_plus_125"
    if val <= 150:
        return "plus_126_to_plus_150"
    if val <= 200:
        return "plus_151_to_plus_200"
    return "plus_201_or_higher"


# =========================
# ANALYTIC PREP
# =========================

def prepare_pick_level_df(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work = normalize_result(work)

    if "market_type" not in work.columns or "bet_result" not in work.columns:
        log(f"{market_name}: missing required columns for deep summaries")
        return pd.DataFrame()

    work["market"] = market_name
    work["side_group"] = work.apply(get_side_group, axis=1)
    work["total_side"] = work.apply(get_total_side, axis=1)
    work["selected_edge"] = work.apply(get_selected_edge, axis=1)
    work["selected_american_odds"] = work.apply(get_selected_american_odds, axis=1)
    work["selected_line_value"] = work.apply(get_selected_spread_like_line, axis=1)
    work["favorite_dog"] = work.apply(get_favorite_dog_bucket, axis=1)

    work["edge_bucket"] = work["selected_edge"].apply(edge_bucket)
    work["odds_bucket"] = work["selected_american_odds"].apply(odds_bucket)

    return work


# =========================
# DEEP SUMMARY BUILDERS
# =========================

def build_edge_bucket_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    work = prepare_pick_level_df(df, market_name)

    if work.empty:
        return pd.DataFrame()

    work = work[work["edge_bucket"] != ""]

    out = aggregate_results(work, ["market", "market_type", "edge_bucket"])

    if out.empty:
        return out

    return out.sort_values(["market_type", "edge_bucket"]).reset_index(drop=True)


def build_edge_bucket_home_away_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    work = prepare_pick_level_df(df, market_name)

    if work.empty:
        return pd.DataFrame()

    work = work[
        (work["edge_bucket"] != "") &
        (work["side_group"].isin(["Home", "Away"]))
    ]

    out = aggregate_results(work, ["market", "market_type", "side_group", "edge_bucket"])

    if out.empty:
        return out

    return out.sort_values(["market_type", "side_group", "edge_bucket"]).reset_index(drop=True)


def build_odds_bucket_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    work = prepare_pick_level_df(df, market_name)

    if work.empty:
        return pd.DataFrame()

    work = work[work["odds_bucket"] != ""]

    out = aggregate_results(work, ["market", "market_type", "odds_bucket"])

    if out.empty:
        return out

    return out.sort_values(["market_type", "odds_bucket"]).reset_index(drop=True)


def build_odds_bucket_home_away_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    work = prepare_pick_level_df(df, market_name)

    if work.empty:
        return pd.DataFrame()

    work = work[
        (work["odds_bucket"] != "") &
        (work["side_group"].isin(["Home", "Away"]))
    ]

    out = aggregate_results(work, ["market", "market_type", "side_group", "odds_bucket"])

    if out.empty:
        return out

    return out.sort_values(["market_type", "side_group", "odds_bucket"]).reset_index(drop=True)


def build_favorite_dog_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    work = prepare_pick_level_df(df, market_name)

    if work.empty:
        return pd.DataFrame()

    work = work[work["favorite_dog"] != ""]

    out = aggregate_results(work, ["market", "market_type", "favorite_dog"])

    if out.empty:
        return out

    return out.sort_values(["market_type", "favorite_dog"]).reset_index(drop=True)


def build_favorite_dog_home_away_summary(df: pd.DataFrame, market_name: str) -> pd.DataFrame:
    work = prepare_pick_level_df(df, market_name)

    if work.empty:
        return pd.DataFrame()

    work = work[
        (work["favorite_dog"] != "") &
        (work["side_group"].isin(["Home", "Away"]))
    ]

    out = aggregate_results(work, ["market", "market_type", "side_group", "favorite_dog"])

    if out.empty:
        return out

    return out.sort_values(["market_type", "side_group", "favorite_dog"]).reset_index(drop=True)


def write_deep_summaries(df: pd.DataFrame, market_name: str) -> None:
    if df is None or df.empty:
        return

    DEEP_SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

    outputs = {
        f"{market_name}_edge_bucket_summary.csv": build_edge_bucket_summary(df, market_name),
        f"{market_name}_edge_bucket_home_away_summary.csv": build_edge_bucket_home_away_summary(df, market_name),
        f"{market_name}_odds_bucket_summary.csv": build_odds_bucket_summary(df, market_name),
        f"{market_name}_odds_bucket_home_away_summary.csv": build_odds_bucket_home_away_summary(df, market_name),
        f"{market_name}_favorite_dog_summary.csv": build_favorite_dog_summary(df, market_name),
        f"{market_name}_favorite_dog_home_away_summary.csv": build_favorite_dog_home_away_summary(df, market_name),
    }

    for filename, out_df in outputs.items():
        out_path = DEEP_SUMMARY_DIR / filename

        if out_df is None or out_df.empty:
            log(f"{market_name}: deep summary empty -> {out_path}")
            continue

        out_df.to_csv(out_path, index=False)
        log(f"{market_name}: wrote deep summary {out_path}")


# =========================
# MARKET TALLY
# =========================

def create_market_tally_file(market_name: str, in_path: Path, out_path: Path) -> None:
    df = safe_read(in_path)

    if df.empty:
        log(f"{market_name}: tally input missing/empty -> {in_path}")
        return

    required = {"market_type", "Win", "Loss", "Push", "Total", "Win_Pct"}

    if not required.issubset(df.columns):
        log(f"{market_name}: tally input missing required columns -> {in_path}")
        return

    out = df[["market_type", "Win", "Loss", "Push", "Total", "Win_Pct"]].copy()
    out.insert(0, "market", market_name)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    log(f"{market_name}: wrote tally {out_path}")


def create_all_market_tally_files() -> None:
    for market_name, in_path in MARKET_TALLY_INPUTS.items():
        create_market_tally_file(
            market_name,
            in_path,
            MARKET_TALLY_OUTPUTS[market_name]
        )


# =========================
# MAIN
# =========================

def main() -> None:
    ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)

    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("=== results_sorted.py log ===\n")

    for market_name, in_path in INPUTS.items():
        df = safe_read(in_path)

        if df.empty:
            log(f"{market_name}: input missing or empty, skipped")
            continue

        out_df = build_sorted_output(df, market_name)
        out_path = OUTPUTS[market_name]

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_csv(out_path, index=False)

        log(f"{market_name}: wrote sorted file {out_path}")

        write_deep_summaries(df, market_name)

    create_all_market_tally_files()

    print("results_sorted.py complete.")


if __name__ == "__main__":
    main()
