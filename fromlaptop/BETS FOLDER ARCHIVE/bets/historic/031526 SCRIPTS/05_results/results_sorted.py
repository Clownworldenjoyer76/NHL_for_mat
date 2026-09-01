#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/results_sorted.py

from pathlib import Path
from datetime import datetime
import pandas as pd


# =========================
# PATHS
# =========================

INPUTS = {
    "NHL": Path("docs/win/final_scores/results/nhl/graded/NHL_final.csv"),
    "SOCCER": Path("docs/win/final_scores/results/soccer/graded/SOCCER_final.csv"),
}

OUTPUTS = {
    "NHL": Path("docs/win/final_scores/results/nhl/graded/nhl_final_sorted.csv"),
    "SOCCER": Path("docs/win/final_scores/results/soccer/graded/soccer_final_sorted.csv"),
}

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

    print("results_sorted.py complete.")


if __name__ == "__main__":
    main()
