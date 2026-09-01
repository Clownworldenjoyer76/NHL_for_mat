#!/usr/bin/env python3
# scripts/dk_04.py
#
# Purpose:
# Update files in docs/win/manual/normalized by copying handle/bets % fields
# from the associated cleaned files in docs/win/manual/cleaned/dk_{league}_{market}_{YYYY_MM_DD}.csv
#
# Market rules:
# - moneyline OR spread: add away/home handle/bets pct columns via team matching
# - totals: add over/under handle/bets pct columns via team + side matching
#
# Notes:
# - Overwrites an error/summary log each run.
# - Overwrites normalized CSVs in-place.

import re
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

# =========================
# PATHS
# =========================

NORMALIZED_DIR = Path("docs/win/manual/normalized")
CLEANED_DIR = Path("docs/win/manual/cleaned")

ERROR_DIR = Path("docs/win/errors/03_dk_iv/")
ERROR_LOG = ERROR_DIR / "dk_04.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

DATE_RE = re.compile(r"(\d{4}_\d{2}_\d{2})")

KNOWN_LEAGUES = {
    "nba",
    "ncaab",
    "nhl",
    "nfl",
    "mlb",
    "wnba",
    "ncaaf",
    "soccer",
    "epl",
}


def norm_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def infer_league_market_date_from_filename(path: Path):
    """
    Attempts to infer league, market, date (YYYY_MM_DD) from a normalized filename.
    This is intentionally tolerant of different naming patterns.

    Market inference:
      - if name contains "moneyline" or "ml" -> moneyline
      - if name contains "spread" or "spreads" -> spread
      - if name contains "total" or "totals" -> totals
    """
    stem = path.stem.lower()

    # date
    m = DATE_RE.search(stem)
    date_str = m.group(1) if m else ""

    # league
    tokens = re.split(r"[^a-z0-9]+", stem)
    league = ""
    for t in tokens:
        if t in KNOWN_LEAGUES:
            league = t
            break

    # market
    market = ""
    if "moneyline" in stem or re.search(r"\bml\b", stem):
        market = "moneyline"
    elif "spreads" in stem or "spread" in stem:
        market = "spread"
    elif "totals" in stem or "total" in stem:
        market = "totals"

    return league, market, date_str


def cleaned_file_candidates(league: str, market: str, date_str: str):
    """
    The user specified cleaned file name pattern:
      dk_{league}_{market}_{YYYY_MM_DD}.csv

    But to be resilient, try a few common variants (spread/spreads, total/totals).
    """
    if not league or not market or not date_str:
        return []

    market_variants = []
    if market == "spread":
        market_variants = ["spread", "spreads"]
    elif market == "totals":
        market_variants = ["totals", "total"]
    elif market == "moneyline":
        market_variants = ["moneyline", "ml"]
    else:
        market_variants = [market]

    cands = []
    for mv in market_variants:
        cands.append(CLEANED_DIR / f"dk_{league}_{mv}_{date_str}.csv")
    return cands


def load_cleaned(league: str, market: str, date_str: str):
    cands = cleaned_file_candidates(league, market, date_str)
    for p in cands:
        if p.exists():
            return p, pd.read_csv(p)
    return None, None


# =========================
# CORE UPDATERS
# =========================

def update_moneyline_or_spread(norm_df: pd.DataFrame, cleaned_df: pd.DataFrame):
    """
    Adds:
      away_handle_pct, home_handle_pct, away_bets_pct, home_bets_pct

    Uses:
      cleaned_df columns: team, handle_pct, bets_pct
      normalized columns: away_team, home_team
    """
    required_norm = ["away_team", "home_team"]
    required_clean = ["team", "handle_pct", "bets_pct"]

    for c in required_norm:
        if c not in norm_df.columns:
            raise ValueError(f"Missing required column in normalized: {c}")
    for c in required_clean:
        if c not in cleaned_df.columns:
            raise ValueError(f"Missing required column in cleaned: {c}")

    # Map by team name (exact match)
    tmp = cleaned_df.copy()
    tmp["team"] = tmp["team"].astype(str)

    team_map_handle = dict(zip(tmp["team"], tmp["handle_pct"]))
    team_map_bets = dict(zip(tmp["team"], tmp["bets_pct"]))

    # Create columns
    norm_df["away_handle_pct"] = ""
    norm_df["home_handle_pct"] = ""
    norm_df["away_bets_pct"] = ""
    norm_df["home_bets_pct"] = ""

    # Fill
    away_vals = norm_df["away_team"].astype(str)
    home_vals = norm_df["home_team"].astype(str)

    norm_df["away_handle_pct"] = away_vals.map(team_map_handle).fillna("")
    norm_df["home_handle_pct"] = home_vals.map(team_map_handle).fillna("")
    norm_df["away_bets_pct"] = away_vals.map(team_map_bets).fillna("")
    norm_df["home_bets_pct"] = home_vals.map(team_map_bets).fillna("")

    # Stats
    matched_away = (norm_df["away_handle_pct"].astype(str) != "").sum()
    matched_home = (norm_df["home_handle_pct"].astype(str) != "").sum()

    return {
        "rows": len(norm_df),
        "matched_away": int(matched_away),
        "matched_home": int(matched_home),
    }


def update_totals(norm_df: pd.DataFrame, cleaned_df: pd.DataFrame):
    """
    Adds:
      over_handle_pct, over_bets_pct, under_handle_pct, under_bets_pct

    Matching (per user requirement):
      Match on normalized 'away_team' to cleaned 'team' AND cleaned 'side' == Over/Under
      Use handle_pct / bets_pct from that cleaned row.

    Assumes cleaned totals file has:
      team, side, handle_pct, bets_pct
    """
    required_norm = ["away_team"]
    required_clean = ["team", "side", "handle_pct", "bets_pct"]

    for c in required_norm:
        if c not in norm_df.columns:
            raise ValueError(f"Missing required column in normalized: {c}")
    for c in required_clean:
        if c not in cleaned_df.columns:
            raise ValueError(f"Missing required column in cleaned: {c}")

    tmp = cleaned_df.copy()
    tmp["team"] = tmp["team"].astype(str)
    tmp["side"] = tmp["side"].astype(str).str.strip().str.lower()

    # Build (team, side) -> value maps
    handle_map = {(r["team"], r["side"]): r["handle_pct"] for _, r in tmp.iterrows()}
    bets_map = {(r["team"], r["side"]): r["bets_pct"] for _, r in tmp.iterrows()}

    # Create columns
    norm_df["over_handle_pct"] = ""
    norm_df["over_bets_pct"] = ""
    norm_df["under_handle_pct"] = ""
    norm_df["under_bets_pct"] = ""

    away_teams = norm_df["away_team"].astype(str)

    # Fill per row
    over_h = []
    over_b = []
    under_h = []
    under_b = []

    for t in away_teams:
        over_h.append(handle_map.get((t, "over"), ""))
        over_b.append(bets_map.get((t, "over"), ""))
        under_h.append(handle_map.get((t, "under"), ""))
        under_b.append(bets_map.get((t, "under"), ""))

    norm_df["over_handle_pct"] = over_h
    norm_df["over_bets_pct"] = over_b
    norm_df["under_handle_pct"] = under_h
    norm_df["under_bets_pct"] = under_b

    matched_over = sum(1 for v in over_h if str(v) != "")
    matched_under = sum(1 for v in under_h if str(v) != "")

    return {
        "rows": len(norm_df),
        "matched_over": int(matched_over),
        "matched_under": int(matched_under),
    }


# =========================
# MAIN
# =========================

def process_files():
    summary = []
    summary.append(f"=== DK_04 RUN @ {datetime.utcnow().isoformat()}Z ===")

    norm_files = sorted(NORMALIZED_DIR.glob("*.csv"))

    if not norm_files:
        summary.append("No normalized files found.")
        ERROR_LOG.write_text("\n".join(summary))
        return

    files_processed = 0
    files_skipped = 0
    files_errors = 0

    for nf in norm_files:
        try:
            league, market, date_str = infer_league_market_date_from_filename(nf)

            if not league or not market or not date_str:
                files_skipped += 1
                summary.append(f"SKIP {nf.name} | unable to infer league/market/date")
                continue

            cleaned_path, cleaned_df = load_cleaned(league, market, date_str)
            if cleaned_df is None:
                files_skipped += 1
                summary.append(f"SKIP {nf.name} | cleaned not found for dk_{league}_{market}_{date_str}.csv")
                continue

            norm_df = pd.read_csv(nf)

            # Decide updater
            if market in ("moneyline", "spread"):
                stats = update_moneyline_or_spread(norm_df, cleaned_df)
                summary.append(
                    f"OK   {nf.name} | cleaned={cleaned_path.name} | rows={stats['rows']} "
                    f"matched_away={stats['matched_away']} matched_home={stats['matched_home']}"
                )
            elif market == "totals":
                stats = update_totals(norm_df, cleaned_df)
                summary.append(
                    f"OK   {nf.name} | cleaned={cleaned_path.name} | rows={stats['rows']} "
                    f"matched_over={stats['matched_over']} matched_under={stats['matched_under']}"
                )
            else:
                files_skipped += 1
                summary.append(f"SKIP {nf.name} | unsupported market={market}")
                continue

            # Write back in-place
            norm_df.to_csv(nf, index=False)
            files_processed += 1

        except Exception as e:
            files_errors += 1
            summary.append(f"ERROR {nf.name}")
            summary.append(str(e))
            summary.append(traceback.format_exc())

    summary.append("")
    summary.append(f"Files processed: {files_processed}")
    summary.append(f"Files skipped:   {files_skipped}")
    summary.append(f"Files errored:   {files_errors}")

    # Overwrite log each run
    ERROR_LOG.write_text("\n".join(summary))


if __name__ == "__main__":
    process_files()
