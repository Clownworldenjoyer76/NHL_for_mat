#!/usr/bin/env python3

import csv
from pathlib import Path
import traceback
import pandas as pd
from datetime import datetime

# =========================
# PATHS
# =========================

INPUT_DIR = Path("docs/win/manual/cleaned")
OUTPUT_DIR = Path("docs/win/manual/normalized")
GAMES_MASTER_DIR = Path("docs/win/games_master")

ERROR_DIR = Path("docs/win/errors/03_dk_iv")
ERROR_LOG = ERROR_DIR / "dk_03.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def log(msg: str):
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def norm(s: str) -> str:
    return " ".join(str(s).split()) if s else ""

def load_games_master():
    files = list(GAMES_MASTER_DIR.glob("games_*.csv"))
    if not files:
        raise RuntimeError("No games_master files found")

    return pd.concat(
        [pd.read_csv(f, dtype=str) for f in files],
        ignore_index=True
    )

# =========================
# CORE
# =========================

def process_file(path: Path, gm_df: pd.DataFrame, date_summary: dict):
    try:
        df = pd.read_csv(path, dtype=str)
        if df.empty:
            return

        parts = path.stem.split("_")
        if len(parts) < 6:
            return

        _, league, market, year, month, day = parts
        base_league = league
        date = f"{year}_{month}_{day}"

        gm_slice = gm_df[
            (gm_df["league"].str.startswith(base_league)) &
            (gm_df["date"] == date)
        ]

        if gm_slice.empty:
            return

        df["team_norm"] = df["team"].apply(norm)
        df["opponent_norm"] = df["opponent"].apply(norm)

        if date not in date_summary:
            date_summary[date] = {
                "matched": set(),
                "unmatched": set(),
                "master_ids": set(gm_slice["game_id"]),
                "master_games": {
                    row["game_id"]: (
                        norm(row["away_team"]),
                        norm(row["home_team"])
                    )
                    for _, row in gm_slice.iterrows()
                }
            }

        summary = date_summary[date]

        out_rows = []

        for _, gm in gm_slice.iterrows():
            gid = gm["game_id"]
            away = norm(gm["away_team"])
            home = norm(gm["home_team"])

            matchup_rows = df[
                (
                    (df["team_norm"] == away) &
                    (df["opponent_norm"] == home)
                ) |
                (
                    (df["team_norm"] == home) &
                    (df["opponent_norm"] == away)
                )
            ]

            if matchup_rows.empty:
                summary["unmatched"].add(gid)
                continue

            summary["matched"].add(gid)

            # (Original row-building logic preserved)
            # Only building minimal output as before

            if market == "totals":
                over_rows = matchup_rows[
                    matchup_rows["side"].str.lower() == "over"
                ]
                under_rows = matchup_rows[
                    matchup_rows["side"].str.lower() == "under"
                ]
                if over_rows.empty or under_rows.empty:
                    continue

                over = over_rows.iloc[0]
                under = under_rows.iloc[0]

                out_rows.append({
                    "date": over["date"],
                    "time": over["time"],
                    "league": over["league"],
                    "game_id": gid,
                    "away_team": away,
                    "home_team": home,
                    "total": over["total"],
                    "over_odds": over["odds"],
                    "under_odds": under["odds"],
                    "over_decimal_odds": over["decimal_odds"],
                    "under_decimal_odds": under["decimal_odds"],
                })
            else:
                if len(matchup_rows) != 2:
                    continue

                away_row = matchup_rows[
                    matchup_rows["team_norm"] == away
                ]
                home_row = matchup_rows[
                    matchup_rows["team_norm"] == home
                ]

                if len(away_row) != 1 or len(home_row) != 1:
                    continue

                away_row = away_row.iloc[0]
                home_row = home_row.iloc[0]

                out = {
                    "date": away_row["date"],
                    "time": away_row["time"],
                    "league": away_row["league"],
                    "game_id": gid,
                    "away_team": away,
                    "home_team": home,
                }

                out_rows.append(out)

        out_path = OUTPUT_DIR / path.name
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            if out_rows:
                writer = csv.DictWriter(f, fieldnames=out_rows[0].keys())
                writer.writeheader()
                writer.writerows(out_rows)
            else:
                f.write("")

    except Exception as e:
        log(f"FILE ERROR: {path.name}")
        log(str(e))
        log(traceback.format_exc())
        log("-" * 80)

# =========================
# MAIN
# =========================

def main():
    ERROR_LOG.write_text("", encoding="utf-8")
    log("DK_03 START")
    log(f"Run timestamp (UTC): {datetime.utcnow().isoformat()}")

    gm_df = load_games_master()
    date_summary = {}

    for path in INPUT_DIR.glob("dk_*_*.csv"):
        process_file(path, gm_df, date_summary)

    for date in sorted(date_summary.keys()):
        summary = date_summary[date]

        matched_count = len(summary["matched"])
        unmatched_ids = summary["master_ids"] - summary["matched"]

        log("—————————————")
        log(date)
        log(f"Matched Games: {matched_count}")
        log(f"Unmatched Games {len(unmatched_ids)}")
        log("")

        if unmatched_ids:
            log("MASTER NOT IN DK:")
            for gid in sorted(unmatched_ids):
                away, home = summary["master_games"][gid]
                log(f"{away} vs {home}")

        log("—————————————")

    log("DK_03 END")

if __name__ == "__main__":
    main()
