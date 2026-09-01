# scripts/winners_03.py

#!/usr/bin/env python3

import csv
import glob
import math
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

# =========================
# PATHS
# =========================

STEP_02_DIR = Path("docs/win/winners/step_02")
STEP_03_DIR = Path("docs/win/winners/step_02_1")

ERROR_DIR = Path("docs/win/errors/09_winners")
ERROR_LOG = ERROR_DIR / "winners_03.txt"

STEP_03_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# OUTPUT HEADERS (EXACT ORDER)
# =========================
# Note: includes duplicated away_spread/home_spread exactly as provided.
OUTPUT_COLUMNS = [
    "date", "time", "away_team", "home_team", "league", "game_id",
    "home_ml_edge", "away_ml_edge", "away_ml_odds", "home_ml_odds",
    "away_spread", "home_spread", "home_spread_edge", "away_spread_edge",
    "away_spread_odds", "home_spread_odds",
    "over_edge", "under_edge", "over_odds", "under_odds",
    "away_spread", "home_spread", "total", "bet",
]

# =========================
# HELPERS
# =========================

def safe_float(v):
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                return None
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except Exception:
        return None

def extract_date_from_filename(path_str: str) -> str | None:
    # expected: winners_<league>_<market>_YYYY_MM_DD.csv
    stem = Path(path_str).stem
    parts = stem.split("_")
    if len(parts) < 4:
        return None
    d = "_".join(parts[-3:])
    # minimal validation: YYYY_MM_DD
    if len(d) == 10 and d[4] == "_" and d[7] == "_":
        return d
    return None

def build_base_row(src_row: dict) -> dict:
    return {
        "date": src_row.get("date", ""),
        "time": src_row.get("time", ""),
        "away_team": src_row.get("away_team", ""),
        "home_team": src_row.get("home_team", ""),
        "league": src_row.get("league", ""),
        "game_id": src_row.get("game_id", ""),

        "home_ml_edge": "",
        "away_ml_edge": "",
        "away_ml_odds": "",
        "home_ml_odds": "",

        "away_spread": "",
        "home_spread": "",
        "home_spread_edge": "",
        "away_spread_edge": "",
        "away_spread_odds": "",
        "home_spread_odds": "",

        "over_edge": "",
        "under_edge": "",
        "over_odds": "",
        "under_odds": "",

        "total": "",
        "bet": "",
    }

def write_output_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_COLUMNS)
        for r in rows:
            w.writerow([r.get(col, "") for col in OUTPUT_COLUMNS])

# =========================
# CORE PER-DATE PROCESSOR
# =========================

def process_date(date_str: str, files_for_date: list[str], summary_lines: list[str]) -> tuple[int, int]:
    rows_out: list[dict] = []
    files_ok = 0

    for file_path in sorted(files_for_date):
        try:
            df = pd.read_csv(file_path)
            filename = Path(file_path).name

            is_ml = "_ml_" in filename
            is_spreads = "_spreads_" in filename
            is_totals = "_totals_" in filename

            # only these three markets are valid inputs
            if not (is_ml or is_spreads or is_totals):
                continue

            for _, s in df.iterrows():
                src = s.to_dict()
                base = build_base_row(src)

                bets_to_emit: list[str] = []

                if is_ml:
                    home_juice = safe_float(src.get("deci_home_ml_juice_odds"))
                    home_dk = safe_float(src.get("deci_dk_home_odds"))
                    away_juice = safe_float(src.get("deci_away_ml_juice_odds"))
                    away_dk = safe_float(src.get("deci_dk_away_odds"))

                    if home_juice is not None and home_dk is not None and home_juice < home_dk:
                        bets_to_emit.append("home_ml")
                    if away_juice is not None and away_dk is not None and away_juice < away_dk:
                        bets_to_emit.append("away_ml")

                    base["home_ml_edge"] = src.get("home_ml_edge", "")
                    base["away_ml_edge"] = src.get("away_ml_edge", "")
                    base["away_ml_odds"] = src.get("away_ml_odds", "")
                    base["home_ml_odds"] = src.get("home_ml_odds", "")

                elif is_spreads:
                    home_juice = safe_float(src.get("deci_home_spread_juice_odds"))
                    home_dk = safe_float(src.get("deci_dk_home_odds"))
                    away_juice = safe_float(src.get("deci_away_spread_juice_odds"))
                    away_dk = safe_float(src.get("deci_dk_away_odds"))

                    if home_juice is not None and home_dk is not None and home_juice < home_dk:
                        bets_to_emit.append("home_spread")
                    if away_juice is not None and away_dk is not None and away_juice < away_dk:
                        bets_to_emit.append("away_spread")

                    base["away_spread"] = src.get("away_spread", "")
                    base["home_spread"] = src.get("home_spread", "")
                    base["home_spread_edge"] = src.get("home_spread_edge", "")
                    base["away_spread_edge"] = src.get("away_spread_edge", "")
                    base["away_spread_odds"] = src.get("away_spread_odds", "")
                    base["home_spread_odds"] = src.get("home_spread_odds", "")

                elif is_totals:
                    over_juice = safe_float(src.get("deci_over_juice_odds"))
                    over_dk = safe_float(src.get("deci_dk_over_odds"))
                    under_juice = safe_float(src.get("deci_under_juice_odds"))
                    under_dk = safe_float(src.get("deci_dk_under_odds"))

                    if over_juice is not None and over_dk is not None and over_juice < over_dk:
                        bets_to_emit.append("over_bet")
                    if under_juice is not None and under_dk is not None and under_juice < under_dk:
                        bets_to_emit.append("under_bet")

                    base["over_edge"] = src.get("over_edge", "")
                    base["under_edge"] = src.get("under_edge", "")
                    base["over_odds"] = src.get("over_odds", "")
                    base["under_odds"] = src.get("under_odds", "")
                    base["total"] = src.get("total", "")

                for bet in bets_to_emit:
                    out_row = dict(base)
                    out_row["bet"] = bet
                    rows_out.append(out_row)

            files_ok += 1

        except Exception:
            summary_lines.append(f"ERROR processing {file_path}")
            summary_lines.append(traceback.format_exc())

    out_path = STEP_03_DIR / f"winners_{date_str}.csv"
    write_output_csv(out_path, rows_out)
    summary_lines.append(f"Wrote {out_path} | rows={len(rows_out)} | files_ok={files_ok}/{len(files_for_date)}")
    return len(rows_out), files_ok

# =========================
# ENTRY
# =========================

def main():
    summary: list[str] = []
    summary.append(f"=== WINNERS_03 RUN @ {datetime.utcnow().isoformat()}Z ===")

    step2_files = glob.glob(str(STEP_02_DIR / "winners_*_*.csv"))
    if not step2_files:
        summary.append("No input files found in docs/win/winners/step_02")
        with open(ERROR_LOG, "w", encoding="utf-8") as f:
            f.write("\n".join(summary) + "\n")
        return

    by_date: dict[str, list[str]] = defaultdict(list)
    for fp in step2_files:
        d = extract_date_from_filename(fp)
        if d:
            by_date[d].append(fp)

    dates = sorted(by_date.keys())
    summary.append(f"Dates found: {len(dates)}")
    total_rows = 0
    total_files = 0
    total_files_ok = 0

    for d in dates:
        files_for_date = by_date[d]
        summary.append(f"--- DATE {d} | files={len(files_for_date)} ---")
        rows_written, files_ok = process_date(d, files_for_date, summary)
        total_rows += rows_written
        total_files += len(files_for_date)
        total_files_ok += files_ok

    summary.append(f"TOTAL files={total_files} files_ok={total_files_ok} rows_written={total_rows}")

    with open(ERROR_LOG, "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")

if __name__ == "__main__":
    main()
