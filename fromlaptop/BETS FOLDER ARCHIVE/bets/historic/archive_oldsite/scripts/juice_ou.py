# scripts/juice_ou.py

import pandas as pd
import glob
from pathlib import Path
import math
from datetime import datetime

# ---------- LOGGING ----------

ERROR_DIR = Path("docs/win/errors/07_juice")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "juice_totals.log"

files_scanned = 0
files_written = 0
rows_processed = 0
rows_defaulted = 0

def log(msg):
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ---------- ODDS HELPERS ----------

def american_to_decimal(a):
    return 1 + (a / 100 if a > 0 else 100 / abs(a))

def decimal_to_american(d):
    if not math.isfinite(d) or d <= 1:
        return ""
    return int(round((d - 1) * 100)) if d >= 2 else int(round(-100 / (d - 1)))

# ---------- JUICE LOOKUPS ----------

def band_lookup_total(total, side, jt):
    r = jt[
        (jt.band_min <= total) &
        (total < jt.band_max) &
        (jt.side == side)
    ]
    return float(r.iloc[0].extra_juice) if not r.empty else 0.0

def exact_lookup_total(total, side, jt):
    r = jt[
        (jt.over_under == total) &
        (jt.side == side)
    ]
    return float(r.iloc[0].extra_juice) if not r.empty else 0.0

# ---------- MAIN ----------

def normalize_date(val):
    return str(val).replace("-", "_")

def load_juice_table(path):
    jt = pd.read_csv(path)
    jt["extra_juice"] = jt["extra_juice"].replace([math.inf, -math.inf], 2.0)
    return jt

def run():
    global files_scanned, files_written, rows_processed, rows_defaulted

    LOG_FILE.write_text("", encoding="utf-8")
    
    log(f"\n=== JUICE TOTALS RUN @ {datetime.utcnow().isoformat()}Z ===")

    JOBS = [
        (
            "nba",
            "docs/win/nba/totals/totals_nba_*.csv",
            "config/nba/nba_totals_juice.csv",
            "band",
        ),
        (
            "ncaab",
            "docs/win/ncaab/totals/totals_ncaab_*.csv",
            "config/ncaab/ncaab_totals_juice.csv",
            "exact",
        ),
        (
            "nhl",
            "docs/win/nhl/totals/totals_nhl_*.csv",
            "config/nhl/nhl_totals_juice.csv",
            "band",
        ),
    ]

    for league, pattern, juice_file, mode in JOBS:
        if not Path(juice_file).exists():
            continue

        jt = load_juice_table(juice_file)

        out_dir = Path(f"docs/win/juice/{league}/totals")
        out_dir.mkdir(parents=True, exist_ok=True)

        for f in glob.glob(pattern):
            files_scanned += 1
            df = pd.read_csv(f)
            rows_processed += len(df)

            game_date = normalize_date(df["date"].iloc[0])

            def apply_over(row):
                global rows_defaulted
                try:
                    base_dec = american_to_decimal(row["over_acceptable_american_odds"])
                    juice = (
                        band_lookup_total(row["total"], "over", jt)
                        if mode == "band"
                        else exact_lookup_total(row["total"], "over", jt)
                    )
                    return decimal_to_american(base_dec * (1 + juice))
                except Exception:
                    rows_defaulted += 1
                    return ""

            def apply_under(row):
                global rows_defaulted
                try:
                    base_dec = american_to_decimal(row["under_acceptable_american_odds"])
                    juice = (
                        band_lookup_total(row["total"], "under", jt)
                        if mode == "band"
                        else exact_lookup_total(row["total"], "under", jt)
                    )
                    return decimal_to_american(base_dec * (1 + juice))
                except Exception:
                    rows_defaulted += 1
                    return ""

            df["over_juice_odds"] = df.apply(apply_over, axis=1)
            df["under_juice_odds"] = df.apply(apply_under, axis=1)

            out = out_dir / f"juice_{league}_totals_{game_date}.csv"
            df.to_csv(out, index=False)

            files_written += 1
            log(f"Wrote {out}")

    log(f"Files scanned: {files_scanned}")
    log(f"Files written: {files_written}")
    log(f"Rows processed: {rows_processed}")
    log(f"Rows defaulted: {rows_defaulted}")

if __name__ == "__main__":
    run()
