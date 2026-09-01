import pandas as pd
import glob
from pathlib import Path
import math
from datetime import datetime

# ---------- LOGGING ----------

ERROR_DIR = Path("docs/win/errors/07_juice")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "juice_spreads.log"

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
        raise ValueError("Invalid decimal odds")
    return int(round((d - 1) * 100)) if d >= 2 else int(round(-100 / (d - 1)))

# ---------- SPREAD JUICE LOOKUPS ----------

def band_lookup_spread(spread_abs, fav_ud, venue, jt):
    r = jt[
        (jt["band_min"] <= spread_abs) &
        (spread_abs < jt["band_max"]) &
        (jt["fav_ud"] == fav_ud) &
        (jt["venue"] == venue)
    ]
    return float(r.iloc[0].extra_juice) if not r.empty else 0.0

def exact_lookup_spread(spread, jt):
    r = jt[jt["spread"] == spread]
    if r.empty:
        return 0.0
    val = r.iloc[0].extra_juice
    return 2.0 if not math.isfinite(val) else float(val)

# ---------- MAIN ----------

def normalize_date(val):
    return str(val).replace("-", "_")

def apply_spread_juice(df, jt, league, out_dir):
    global rows_defaulted

    game_date = normalize_date(df["date"].iloc[0])

    def apply_away(row):
        global rows_defaulted
        try:
            spread = row["away_spread"]
            base_dec = american_to_decimal(row["away_spread_acceptable_american_odds"])

            if league == "ncaab":
                juice = exact_lookup_spread(spread, jt)
            else:
                fav_ud = "favorite" if spread < 0 else "underdog"
                juice = band_lookup_spread(abs(spread), fav_ud, "away", jt)

            return decimal_to_american(base_dec * (1 + juice))
        except Exception:
            rows_defaulted += 1
            return row["away_spread_acceptable_american_odds"]

    def apply_home(row):
        global rows_defaulted
        try:
            spread = row["home_spread"]
            base_dec = american_to_decimal(row["home_spread_acceptable_american_odds"])

            if league == "ncaab":
                juice = exact_lookup_spread(spread, jt)
            else:
                fav_ud = "favorite" if spread < 0 else "underdog"
                juice = band_lookup_spread(abs(spread), fav_ud, "home", jt)

            return decimal_to_american(base_dec * (1 + juice))
        except Exception:
            rows_defaulted += 1
            return row["home_spread_acceptable_american_odds"]

    df["away_spread_juice_odds"] = df.apply(apply_away, axis=1)
    df["home_spread_juice_odds"] = df.apply(apply_home, axis=1)

    out = out_dir / f"juice_{league}_spreads_{game_date}.csv"
    df.to_csv(out, index=False)
    log(f"Wrote {out}")

def run():
    global files_scanned, files_written, rows_processed

    LOG_FILE.write_text("", encoding="utf-8")

    log(f"\n=== JUICE SPREADS RUN @ {datetime.utcnow().isoformat()}Z ===")

    CONFIGS = [
        ("nba", "docs/win/nba/spreads/spreads_nba_*.csv", "config/nba/nba_spreads_juice.csv"),
        ("ncaab", "docs/win/ncaab/spreads/spreads_ncaab_*.csv", "config/ncaab/ncaab_spreads_juice.csv"),
        ("nhl", "docs/win/nhl/spreads/spreads_nhl_*.csv", "config/nhl/nhl_spreads_juice.csv"),
    ]

    for league, pattern, juice_file in CONFIGS:
        jt = pd.read_csv(juice_file)
        out_dir = Path(f"docs/win/juice/{league}/spreads")
        out_dir.mkdir(parents=True, exist_ok=True)

        for f in glob.glob(pattern):
            files_scanned += 1
            df = pd.read_csv(f)
            rows_processed += len(df)
            apply_spread_juice(df, jt, league, out_dir)
            files_written += 1

    log(f"Files scanned: {files_scanned}")
    log(f"Files written: {files_written}")
    log(f"Rows processed: {rows_processed}")
    log(f"Rows defaulted: {rows_defaulted}")

if __name__ == "__main__":
    run()
