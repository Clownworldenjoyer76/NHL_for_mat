# docs/win/soccer/scripts/00_parsing/drat.py
#!/usr/bin/env python3

import sys
import re
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "drat_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# ARGS
# =========================

if len(sys.argv) < 3:
    raise ValueError(
        "Usage:\n"
        "  drat.py <league> <market> <raw_text>\n"
        "  drat.py <league> <market> -\n"
        "  drat.py <league> <market> <path.txt>\n"
    )

league_arg = sys.argv[1].strip()
market_input = sys.argv[2].strip()

raw_text = ""

if len(sys.argv) >= 4:
    third = sys.argv[3]

    if third == "-":
        raw_text = sys.stdin.read()
        log("Read raw_text from stdin.")
    else:
        p = Path(third)
        if p.exists() and p.is_file():
            raw_text = p.read_text(encoding="utf-8", errors="replace")
            log(f"Read raw_text from file: {p}")
        else:
            raw_text = " ".join(sys.argv[3:])
            log("Read raw_text from argv fallback.")
else:
    raw_text = sys.stdin.read()

if not raw_text.strip():
    raise ValueError("raw_text is empty.")

# =========================
# MARKET MAP
# =========================

market_map = {
    "MLS": "mls",
    "EPL": "epl",
    "Ligue 1": "ligue1",
    "Serie A": "seriea",
    "La Liga": "laliga",
    "Bundesliga": "bundesliga",
}

market = market_map.get(market_input)
if not market:
    raise ValueError(f"Invalid soccer market: {market_input}")

league = "soccer"

FIELDNAMES = [
    "league","market","match_date","match_time",
    "home_team","away_team",
    "home_prob","draw_prob","away_prob",
    "home_xg","away_xg","expected_total_goals"
]

# =========================
# REGEX
# =========================

RE_DATE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
RE_TIME = re.compile(r"\b\d{1,2}:\d{2}(?:\s*(AM|PM))?\b", re.IGNORECASE)
RE_PCT  = re.compile(r"(\d+(?:\.\d+)?)%")
RE_FLOAT = re.compile(r"\d+(?:\.\d+)?")
RE_ODDS = re.compile(r"[+-]\d+")

# =========================
# HELPERS
# =========================

def clean_team(text: str) -> str:
    return RE_PCT.sub("", text).strip()

def normalize_time(time_str: str) -> str:
    t = time_str.strip().upper()
    try:
        if "AM" in t or "PM" in t:
            dt = datetime.strptime(t, "%I:%M %p")
        else:
            dt = datetime.strptime(t, "%H:%M")
        return dt.strftime("%H:%M")
    except Exception:
        return t

lines = [
    re.sub(r"\s+", " ", l).strip()
    for l in raw_text.splitlines()
    if l.strip()
]

n = len(lines)
rows_by_date = defaultdict(list)

# =========================
# MAIN PARSER
# =========================

for idx, line in enumerate(lines):

    dm = RE_DATE.search(line)
    if not dm:
        continue

    mm, dd, yyyy = dm.groups()
    file_date = f"{yyyy}_{mm.zfill(2)}_{dd.zfill(2)}"

    t_idx = None
    for j in range(idx+1, n):
        if RE_TIME.search(lines[j]):
            t_idx = j
            break

    if t_idx is None or t_idx+2 >= n:
        continue

    match_time = normalize_time(lines[t_idx])

    away_team = clean_team(lines[t_idx+1])
    home_team = clean_team(lines[t_idx+2])

    pct_vals = []

    for k in range(t_idx+1, min(t_idx+15, n)):

        found = RE_PCT.findall(lines[k])

        for v in found:
            pct_vals.append(float(v)/100)

            if len(pct_vals) == 3:
                break

        if len(pct_vals) == 3:
            break

    if len(pct_vals) != 3:
        log(f"Missing probabilities: {home_team} vs {away_team}")
        continue

    away_prob = pct_vals[0]
    home_prob = pct_vals[1]
    draw_prob = pct_vals[2]

    odds_idx = None
    odds_found = 0

    for k in range(t_idx+1, min(t_idx+12, n)):

        if RE_ODDS.match(lines[k]):
            odds_found += 1

            if odds_found == 2:
                odds_idx = k
                break

    if odds_idx is None:
        log(f"ML odds not found: {home_team} vs {away_team}")
        continue

    float_vals = []

    for k in range(odds_idx+1, min(odds_idx+10, n)):

        found = RE_FLOAT.findall(lines[k])

        for v in found:

            float_vals.append(float(v))

            if len(float_vals) == 3:
                break

        if len(float_vals) == 3:
            break

    if len(float_vals) != 3:
        log(f"xG not found: {home_team} vs {away_team}")
        continue

    away_xg = float_vals[0]
    home_xg = float_vals[1]
    expected_total = float_vals[2]

    rows_by_date[file_date].append({
        "league":league,
        "market":market,
        "match_date":file_date,
        "match_time":match_time,
        "home_team":home_team,
        "away_team":away_team,
        "home_prob":f"{home_prob:.6f}",
        "draw_prob":f"{draw_prob:.6f}",
        "away_prob":f"{away_prob:.6f}",
        "home_xg":f"{home_xg:.2f}",
        "away_xg":f"{away_xg:.2f}",
        "expected_total_goals":f"{expected_total:.2f}",
    })

if not rows_by_date:
    raise ValueError("No rows parsed.")

# =========================
# WRITE FILES
# =========================

output_dir = Path("docs/win/soccer/00_intake/predictions")
output_dir.mkdir(parents=True, exist_ok=True)

for d in sorted(rows_by_date.keys()):

    outfile = output_dir / f"soccer_{d}_{market}.csv"

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows_by_date[d])

    print(f"Wrote {outfile} ({len(rows_by_date[d])} rows)")

# =========================
# BUILD COMBINED FILE
# =========================

combined_dir = output_dir / "combined"
combined_dir.mkdir(parents=True, exist_ok=True)

for d in sorted(rows_by_date.keys()):

    combined_file = combined_dir / f"soccer_{d}.csv"

    combined_rows = []

    for f in output_dir.glob(f"soccer_{d}_*.csv"):
        with open(f, newline="", encoding="utf-8") as r:
            reader = csv.DictReader(r)
            for row in reader:
                combined_rows.append(row)

    with open(combined_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(combined_rows)

    print(f"Wrote combined file {combined_file} ({len(combined_rows)} rows)")
