# docs/win/soccer/scripts/00_parsing/dk.py
#!/usr/bin/env python3

import sys
import re
import csv
from pathlib import Path
from datetime import datetime

ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "dk_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

league_input = sys.argv[1].strip()
market_input = sys.argv[2].strip()
raw_input = sys.argv[3]

# Read dump.txt if provided
p = Path(raw_input)
if p.exists() and p.is_file():
    raw_text = p.read_text(encoding="utf-8", errors="replace")
else:
    raw_text = raw_input

league = "soccer"

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
    raise ValueError("Invalid soccer market")

FIELDNAMES = [
    "league","market","match_date","match_time",
    "home_team","away_team",
    "dk_home_american","dk_draw_american","dk_away_american",
]

MONTH_MAP = {
    "JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
    "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12,
}

RE_HEADER_DATE = re.compile(
    r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2})(?:st|nd|rd|th)?",
    re.IGNORECASE
)

today = datetime.today()
match_date_dt = None

for line in raw_text.splitlines():
    line_clean = line.strip().upper()
    if "TODAY" in line_clean:
        match_date_dt = today
        break
    m = RE_HEADER_DATE.search(line_clean)
    if m:
        month = MONTH_MAP[m.group(1)[:3].upper()]
        day = int(m.group(2))
        match_date_dt = datetime(today.year,month,day)
        break

if not match_date_dt:
    match_date_dt = today

match_date = match_date_dt.strftime("%Y_%m_%d")

def clean_team(name):
    return name.replace("-logo","").strip()

def normalize_odds(o):
    return o.replace("−","-").strip()

def clean_time(t):
    t = re.sub(r"(?i)^today\s*", "", t).strip()
    return t

blocks = raw_text.split("More Bets")
rows = []
errors = 0

for block in blocks:
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    if "vs" not in lines:
        continue
    try:
        vs_index = lines.index("vs")
        home_team = clean_team(lines[vs_index-1])
        away_team = clean_team(lines[vs_index+2])

        odds = [normalize_odds(x) for x in lines if re.match(r"[+\-−]\d+",x)]
        if len(odds)!=3:
            log(f"ERROR: Expected 3 odds but found {len(odds)}")
            errors+=1
            continue

        match_time=""
        for l in lines:
            if "AM" in l or "PM" in l:
                match_time = clean_time(l)
                break

        rows.append({
            "league":league,
            "market":market,
            "match_date":match_date,
            "match_time":match_time,
            "home_team":home_team,
            "away_team":away_team,
            "dk_home_american":odds[0],
            "dk_draw_american":odds[1],
            "dk_away_american":odds[2],
        })

    except Exception as e:
        log(f"ERROR parsing block: {str(e)}")
        errors+=1

print("PARSED ROWS:")
for r in rows:
    print(r)

if not rows:
    log("SUMMARY: wrote 0 rows")
    sys.exit()

# -----------------------------
# WRITE MARKET FILE
# -----------------------------

output_dir = Path("docs/win/soccer/00_intake/sportsbook")
output_dir.mkdir(parents=True,exist_ok=True)

outfile = output_dir / f"soccer_{match_date}_{market}.csv"

with open(outfile,"w",newline="",encoding="utf-8") as f:
    writer=csv.DictWriter(f,fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {outfile} ({len(rows)} rows)")

# -----------------------------
# BUILD COMBINED FILE
# -----------------------------

combined_dir = output_dir / "combined"
combined_dir.mkdir(parents=True,exist_ok=True)

combined_file = combined_dir / f"soccer_{match_date}.csv"

combined_rows = []

for f in output_dir.glob(f"soccer_{match_date}_*.csv"):
    with open(f,newline="",encoding="utf-8") as r:
        reader = csv.DictReader(r)
        for row in reader:
            combined_rows.append(row)

with open(combined_file,"w",newline="",encoding="utf-8") as f:
    writer = csv.DictWriter(f,fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(combined_rows)

print(f"Wrote combined file {combined_file} ({len(combined_rows)} rows)")

log(f"SUMMARY: wrote {len(rows)} rows, {errors} errors")
