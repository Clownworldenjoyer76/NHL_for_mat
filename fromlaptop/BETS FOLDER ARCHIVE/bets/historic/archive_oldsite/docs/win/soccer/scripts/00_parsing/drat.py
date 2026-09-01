#!/usr/bin/env python3
# docs/win/soccer/scripts/00_parsing/drat.py

import sys
import re
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# =========================
# LOGGING
# =========================

ERROR_DIR = Path("docs/win/soccer/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "drat_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# ARGS / INPUT
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
            log("Read raw_text from joined argv tokens (fallback).")
else:
    raw_text = sys.stdin.read()
    log("No raw_text arg; read from stdin fallback.")

if not raw_text or not raw_text.strip():
    log("ERROR: raw_text empty after ingestion.")
    raise ValueError("raw_text is empty.")

log(f"raw_text_len={len(raw_text)}")

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
    raise ValueError(f"Invalid soccer market: {market_input!r}")

league = "soccer"

FIELDNAMES = [
    "league", "market", "match_date", "match_time",
    "home_team", "away_team",
    "home_prob", "draw_prob", "away_prob",
]

# =========================
# REGEX
# =========================

RE_DATE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
RE_TIME = re.compile(r"\b\d{1,2}:\d{2}(?:\s*(AM|PM))?\b", re.IGNORECASE)
RE_PCT  = re.compile(r"(\d+(?:\.\d+)?)%")

# =========================
# NORMALIZE LINES
# =========================

lines = []
for l in raw_text.splitlines():
    s = (
        l.replace("−", "-")
         .replace("\ufeff", "")
         .strip()
    )
    if s:
        lines.append(s)

n = len(lines)
log(f"lines_count={n}")

# =========================
# PARSE
# =========================

rows_by_date = defaultdict(list)

def strip_pct(text: str) -> str:
    return RE_PCT.sub("", text).strip()

for idx, line in enumerate(lines):

    dm = RE_DATE.search(line)
    if not dm:
        continue

    mm, dd, yyyy = dm.groups()
    file_date = f"{yyyy}_{mm.zfill(2)}_{dd.zfill(2)}"

    # Find first time AFTER this date
    t_idx = None
    for j in range(idx + 1, n):
        if RE_TIME.search(lines[j]):
            t_idx = j
            break

    if t_idx is None:
        continue

    if t_idx + 2 >= n:
        continue

    match_time = lines[t_idx]

    team_a = strip_pct(lines[t_idx + 1])
    team_b = strip_pct(lines[t_idx + 2])

    if not team_a or not team_b:
        continue

    away_team = team_a
    home_team = team_b

    # Collect next 3 percentages
    pct_vals = []
    for k in range(t_idx + 1, n):
        found = RE_PCT.findall(lines[k])
        for v in found:
            pct_vals.append(float(v) / 100.0)
            if len(pct_vals) == 3:
                break
        if len(pct_vals) == 3:
            break

    if len(pct_vals) != 3:
        continue

    away_prob = pct_vals[0]
    home_prob = pct_vals[1]
    draw_prob = pct_vals[2]

    rows_by_date[file_date].append({
        "league": league,
        "market": market,
        "match_date": file_date,
        "match_time": match_time,
        "home_team": home_team,
        "away_team": away_team,
        "home_prob": f"{home_prob:.6f}",
        "draw_prob": f"{draw_prob:.6f}",
        "away_prob": f"{away_prob:.6f}",
    })

# =========================
# WRITE OUTPUT
# =========================

total_rows = sum(len(v) for v in rows_by_date.values())
log(f"total_rows={total_rows}")

if total_rows == 0:
    raise ValueError("No rows parsed from raw_text.")

outdir = Path("docs/win/soccer/00_intake/predictions")
outdir.mkdir(parents=True, exist_ok=True)

for d in sorted(rows_by_date.keys()):
    outfile = outdir / f"soccer_{d}_{market}.csv"
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows_by_date[d])
    print(f"Wrote {outfile} ({len(rows_by_date[d])} rows)")
