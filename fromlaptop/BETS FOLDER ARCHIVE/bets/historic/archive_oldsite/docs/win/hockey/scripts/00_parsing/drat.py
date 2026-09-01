# docs/win/hockey/scripts/00_parsing/drat.py
#!/usr/bin/env python3

import sys
import re
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# =========================
# LOGGING
# =========================

ERROR_DIR = Path("docs/win/hockey/errors/00_intake")
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
    raise ValueError("Usage: drat.py <league> <market> <dump.txt>")

league_input = sys.argv[1].strip()
market_input = sys.argv[2].strip()

raw_text = ""

if len(sys.argv) >= 4:
    p = Path(sys.argv[3])
    if p.exists() and p.is_file():
        raw_text = p.read_text(encoding="utf-8", errors="replace")
        log(f"Read raw_text from file: {p}")
    else:
        raw_text = " ".join(sys.argv[3:])
        log("Read raw_text from argv fallback.")
else:
    raw_text = sys.stdin.read()
    log("Read raw_text from stdin fallback.")

if not raw_text.strip():
    raise ValueError("raw_text is empty.")

league = "hockey"

market_map = {
    "NHL": "NHL",
}

market = market_map.get(market_input)
if not market:
    raise ValueError(f"Invalid hockey market: {market_input!r}")

FIELDNAMES = [
    "league",
    "market",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_prob",
    "away_prob",
    "away_projected_goals",
    "home_projected_goals",
    "total_projected_goals",
]

# =========================
# REGEX
# =========================

RE_DATE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
RE_TIME = re.compile(r"\b\d{1,2}:\d{2}\s*[AP]M\b", re.IGNORECASE)
RE_PCT  = re.compile(r"(\d+(?:\.\d+)?)%")
RE_FLOAT = re.compile(r"^\d+\.\d+$")

# =========================
# NORMALIZE LINES
# =========================

lines = []
for l in raw_text.splitlines():
    s = l.replace("−", "-").replace("\ufeff", "").strip()
    if s:
        lines.append(s)

n = len(lines)
log(f"lines_count={n}")

# =========================
# PARSE
# =========================

rows_by_date = defaultdict(list)

def clean_team(name: str) -> str:
    # Remove record in parentheses and trailing text
    name = re.sub(r"\(.*?\)", "", name).strip()
    # Remove trailing tab-separated junk if present
    name = name.split("\t")[0].strip()
    return name

i = 0
while i < n:

    dm = RE_DATE.match(lines[i])
    if not dm:
        i += 1
        continue

    mm, dd, yyyy = dm.groups()
    file_date = f"{yyyy}_{mm.zfill(2)}_{dd.zfill(2)}"

    # Expect time next
    if i + 1 >= n or not RE_TIME.search(lines[i + 1]):
        i += 1
        continue

    game_time = lines[i + 1]

    # Teams
    if i + 3 >= n:
        break

    away_team = clean_team(lines[i + 2])
    home_team = clean_team(lines[i + 3])

    # Collect probabilities
    probs = []
    j = i + 4
    while j < n and len(probs) < 2:
        found = RE_PCT.findall(lines[j])
        for v in found:
            probs.append(float(v) / 100.0)
            if len(probs) == 2:
                break
        j += 1

    if len(probs) != 2:
        i += 1
        continue

    away_prob = probs[0]
    home_prob = probs[1]

    # Collect projected goals (first 3 standalone floats after probs)
    floats = []
    while j < n and len(floats) < 3:
        if RE_FLOAT.match(lines[j]):
            floats.append(lines[j])
        else:
            # line may contain multiple floats separated by tabs
            parts = re.findall(r"\d+\.\d+", lines[j])
            for p in parts:
                floats.append(p)
                if len(floats) == 3:
                    break
        j += 1

    if len(floats) != 3:
        i += 1
        continue

    away_proj = floats[0]
    home_proj = floats[1]
    total_proj = floats[2]

    rows_by_date[file_date].append({
        "league": league,
        "market": market,
        "game_date": file_date,
        "game_time": game_time,
        "home_team": home_team,
        "away_team": away_team,
        "home_prob": f"{home_prob:.6f}",
        "away_prob": f"{away_prob:.6f}",
        "away_projected_goals": away_proj,
        "home_projected_goals": home_proj,
        "total_projected_goals": total_proj,
    })

    i = j  # jump to next block

# =========================
# WRITE OUTPUT
# =========================

if not rows_by_date:
    raise ValueError("No rows parsed from raw_text.")

outdir = Path("docs/win/hockey/00_intake/predictions")
outdir.mkdir(parents=True, exist_ok=True)

for d in sorted(rows_by_date.keys()):
    outfile = outdir / f"hockey_{d}.csv"
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows_by_date[d])
    print(f"Wrote {outfile} ({len(rows_by_date[d])} rows)")
