#!/usr/bin/env python3
# docs/win/basketball/scripts/00_parsing/drat.py

import sys
import re
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# =========================
# LOGGING
# =========================

ERROR_DIR = Path("docs/win/basketball/errors/00_intake")
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

if len(sys.argv) < 4:
    log("ERROR: Expected args: drat.py <league> <market> <raw_text_file>")
    raise SystemExit(1)

league_input = sys.argv[1].strip()   # workflow input: "basketball"
market_input = sys.argv[2].strip()   # "NBA" or "NCAA Men"
raw_path = sys.argv[3]

try:
    raw_text = Path(raw_path).read_text(encoding="utf-8", errors="replace")
except Exception as e:
    log(f"ERROR: Failed reading raw_text file '{raw_path}': {e}")
    raise

if not raw_text or not raw_text.strip():
    log("ERROR: raw_text empty after ingestion.")
    raise ValueError("raw_text is empty.")

log(f"raw_text_len={len(raw_text)}")

# =========================
# MARKET MAP
# =========================

market_map = {
    "NBA": "NBA",
    "NCAA Men": "NCAAB",
    "NCAAB": "NCAAB",
}

market_out = market_map.get(market_input)
if not market_out:
    log(f"ERROR: Invalid basketball market input: '{market_input}'")
    raise ValueError(f"Invalid basketball market: {market_input!r}")

league_out = "Basketball"

FIELDNAMES = [
    "league", "market", "game_date", "game_time",
    "home_team", "away_team",
    "home_prob", "away_prob",
    "away_projected_points", "home_projected_points", "total_projected_points",
]

# =========================
# REGEX
# =========================

RE_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
RE_TIME = re.compile(r"^\d{1,2}:\d{2}\s*(AM|PM)$", re.IGNORECASE)
RE_PCT  = re.compile(r"^(\d+(?:\.\d+)?)%$")
RE_FLOAT = re.compile(r"^\d+(?:\.\d+)?$")
RE_TEAM_REC = re.compile(r"^(.*?)\s*\(\d{1,3}-\d{1,3}\)\s*$")

# =========================
# TOKENIZE (split tabs + lines)
# =========================

tokens: list[str] = []
for line in raw_text.replace("\ufeff", "").splitlines():
    parts = [p.strip() for p in line.split("\t")]
    for p in parts:
        if p:
            tokens.append(p)

log(f"tokens_count={len(tokens)}")

# =========================
# HELPERS
# =========================

def team_name_only(s: str) -> str:
    s = s.strip()
    m = RE_TEAM_REC.match(s)
    if m:
        return m.group(1).strip()
    # If record format differs, still strip trailing parentheses if present
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s).strip()
    return s

def pct_to_decimal_str(p: str) -> str:
    m = RE_PCT.match(p.strip())
    if not m:
        return ""
    val = float(m.group(1)) / 100.0
    return f"{val:.6f}"

# =========================
# PARSE
# =========================

rows_by_date = defaultdict(list)
i = 0
n = len(tokens)

while i < n:
    dm = RE_DATE.match(tokens[i])
    if not dm:
        i += 1
        continue

    mm, dd, yyyy = dm.groups()
    game_date = f"{yyyy}_{mm.zfill(2)}_{dd.zfill(2)}"

    # Expect time next (scan forward a bit)
    t_idx = None
    for j in range(i + 1, min(i + 10, n)):
        if RE_TIME.match(tokens[j].strip()):
            t_idx = j
            break
    if t_idx is None:
        log(f"WARNING: No time found after date token at i={i} ({tokens[i]!r})")
        i += 1
        continue

    game_time = tokens[t_idx].strip().upper()

    if t_idx + 3 >= n:
        log(f"WARNING: Not enough tokens after time at t_idx={t_idx}")
        i = t_idx + 1
        continue

    away_team = team_name_only(tokens[t_idx + 1])
    home_team = team_name_only(tokens[t_idx + 2])

    # Next two percentage tokens (away_prob, home_prob)
    away_prob = ""
    home_prob = ""

    # Percent tokens can appear immediately after home team (often tab-separated)
    pct_vals = []
    scan_start = t_idx + 1
    scan_end = min(t_idx + 30, n)

    for k in range(scan_start, scan_end):
        if RE_PCT.match(tokens[k].strip()):
            pct_vals.append(tokens[k].strip())
            if len(pct_vals) == 2:
                break

    if len(pct_vals) == 2:
        away_prob = pct_to_decimal_str(pct_vals[0])
        home_prob = pct_to_decimal_str(pct_vals[1])
    else:
        log(f"WARNING: Missing probabilities for game block date={game_date} time={game_time}")
        i = t_idx + 1
        continue

    # Find next 3 float tokens for projected points: away, home, total
    floats = []
    float_scan_start = t_idx + 1
    float_scan_end = min(t_idx + 80, n)

    for k in range(float_scan_start, float_scan_end):
        tok = tokens[k].strip()
        # ignore percentages
        if RE_PCT.match(tok):
            continue
        # keep pure floats like 119.8 / 231.5
        if RE_FLOAT.match(tok):
            floats.append(tok)
            if len(floats) == 3:
                break

    if len(floats) != 3:
        log(f"WARNING: Missing projected points for game block date={game_date} time={game_time} (found {len(floats)})")
        i = t_idx + 1
        continue

    away_proj = floats[0]
    home_proj = floats[1]
    total_proj = floats[2]

    rows_by_date[game_date].append({
        "league": league_out,
        "market": market_out,
        "game_date": game_date,
        "game_time": game_time,
        "home_team": home_team,
        "away_team": away_team,
        "home_prob": home_prob,
        "away_prob": away_prob,
        "away_projected_points": away_proj,
        "home_projected_points": home_proj,
        "total_projected_points": total_proj,
    })

    # Advance to continue after this block (next date token will restart)
    i = t_idx + 1

total_rows = sum(len(v) for v in rows_by_date.values())
log(f"total_rows={total_rows}")

if total_rows == 0:
    raise ValueError("No rows parsed from raw_text.")

# =========================
# WRITE OUTPUT (upsert per date file)
# =========================

outdir = Path("docs/win/basketball/00_intake/predictions")
outdir.mkdir(parents=True, exist_ok=True)

for d in sorted(rows_by_date.keys()):
    outfile = outdir / f"{league_input}_{market_out}_{d}.csv"

    existing_rows: dict[tuple[str, str, str, str], dict] = {}

    if outfile.exists():
        try:
            with open(outfile, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames == FIELDNAMES:
                    for row in reader:
                        if all(k in row for k in FIELDNAMES):
                            key = (row["game_date"], row["market"], row["away_team"], row["home_team"])
                            existing_rows[key] = row
                else:
                    log("WARNING: Invalid header detected. Rebuilding clean.")
        except Exception as e:
            log(f"WARNING: Failed reading existing file for upsert: {e}")

    for new_row in rows_by_date[d]:
        key = (new_row["game_date"], new_row["market"], new_row["away_team"], new_row["home_team"])
        existing_rows[key] = new_row

    temp_file = outfile.with_suffix(".tmp")
    with open(temp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in existing_rows.values():
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

    temp_file.replace(outfile)
    print(f"Wrote {outfile} ({len(rows_by_date[d])} rows)")
