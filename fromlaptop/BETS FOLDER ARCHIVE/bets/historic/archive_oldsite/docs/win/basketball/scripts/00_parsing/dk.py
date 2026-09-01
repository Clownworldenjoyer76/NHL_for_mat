#!/usr/bin/env python3
# docs/win/basketball/scripts/00_parsing/dk.py

import sys
import re
import csv
from pathlib import Path
from datetime import datetime, timedelta

# ----------------------------
# Logging
# ----------------------------
ERROR_DIR = Path("docs/win/basketball/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "dk_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")


# ----------------------------
# Args
# ----------------------------
if len(sys.argv) < 4:
    log("ERROR: Expected args: league market raw_text_file")
    raise SystemExit(1)

league_input = sys.argv[1].strip()   # from workflow input: "basketball"
market_input = sys.argv[2].strip()   # "NBA" or "NCAA Men"
raw_path = sys.argv[3]

try:
    raw_text = Path(raw_path).read_text(encoding="utf-8", errors="replace")
except Exception as e:
    log(f"ERROR: Failed reading raw_text file '{raw_path}': {e}")
    raise

# Output value requirements
league_out = "Basketball"

market_map = {
    "NBA": "NBA",
    "NCAA Men": "NCAAB",
    "NCAAB": "NCAAB",
}
market_out = market_map.get(market_input)
if not market_out:
    log(f"ERROR: Invalid basketball market input: '{market_input}'")
    raise ValueError("Invalid basketball market")

# ----------------------------
# Helpers
# ----------------------------
MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

RE_HEADER_DATE = re.compile(
    r"\b(?:MON|TUE|WED|THU|FRI|SAT|SUN)?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2})(?:st|nd|rd|th)?\b",
    re.IGNORECASE
)
RE_TODAY = re.compile(r"\bTODAY\b", re.IGNORECASE)
RE_TOMORROW = re.compile(r"\bTOMORROW\b", re.IGNORECASE)
RE_TIME = re.compile(r"\b(\d{1,2}:\d{2})\s*(AM|PM)\b", re.IGNORECASE)
RE_RANK = re.compile(r"^\d{1,3}$")
RE_AMERICAN = re.compile(r"^[+\-]\d+$")
RE_SPREAD = re.compile(r"^[+\-]\d+(?:\.\d+)?$")
RE_TOTAL = re.compile(r"^\d+(?:\.\d+)?$")

def norm_minus(s: str) -> str:
    return s.replace("−", "-").strip()

def clean_team(s: str) -> str:
    return s.replace("-logo", "").strip()

def is_team_candidate(s: str) -> bool:
    if not s:
        return False
    if "-logo" in s:
        return False
    if s.lower() in {"today", "tomorrow", "spread", "total", "moneyline", "more bets", "at", "o", "u"}:
        return False
    if RE_RANK.match(s.strip()):
        return False
    if RE_AMERICAN.match(norm_minus(s)):
        return False
    if RE_SPREAD.match(norm_minus(s)):
        return False
    if RE_TOTAL.match(norm_minus(s)):
        return False
    return True

def extract_game_date(text: str) -> str:
    today = datetime.today()

    for line in text.splitlines():
        line_clean = line.strip()
        if not line_clean:
            continue

        if RE_TODAY.search(line_clean):
            return today.strftime("%Y_%m_%d")
        if RE_TOMORROW.search(line_clean):
            return (today + timedelta(days=1)).strftime("%Y_%m_%d")

        m = RE_HEADER_DATE.search(line_clean.upper())
        if m:
            mon = MONTH_MAP[m.group(1)[:3].upper()]
            day = int(m.group(2))
            dt = datetime(today.year, mon, day)
            return dt.strftime("%Y_%m_%d")

    return today.strftime("%Y_%m_%d")

def extract_game_time(lines: list[str]) -> str:
    for l in lines:
        m = RE_TIME.search(l)
        if m:
            return f"{m.group(1)} {m.group(2).upper()}"
    return ""

# ----------------------------
# Parse
# ----------------------------
FIELDNAMES = [
    "league","market","game_date","game_time",
    "home_team","away_team",
    "away_spread","home_spread","total",
    "away_dk_spread_american","home_dk_spread_american",
    "dk_total_over_american","dk_total_under_american",
    "away_dk_moneyline_american","home_dk_moneyline_american",
]

game_date = extract_game_date(raw_text)

blocks = raw_text.split("More Bets")
rows = []
errors = 0

for block in blocks:
    raw_lines = [l.strip() for l in block.splitlines() if l.strip()]
    if not raw_lines:
        continue

    # Require "at" separator for basketball blocks
    if "at" not in raw_lines:
        continue

    try:
        at_idx = raw_lines.index("at")

        # Away team: nearest valid team candidate above "at"
        away_team = ""
        for i in range(at_idx - 1, -1, -1):
            s = raw_lines[i]
            if is_team_candidate(s):
                away_team = clean_team(s)
                break

        # Home team: nearest valid team candidate below "at"
        home_team = ""
        for i in range(at_idx + 1, len(raw_lines)):
            s = raw_lines[i]
            if is_team_candidate(s):
                home_team = clean_team(s)
                break

        if not away_team or not home_team:
            log(f"ERROR: Could not find teams in block (away='{away_team}', home='{home_team}')")
            errors += 1
            continue

        # Find O/U anchors
        # Use exact "O" and "U" lines as anchors
        o_idx = None
        u_idx = None
        for i, s in enumerate(raw_lines):
            if s == "O" and o_idx is None:
                o_idx = i
            if s == "U" and u_idx is None:
                u_idx = i

        if o_idx is None or u_idx is None:
            log("ERROR: Missing O/U anchors in block")
            errors += 1
            continue

        # Normalize all numeric-ish tokens
        L = [norm_minus(x) for x in raw_lines]

        # Away spread + american: two lines immediately before O
        away_spread = L[o_idx - 2] if o_idx >= 2 else ""
        away_dk_spread_american = L[o_idx - 1] if o_idx >= 1 else ""

        # Total + over american: immediately after O
        total = L[o_idx + 1] if (o_idx + 1) < len(L) else ""
        dk_total_over_american = L[o_idx + 2] if (o_idx + 2) < len(L) else ""

        # Away moneyline: next token after over american
        away_dk_moneyline_american = L[o_idx + 3] if (o_idx + 3) < len(L) else ""

        # Home spread + american: two lines immediately before U
        home_spread = L[u_idx - 2] if u_idx >= 2 else ""
        home_dk_spread_american = L[u_idx - 1] if u_idx >= 1 else ""

        # Under american: immediately after U's total repeat
        dk_total_under_american = L[u_idx + 2] if (u_idx + 2) < len(L) else ""

        # Home moneyline: next token after under american
        home_dk_moneyline_american = L[u_idx + 3] if (u_idx + 3) < len(L) else ""

        # Validate key numeric fields (best-effort)
        def ok_spread(x: str) -> bool:
            return bool(RE_SPREAD.match(x))

        def ok_amer(x: str) -> bool:
            return bool(RE_AMERICAN.match(x))

        def ok_total(x: str) -> bool:
            return bool(RE_TOTAL.match(x))

        if not (ok_spread(away_spread) and ok_spread(home_spread) and ok_total(total)):
            log(f"ERROR: Invalid spread/total values: away_spread='{away_spread}', home_spread='{home_spread}', total='{total}'")
            errors += 1
            continue

        if not (ok_amer(away_dk_spread_american) and ok_amer(home_dk_spread_american) and ok_amer(dk_total_over_american)
                and ok_amer(dk_total_under_american) and ok_amer(away_dk_moneyline_american) and ok_amer(home_dk_moneyline_american)):
            log("ERROR: Invalid american odds in block")
            errors += 1
            continue

        game_time = extract_game_time(raw_lines)

        rows.append({
            "league": league_out,
            "market": market_out,
            "game_date": game_date,
            "game_time": game_time,
            "home_team": home_team,
            "away_team": away_team,
            "away_spread": away_spread,
            "home_spread": home_spread,
            "total": total,
            "away_dk_spread_american": away_dk_spread_american,
            "home_dk_spread_american": home_dk_spread_american,
            "dk_total_over_american": dk_total_over_american,
            "dk_total_under_american": dk_total_under_american,
            "away_dk_moneyline_american": away_dk_moneyline_american,
            "home_dk_moneyline_american": home_dk_moneyline_american,
        })

    except Exception as e:
        log(f"ERROR parsing block: {e}")
        errors += 1

if not rows:
    log("SUMMARY: wrote 0 rows")
    raise SystemExit(0)

# ----------------------------
# Write (upsert)
# ----------------------------
out_dir = Path("docs/win/basketball/00_intake/sportsbook")
out_dir.mkdir(parents=True, exist_ok=True)

outfile = out_dir / f"{league_input}_{market_out}_{game_date}.csv"

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
        log(f"WARNING: Failed reading existing file for dedupe: {e}")

for new_row in rows:
    key = (new_row["game_date"], new_row["market"], new_row["away_team"], new_row["home_team"])
    existing_rows[key] = new_row

temp_file = outfile.with_suffix(".tmp")
with open(temp_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    for r in existing_rows.values():
        writer.writerow({k: r.get(k, "") for k in FIELDNAMES})

temp_file.replace(outfile)

log(f"SUMMARY: upserted {len(rows)} rows, {errors} errors")
print(f"Wrote {outfile} ({len(rows)} rows processed)")
