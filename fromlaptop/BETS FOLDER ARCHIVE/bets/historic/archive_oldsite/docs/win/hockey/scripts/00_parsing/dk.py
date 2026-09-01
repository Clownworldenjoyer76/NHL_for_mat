# docs/win/hockey/scripts/00_parsing/dk.py
#!/usr/bin/env python3

import sys
import re
import csv
from pathlib import Path
from datetime import datetime, timedelta

# =========================
# PATHS / LOGGING
# =========================

ERROR_DIR = Path("docs/win/hockey/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "dk_log.txt"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write("")

def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} | {msg}\n")

# =========================
# ARGS
# =========================

if len(sys.argv) < 4:
    raise SystemExit("Usage: dk.py <league> <market> <dump.txt>")

league_input = sys.argv[1].strip()
market_input = sys.argv[2].strip()
dump_path = Path(sys.argv[3])

if not dump_path.exists():
    raise FileNotFoundError(f"dump file not found: {dump_path}")

raw_text = dump_path.read_text(encoding="utf-8", errors="replace")

league = "hockey"

market_map = {
    "NHL": "NHL",
}

market = market_map.get(market_input)
if not market:
    raise ValueError(f"Invalid hockey market: {market_input}")

FIELDNAMES = [
    "league",
    "market",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "away_puck_line",
    "home_puck_line",
    "total",
    "away_dk_puck_line_american",
    "home_dk_puck_line_american",
    "dk_total_over_american",
    "dk_total_under_american",
    "away_dk_moneyline_american",
    "home_dk_moneyline_american",
]

# =========================
# HELPERS
# =========================

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

RE_DATE_HEADER = re.compile(
    r"^(?:MON|TUE|WED|THU|FRI|SAT|SUN)\s+"
    r"(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2})",
    re.IGNORECASE
)

RE_TIME = re.compile(r"\b(\d{1,2}:\d{2}\s*[AP]M)\b", re.IGNORECASE)

RE_ODDS = re.compile(r"^[+\-−]\d+$")
RE_SPREAD = re.compile(r"^[+\-]\d+(?:\.\d+)?$")
RE_TOTAL_NUM = re.compile(r"^\d+(?:\.\d+)?$")

def clean_line(s: str) -> str:
    return s.strip()

def is_logo_line(s: str) -> bool:
    return s.lower().endswith("-logo")

def normalize_odds(s: str) -> str:
    return s.replace("−", "-").strip()

# =========================
# GLOBAL DATE PARSE (ONCE)
# =========================

def parse_global_game_date(text: str) -> str:
    today = datetime.today()
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    for raw in lines:
        u = raw.upper()

        if "TODAY" in u:
            return today.strftime("%Y_%m_%d")

        if "TOMORROW" in u:
            return (today + timedelta(days=1)).strftime("%Y_%m_%d")

        m = RE_DATE_HEADER.match(u)
        if m:
            mon = MONTH_MAP[m.group(1)[:3].upper()]
            day = int(m.group(2))
            dt = datetime(today.year, mon, day)
            return dt.strftime("%Y_%m_%d")

    # fallback
    return today.strftime("%Y_%m_%d")

GLOBAL_GAME_DATE = parse_global_game_date(raw_text)

# =========================
# EXTRACTION FUNCTIONS
# =========================

def extract_time(lines: list[str]) -> str:
    for raw in lines:
        m = RE_TIME.search(raw)
        if m:
            return m.group(1).upper().replace("  ", " ").strip()
    return ""

def extract_teams(lines: list[str]) -> tuple[str, str]:
    cleaned = [clean_line(x) for x in lines if clean_line(x)]

    try:
        at_i = next(i for i, x in enumerate(cleaned) if x.lower() == "at")
    except StopIteration:
        raise ValueError("Missing 'at' delimiter")

    away_team = ""
    for j in range(at_i - 1, -1, -1):
        if not is_logo_line(cleaned[j]) and cleaned[j].lower() not in ("puck line", "total", "moneyline"):
            away_team = cleaned[j]
            break

    home_team = ""
    for j in range(at_i + 1, len(cleaned)):
        if not is_logo_line(cleaned[j]) and cleaned[j].lower() not in ("puck line", "total", "moneyline"):
            home_team = cleaned[j]
            break

    if not away_team or not home_team:
        raise ValueError("Could not determine teams")

    return away_team, home_team

def parse_numbers(lines: list[str]) -> dict:
    cleaned = [clean_line(x) for x in lines if clean_line(x)]
    tokens = []

    for x in cleaned:
        xl = x.strip()
        if xl.upper() in ("O", "U"):
            tokens.append(xl.upper())
            continue
        if RE_SPREAD.match(xl):
            tokens.append(xl)
            continue
        if RE_TOTAL_NUM.match(xl):
            tokens.append(xl)
            continue
        if RE_ODDS.match(normalize_odds(xl)):
            tokens.append(normalize_odds(xl))
            continue

    out = {
        "away_puck_line": "",
        "home_puck_line": "",
        "total": "",
        "away_dk_puck_line_american": "",
        "home_dk_puck_line_american": "",
        "dk_total_over_american": "",
        "dk_total_under_american": "",
        "away_dk_moneyline_american": "",
        "home_dk_moneyline_american": "",
    }

    i = 0

    if i < len(tokens) and RE_SPREAD.match(tokens[i]):
        out["away_puck_line"] = tokens[i]; i += 1
    if i < len(tokens) and RE_ODDS.match(tokens[i]):
        out["away_dk_puck_line_american"] = tokens[i]; i += 1

    if i < len(tokens) and tokens[i] == "O":
        i += 1
    if i < len(tokens) and RE_TOTAL_NUM.match(tokens[i]):
        out["total"] = tokens[i]; i += 1
    if i < len(tokens) and RE_ODDS.match(tokens[i]):
        out["dk_total_over_american"] = tokens[i]; i += 1

    if i < len(tokens) and RE_ODDS.match(tokens[i]):
        out["away_dk_moneyline_american"] = tokens[i]; i += 1

    if i < len(tokens) and RE_SPREAD.match(tokens[i]):
        out["home_puck_line"] = tokens[i]; i += 1
    if i < len(tokens) and RE_ODDS.match(tokens[i]):
        out["home_dk_puck_line_american"] = tokens[i]; i += 1

    if i < len(tokens) and tokens[i] == "U":
        i += 1
    if i < len(tokens) and RE_TOTAL_NUM.match(tokens[i]):
        i += 1
    if i < len(tokens) and RE_ODDS.match(tokens[i]):
        out["dk_total_under_american"] = tokens[i]; i += 1

    if i < len(tokens) and RE_ODDS.match(tokens[i]):
        out["home_dk_moneyline_american"] = tokens[i]; i += 1

    return out

# =========================
# PARSE BLOCKS
# =========================

blocks = raw_text.split("More Bets")
rows = []
errors = 0

for block in blocks:
    lines = [clean_line(l) for l in block.splitlines() if clean_line(l)]
    if not lines:
        continue

    if not any(l.lower() == "at" for l in lines):
        continue

    try:
        game_time = extract_time(lines)
        away_team, home_team = extract_teams(lines)
        nums = parse_numbers(lines)

        rows.append({
            "league": league,
            "market": market,
            "game_date": GLOBAL_GAME_DATE,
            "game_time": game_time,
            "home_team": home_team,
            "away_team": away_team,
            **nums,
        })

    except Exception as e:
        log(f"ERROR parsing block: {e}")
        errors += 1

if not rows:
    log("SUMMARY: wrote 0 rows")
    sys.exit()

file_date = GLOBAL_GAME_DATE

output_dir = Path("docs/win/hockey/00_intake/sportsbook")
output_dir.mkdir(parents=True, exist_ok=True)
outfile = output_dir / f"hockey_{file_date}.csv"

# =========================
# LOAD + SELF-DEDUP
# =========================

existing_rows = {}

if outfile.exists():
    with open(outfile, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames == FIELDNAMES:
            for row in reader:
                key = (row["game_date"], row["market"], row["home_team"], row["away_team"])
                existing_rows[key] = row
        else:
            log("WARNING: Invalid header detected. Rebuilding clean.")

for new_row in rows:
    key = (new_row["game_date"], new_row["market"], new_row["home_team"], new_row["away_team"])
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
