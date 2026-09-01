import os
import csv
import requests
from collections import defaultdict
from datetime import datetime, timezone

API_KEY = os.environ["CLOUD_API"]

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}

BASE_PATH = "docs/win/soccer/00_intake/sportsbook"
os.makedirs(BASE_PATH, exist_ok=True)

LEAGUES = {
    "soccer-england-premier-league": "epl",
    "soccer-spain-laliga": "laliga",
    "soccer-france-ligue-1": "ligue1",
    "soccer-germany-bundesliga": "bundesliga",
    "soccer-italy-serie-a": "seriea",
    "soccer-usa-major-league-soccer": "mls",
}

FIELDNAMES = [
    "sport",
    "league",
    "game_id",
    "match_date",
    "match_time",
    "home_team",
    "away_team",
    "dk_home_decimal",
    "dk_draw_decimal",
    "dk_away_decimal",
    "dk_over25_decimal",
    "dk_under25_decimal",
    "dk_over35_decimal",
    "dk_under35_decimal",
    "btts_yes",
    "btts_no",
]

def get_json(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def parse_time(game):
    raw = game.get("startTime") or game.get("cutoffTime")
    if not raw:
        return None, None, None

    dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)

    match_date = dt.strftime("%Y_%d_%m")   # ✅ FIXED
    file_date  = dt.strftime("%Y_%m_%d")   # ✅ FIXED
    match_time = dt.strftime("%I:%M %p")

    return match_date, file_date, match_time

def interpolate(lines, target):
    if target in lines:
        return lines[target]

    lower = max((k for k in lines if k < target), default=None)
    upper = min((k for k in lines if k > target), default=None)

    if lower is not None and upper is not None:
        return round((lines[lower] + lines[upper]) / 2, 3)

    return None

output_by_file = defaultdict(list)

for league_key, league_name in LEAGUES.items():

    comp = get_json(f"https://sports-api.cloudbet.com/pub/v2/odds/competitions/{league_key}")

    for event in comp.get("events", []):

        if not event.get("home") or not event.get("away"):
            continue

        event_id = event["id"]
        game = get_json(f"https://sports-api.cloudbet.com/pub/v2/odds/events/{event_id}")

        markets = game.get("markets") or {}

        match_date, file_date, match_time = parse_time(game)
        if not match_date:
            continue

        row = {
            "sport": "soccer",
            "league": league_name,
            "game_id": game.get("id"),
            "match_date": match_date,
            "match_time": match_time,
            "home_team": game["home"]["name"],
            "away_team": game["away"]["name"],

            "dk_home_decimal": None,
            "dk_draw_decimal": None,
            "dk_away_decimal": None,

            "dk_over25_decimal": None,
            "dk_under25_decimal": None,
            "dk_over35_decimal": None,
            "dk_under35_decimal": None,

            "btts_yes": None,
            "btts_no": None,
        }

        # MATCH ODDS
        mo = markets.get("soccer.match_odds")
        if mo:
            for sub in mo["submarkets"].values():
                for s in sub["selections"]:
                    if s["outcome"] == "home":
                        row["dk_home_decimal"] = s["price"]
                    elif s["outcome"] == "draw":
                        row["dk_draw_decimal"] = s["price"]
                    elif s["outcome"] == "away":
                        row["dk_away_decimal"] = s["price"]

        # TOTALS (INTERPOLATED)
        over_lines = {}
        under_lines = {}

        tg = markets.get("soccer.total_goals")
        if tg:
            for sub in tg["submarkets"].values():
                for s in sub["selections"]:
                    params = s.get("params", "")
                    if "total=" in params:
                        try:
                            line = float(params.split("=")[1])
                        except:
                            continue

                        if s["outcome"] == "over":
                            over_lines[line] = s["price"]
                        elif s["outcome"] == "under":
                            under_lines[line] = s["price"]

        row["dk_over25_decimal"] = interpolate(over_lines, 2.5)
        row["dk_under25_decimal"] = interpolate(under_lines, 2.5)
        row["dk_over35_decimal"] = interpolate(over_lines, 3.5)
        row["dk_under35_decimal"] = interpolate(under_lines, 3.5)

        # BTTS
        btts = markets.get("soccer.both_teams_to_score")
        if btts:
            for sub in btts["submarkets"].values():
                for s in sub["selections"]:
                    if s["outcome"] == "yes":
                        row["btts_yes"] = s["price"]
                    elif s["outcome"] == "no":
                        row["btts_no"] = s["price"]

        if any([
            row["dk_home_decimal"],
            row["dk_over25_decimal"],
            row["btts_yes"]
        ]):
            output_by_file[file_date].append(row)

# WRITE FILES
for file_date, rows in output_by_file.items():
    path = os.path.join(BASE_PATH, f"{file_date}_soccer.csv")  # ✅ FIXED

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

print("Done")
