#!/usr/bin/env python3
import sys, time
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

OUT = Path("outputs/players.csv")
TEAM_ABBRS = ['ANA', 'ARI', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL', 'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR', 'OTT', 'PHI', 'PIT', 'SEA', 'SJS', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WPG', 'WSH']
SESSION = requests.Session()

def current_season_code(today=None):
    d = today or datetime.now(timezone.utc)
    y = d.year
    if d.month >= 8:
        a, b = y, y+1
    else:
        a, b = y-1, y
    return f"{a}{b}"

def http_get(url, params=None, timeout=15):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r

def try_roster(abbr, season):
    urls = [
        f"https://api-web.nhle.com/v1/roster/{abbr}/{season}",
        f"https://api-web.nhle.com/v1/roster/{abbr}/current",
    ]
    for u in urls:
        try:
            js = http_get(u).json()
            return js
        except Exception:
            continue
    return None

def main():
    season = current_season_code()
    rows = []
    ok_teams = 0
    for abbr in TEAM_ABBRS:
        js = try_roster(abbr, season)
        if not js:
            continue
        ok_teams += 1
        for grp_key in ("forwards","defensemen","goalies","roster"):
            group = js.get(grp_key, [])
            if not isinstance(group, list):
                continue
            for p in group:
                pid = p.get("playerId") or p.get("id") or (p.get("person") or {}).get("id")
                name = p.get("firstLastName") or p.get("fullName") or (p.get("person") or {}).get("fullName")
                pos = p.get("positionCode") or p.get("positionAbbrev") or (p.get("position") or {}).get("abbreviation")
                rows.append({
                    "player_id": pid,
                    "name": name,
                    "team": abbr,
                    "position": pos,
                })
        time.sleep(0.08)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.dropna(subset=["player_id","name"])
        df = df.drop_duplicates(subset=["player_id"]).reset_index(drop=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"[players] teams_ok=" + str(ok_teams) + " rows=" + str(len(df)) + " -> " + str(OUT))
    return 0

if __name__ == "__main__":
    sys.exit(main())
