#!/usr/bin/env python3
import sys, time, json
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

SESSION = requests.Session()

def http_get(url, params=None, timeout=20):
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 503, 502):
                time.sleep(0.5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            time.sleep(0.5 * (attempt + 1))
    raise last

def ensure_outdir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def current_season_code(today=None):
    d = today or datetime.now(timezone.utc)
    year = d.year
    if d.month >= 8:
        start = year
        end = year + 1
    else:
        start = year - 1
        end = year
    return f"{start}{end}"
OUT = Path("outputs/players.csv")
TEAM_ABBRS = ['ANA', 'ARI', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL', 'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR', 'OTT', 'PHI', 'PIT', 'SEA', 'SJS', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WPG', 'WSH']

def fetch_rosters_statsapi():
    url = "https://statsapi.web.nhl.com/api/v1/teams"
    params = {"expand": "team.roster"}
    data = http_get(url, params=params).json()
    rows = []
    for team in data.get("teams", []):
        team_abbr = team.get("abbreviation") or team.get("name")
        roster = (team.get("roster") or {}).get("roster", [])
        for p in roster:
            person = p.get("person", {}) or {}
            pos = p.get("position", {}) or {}
            rows.append({
                "player_id": person.get("id"),
                "name": person.get("fullName"),
                "team": team_abbr,
                "position": pos.get("abbreviation"),
            })
    return pd.DataFrame(rows)

def fetch_rosters_nhle():
    season = current_season_code()
    rows = []
    for abbr in TEAM_ABBRS:
        try:
            js = http_get(f"https://api-web.nhle.com/v1/roster/{abbr}/{season}").json()
        except Exception:
            continue
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
        time.sleep(0.12)
    return pd.DataFrame(rows)

def fetch_rosters_sportsipy_fallback():
    try:
        from sportsipy.nhl.teams import Teams
        rows = []
        for team in Teams():
            roster = getattr(team, "roster", None)
            if not roster or not getattr(roster, "players", None):
                continue
            for p in roster.players:
                rows.append({
                    "player_id": getattr(p, "player_id", None),
                    "name": getattr(p, "name", None),
                    "team": team.abbreviation,
                    "position": getattr(p, "position", None),
                })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()

def main():
    df = pd.DataFrame()
    try:
        df = fetch_rosters_statsapi()
    except Exception as e:
        print(f"[players] statsapi failed: {e}")
    if df.empty:
        try:
            df = fetch_rosters_nhle()
        except Exception as e:
            print(f"[players] api-web.nhle.com failed: {e}")
    if df.empty:
        df = fetch_rosters_sportsipy_fallback()

    if not df.empty:
        df = df.dropna(subset=["player_id","name"]).drop_duplicates(subset=["player_id"]).reset_index(drop=True)
    ensure_outdir(OUT)
    df.to_csv(OUT, index=False)
    print(f"[players] wrote {len(df)} rows to {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
