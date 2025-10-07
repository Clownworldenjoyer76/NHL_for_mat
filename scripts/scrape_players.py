#!/usr/bin/env python3
import os, sys, time, math, json
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
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError("unreachable")

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

def fetch_rosters_nhlapi():
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
    try:
        df = fetch_rosters_nhlapi()
    except Exception as e:
        print(f"[players] NHL API failed, using sportsipy fallback: {e}")
        df = fetch_rosters_sportsipy_fallback()

    if not df.empty:
        df = df.drop_duplicates(subset=["player_id"]).reset_index(drop=True)
    ensure_outdir(OUT)
    df.to_csv(OUT, index=False)
    print(f"[players] wrote {len(df)} rows to {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
