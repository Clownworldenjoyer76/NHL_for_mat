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

OUT = Path("outputs/team_stats.csv")

def fetch_standings_nhlapi():
    url = "https://statsapi.web.nhl.com/api/v1/standings"
    data = http_get(url).json()
    rows = []
    for rec in data.get("records", []):
        for tr in rec.get("teamRecords", []):
            team = tr.get("team", {}) or {}
            team_abbr = team.get("abbreviation") or team.get("name")
            lr = tr.get("leagueRecord", {}) or {}
            rows.append({
                "Team": team_abbr,
                "Wins": lr.get("wins"),
                "Losses": lr.get("losses"),
                "OT": lr.get("ot"),
                "GF": tr.get("goalsScored"),
                "GA": tr.get("goalsAgainst"),
            })
    return pd.DataFrame(rows)

def fetch_team_sportsipy_fallback():
    try:
        from sportsipy.nhl.teams import Teams
        rows = []
        for t in Teams():
            rows.append({
                "Team": t.abbreviation,
                "Wins": getattr(t, "wins", None),
                "Losses": getattr(t, "losses", None),
                "OT": getattr(t, "ot_losses", None),
                "GF": getattr(t, "goals_scored", None),
                "GA": getattr(t, "goals_against", None),
            })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()

def main():
    try:
        df = fetch_standings_nhlapi()
    except Exception as e:
        print(f"[teams] NHL API failed, using sportsipy fallback: {e}")
        df = fetch_team_sportsipy_fallback()

    for c in ("Wins","Losses","OT","GF","GA"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    ensure_outdir(OUT)
    df.to_csv(OUT, index=False)
    print(f"[teams] wrote {len(df)} rows to {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
