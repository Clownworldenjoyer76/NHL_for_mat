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

OUT = Path("outputs/team_stats.csv")

def fetch_standings_statsapi():
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

def fetch_standings_nhle():
    js = http_get("https://api-web.nhle.com/v1/standings/now").json()
    rows = []
    containers = js.get("standings") or js.get("records") or []
    for cont in containers:
        team_records = cont.get("teamRecords") or cont.get("teamrecords") or []
        for tr in team_records:
            abbr = tr.get("teamAbbrev") or tr.get("teamAbbrevDefault") or (tr.get("team") or {}).get("abbrev") or (tr.get("team") or {}).get("abbreviation")
            wins = tr.get("wins") or (tr.get("leagueRecord") or {}).get("wins")
            losses = tr.get("losses") or (tr.get("leagueRecord") or {}).get("losses")
            ot = tr.get("ot") or tr.get("otLosses") or (tr.get("leagueRecord") or {}).get("ot")
            gf = tr.get("goalsFor") or tr.get("goalsScored")
            ga = tr.get("goalsAgainst")
            rows.append({
                "Team": abbr,
                "Wins": wins,
                "Losses": losses,
                "OT": ot,
                "GF": gf,
                "GA": ga,
            })
    if not rows and isinstance(js, list):
        for tr in js:
            abbr = tr.get("teamAbbrev") or (tr.get("team") or {}).get("abbrev")
            rows.append({
                "Team": abbr,
                "Wins": tr.get("wins"),
                "Losses": tr.get("losses"),
                "OT": tr.get("ot") or tr.get("otLosses"),
                "GF": tr.get("goalsFor"),
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
    df = pd.DataFrame()
    try:
        df = fetch_standings_statsapi()
    except Exception as e:
        print(f"[teams] statsapi failed: {e}")
    if df.empty:
        try:
            df = fetch_standings_nhle()
        except Exception as e:
            print(f"[teams] api-web.nhle.com failed: {e}")
    if df.empty:
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
