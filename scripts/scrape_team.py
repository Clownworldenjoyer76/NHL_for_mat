#!/usr/bin/env python3
import sys, time
from pathlib import Path
import requests
import pandas as pd

OUT = Path("outputs/team_stats.csv")

SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nhl.com/",
}

def ensure_outdir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def http_get(url, params=None, timeout=20):
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code in (429, 503, 502):
                time.sleep(0.5 * (attempt + 1))
                continue
            r.raise_for_status()
            if not r.content:
                raise requests.RequestException("Empty body")
            return r
        except requests.RequestException as e:
            last = e
            time.sleep(0.5 * (attempt + 1))
    raise last

def parse_many_shapes(js):
    rows = []
    containers = js.get("standings") or js.get("records") or js.get("teamRecords") or []
    if isinstance(containers, dict):
        containers = [containers]

    def add_row(tr):
        team_fields = tr.get("team") or {}
        abbr = tr.get("teamAbbrev") or tr.get("teamAbbrevDefault") or team_fields.get("abbrev") or team_fields.get("abbreviation")
        wins = tr.get("wins") or (tr.get("leagueRecord") or {}).get("wins")
        losses = tr.get("losses") or (tr.get("leagueRecord") or {}).get("losses")
        ot = tr.get("ot") or tr.get("otLosses") or (tr.get("leagueRecord") or {}).get("ot")
        gf = tr.get("goalsFor") or tr.get("goalsScored") or tr.get("gf")
        ga = tr.get("goalsAgainst") or tr.get("ga")
        if abbr:
            rows.append({"Team": abbr, "Wins": wins, "Losses": losses, "OT": ot, "GF": gf, "GA": ga})

    for cont in containers:
        team_records = cont.get("teamRecords") or cont.get("teamrecords") or cont.get("teams") or []
        for tr in team_records:
            add_row(tr)

    if not rows and isinstance(js, list):
        for tr in js:
            add_row(tr)

    return pd.DataFrame(rows)

def fetch_standings_statsapi():
    # Fixed mirror domain (no .web.)
    url = "https://statsapi.nhl.com/api/v1/standings"
    return parse_many_shapes(http_get(url).json())

def fetch_standings_nhle():
    url = "https://api-web.nhle.com/v1/standings/now"
    return parse_many_shapes(http_get(url).json())

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
