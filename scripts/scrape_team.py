#!/usr/bin/env python3
import sys, time
from pathlib import Path
import requests
import pandas as pd

OUT = Path("outputs/team_stats.csv")

SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nhl.com/",
}

def ensure_outdir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def http_get(url, params=None, timeout=20, allow_empty=False):
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code in (429, 503, 502):
                time.sleep(0.5 * (attempt + 1))
                continue
            r.raise_for_status()
            if not allow_empty and not r.content:
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
        abbr = (
            tr.get("teamAbbrev") or tr.get("teamAbbrevDefault")
            or team_fields.get("abbrev") or team_fields.get("abbreviation")
        )
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
    url = "https://statsapi.nhl.com/api/v1/standings"
    return parse_many_shapes(http_get(url).json())

def fetch_standings_nhle():
    url = "https://api-web.nhle.com/v1/standings/now"
    return parse_many_shapes(http_get(url).json())

# --- NEW: ESPN fallback (very tolerant parser)
def fetch_standings_espn():
    # Public, GitHub-friendly
    url = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/standings"
    js = http_get(url, timeout=20, allow_empty=True).json()
    rows = []
    leagues = js.get("children") or js.get("leagues") or []
    if not leagues and "standings" in js:
        leagues = [js]  # some shapes flatten

    def num(v):
        try:
            return float(v)
        except Exception:
            return None

    for lg in leagues:
        tables = (lg.get("standings") or {}).get("entries") or lg.get("entries") or []
        for ent in tables:
            team = (ent.get("team") or {})
            abbr = team.get("abbreviation") or team.get("shortDisplayName") or team.get("name")
            stats = { (s.get("name") or s.get("type")): s.get("value") for s in (ent.get("stats") or []) }
            if abbr:
                rows.append({
                    "Team": abbr,
                    "Wins": num(stats.get("wins")),
                    "Losses": num(stats.get("losses")),
                    "OT": num(stats.get("otLosses") or stats.get("ties")),
                    "GF": num(stats.get("pointsFor") or stats.get("goalsFor")),
                    "GA": num(stats.get("pointsAgainst") or stats.get("goalsAgainst")),
                })
    return pd.DataFrame(rows)

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
        try:
            df = fetch_standings_espn()
            if df.empty:
                print("[teams] ESPN standings empty; writing team list only")
        except Exception as e:
            print(f"[teams] espn failed: {e}")
            df = pd.DataFrame()

    if df.empty:
        # absolute fallback: write headers so downstream won’t break
        df = pd.DataFrame(columns=["Team","Wins","Losses","OT","GF","GA"])

    for c in ("Wins","Losses","OT","GF","GA"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    ensure_outdir(OUT)
    df.to_csv(OUT, index=False)
    print(f"[teams] wrote {len(df)} rows to {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
