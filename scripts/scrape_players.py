#!/usr/bin/env python3
import sys, time
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

# --- guaranteed logger import (or safe no-op) ---
try:
    from scripts.netlog import log_event
except Exception:
    def log_event(msg: str):
        try:
            p = Path("outputs/network_log.txt")
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8") as f:
                f.write(f"[{datetime.now(timezone.utc).isoformat()}] {msg}\n")
        except Exception:
            pass

log_event("=== START scrape_players ===")

OUT = Path("outputs/players.csv")

TEAM_ABBRS = [
    "ANA","ARI","BOS","BUF","CAR","CBJ","CGY","CHI","COL","DAL","DET","EDM","FLA","LAK",
    "MIN","MTL","NJD","NSH","NYI","NYR","OTT","PHI","PIT","SEA","SJS","STL","TBL","TOR",
    "VAN","VGK","WPG","WSH"
]

SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nhl.com/",
}

def current_season_code(today=None):
    d = today or datetime.now(timezone.utc)
    y = d.year
    if d.month >= 8:
        a, b = y, y + 1
    else:
        a, b = y - 1, y
    return f"{a}{b}"

def http_get(url, params=None, timeout=20, allow_empty=False):
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, headers=HEADERS, timeout=timeout)
            log_event(f"GET {url} params={params} -> status {r.status_code}, bytes {len(r.content)}")
            if r.status_code in (429, 503, 502):
                time.sleep(0.5 * (attempt + 1))
                continue
            r.raise_for_status()
            if not allow_empty and not r.content:
                raise requests.RequestException("Empty body")
            return r
        except requests.RequestException as e:
            log_event(f"ERROR {url} -> {type(e).__name__}: {e}")
            last = e
            time.sleep(0.5 * (attempt + 1))
    raise last

def ensure_outdir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

# --- NHL (statsapi) ---
def fetch_rosters_statsapi():
    url = "https://statsapi.nhl.com/api/v1/teams"
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

# --- NHL (api-web) ---
def try_roster_nhle(abbr: str, season: str):
    urls = [
        f"https://api-web.nhle.com/v1/roster/{abbr}/{season}",
        f"https://api-web.nhle.com/v1/roster/{abbr}/current",
    ]
    for u in urls:
        try:
            js = http_get(u).json()
            if isinstance(js, dict) and any(k in js for k in ("forwards","defensemen","goalies","roster")):
                return js
        except Exception:
            continue
    return None

def fetch_rosters_nhle():
    season = current_season_code()
    rows = []
    ok = 0
    for abbr in TEAM_ABBRS:
        js = try_roster_nhle(abbr, season)
        if not js:
            continue
        ok += 1
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
        time.sleep(0.1)
    log_event(f"NHLE roster teams_ok={ok}")
    return pd.DataFrame(rows)

# --- ESPN fallback ---
def fetch_rosters_espn():
    base = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"
    teams_js = http_get(f"{base}/teams", allow_empty=True).json()
    buckets = []
    if isinstance(teams_js, dict):
        if teams_js.get("teams"):
            buckets = teams_js["teams"]
        else:
            try:
                buckets = teams_js["sports"][0]["leagues"][0]["teams"]
            except Exception:
                buckets = []
    rows = []
    ok = 0
    for b in buckets:
        team = b.get("team") or b
        tid = team.get("id")
        abbr = team.get("abbreviation") or team.get("shortDisplayName") or team.get("name")
        if not tid or not abbr:
            continue
        rjs = http_get(f"{base}/teams/{tid}", params={"enable":"roster"}, allow_empty=True).json()
        athletes = (((rjs.get("team") or {}).get("roster") or {}).get("entries")) or []
        if athletes:
            ok += 1
        for ent in athletes:
            ath = ent.get("athlete") or {}
            pid = ath.get("id")
            name = ath.get("displayName") or ath.get("shortName")
            pos = ((ath.get("position") or {}).get("abbreviation")) or None
            if pid and name:
                rows.append({
                    "player_id": pid,
                    "name": name,
                    "team": abbr,
                    "position": pos,
                })
        time.sleep(0.06)
    log_event(f"ESPN roster teams_ok={ok}")
    return pd.DataFrame(rows)

def main():
    df = pd.DataFrame()
    try:
        df = fetch_rosters_statsapi()
    except Exception as e:
        log_event(f"[players] statsapi failed: {e}")
        print(f"[players] statsapi failed: {e}")
    if df.empty:
        try:
            df = fetch_rosters_nhle()
        except Exception as e:
            log_event(f"[players] nhle failed: {e}")
            print(f"[players] api-web.nhle.com failed: {e}")
    if df.empty:
        try:
            df = fetch_rosters_espn()
        except Exception as e:
            log_event(f"[players] espn failed: {e}")
            print(f"[players] espn failed: {e}")
            df = pd.DataFrame()

    if not df.empty:
        df = df.dropna(subset=["player_id","name"]).drop_duplicates(subset=["player_id"]).reset_index(drop=True)

    ensure_outdir(OUT)
    df.to_csv(OUT, index=False)
    print(f"[players] wrote {len(df)} rows to {OUT}")
    log_event(f"[players] wrote {len(df)} rows to {OUT}")
    log_event("=== END scrape_players ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
