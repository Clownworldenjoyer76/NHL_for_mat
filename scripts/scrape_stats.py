#!/usr/bin/env python3
import sys, time
from pathlib import Path
from datetime import datetime, timezone
import requests
import pandas as pd

OUT = Path("outputs/player_stats.csv")

SESSION = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nhl.com/",
}

def ensure_outdir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def current_season_code(today=None):
    d = today or datetime.now(timezone.utc)
    y = d.year
    if d.month >= 8:
        a, b = y, y + 1
    else:
        a, b = y - 1, y
    return f"{a}{b}"

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

def load_players():
    for p in (Path("outputs/players.csv"), Path("players.csv")):
        if p.exists():
            try:
                df = pd.read_csv(p)
                if not df.empty:
                    return df
            except Exception:
                pass
    return pd.DataFrame()

def fetch_player_stat_statsapi(person_id, season_code):
    url = f"https://statsapi.web.nhl.com/api/v1/people/{person_id}/stats"
    params = {"stats": "statsSingleSeason", "season": season_code}
    js = http_get(url, params=params).json()
    splits = ((js.get("stats") or [{}])[0].get("splits") or [])
    if not splits:
        return None
    stat = splits[0].get("stat", {}) or {}
    return {
        "games_played": stat.get("games"),
        "goals": stat.get("goals"),
        "assists": stat.get("assists"),
        "points": stat.get("points"),
        "shots": stat.get("shots"),
        "plus_minus": stat.get("plusMinus"),
        "penalty_minutes": stat.get("pim"),
        "time_on_ice": stat.get("timeOnIce"),
    }

def fetch_player_stat_nhle(person_id, season_code):
    # NHLE landing sometimes requires season param, sometimes not; try both
    urls = [
        (f"https://api-web.nhle.com/v1/player/{person_id}/landing", {"season": season_code}),
        (f"https://api-web.nhle.com/v1/player/{person_id}/landing", None),
    ]
    js = None
    for u, q in urls:
        try:
            js = http_get(u, params=q).json()
            if js:
                break
        except Exception:
            continue
    if not isinstance(js, dict):
        return None

    # Try several shapes for totals
    def first_nonempty(*paths):
        for path in paths:
            cur = js
            ok = True
            for key in path:
                if isinstance(cur, dict) and key in cur:
                    cur = cur[key]
                else:
                    ok = False
                    break
            if ok and cur:
                return cur
        return None

    totals = first_nonempty(
        ["seasonTotals"],
        ["skaterStats", "regularSeason", "seasonTotals"],
        ["careerTotals", "regularSeason"]
    )
    if isinstance(totals, list):
        totals = totals[-1] if totals else None
        if isinstance(totals, dict) and "stat" in totals:
            totals = totals["stat"]
    if not isinstance(totals, dict):
        return None

    return {
        "games_played": totals.get("gamesPlayed") or totals.get("games"),
        "goals": totals.get("goals"),
        "assists": totals.get("assists"),
        "points": totals.get("points"),
        "shots": totals.get("shots"),
        "plus_minus": totals.get("plusMinus"),
        "penalty_minutes": totals.get("pim") or totals.get("penaltyMinutes"),
        "time_on_ice": totals.get("timeOnIce"),
    }

def fetch_stats(players_df):
    season = current_season_code()
    rows = []
    for _, r in players_df.iterrows():
        pid = r.get("player_id")
        name = r.get("name")
        team = r.get("team")
        if pd.isna(pid):
            continue
        stat = None
        try:
            stat = fetch_player_stat_statsapi(int(pid), season)
        except Exception:
            pass
        if not stat:
            try:
                stat = fetch_player_stat_nhle(int(pid), season)
            except Exception:
                pass
        if stat:
            s = dict(stat)
            s.update({"player_id": pid, "name": name, "team": team})
            rows.append(s)
        time.sleep(0.12)
    return pd.DataFrame(rows)

def fetch_stats_sportsipy_fallback():
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
                    "games_played": getattr(p, "games_played", None),
                    "goals": getattr(p, "goals", None),
                    "assists": getattr(p, "assists", None),
                    "points": getattr(p, "points", None),
                    "shots": getattr(p, "shots", None),
                    "plus_minus": getattr(p, "plus_minus", None),
                    "penalty_minutes": getattr(p, "penalty_minutes", None),
                    "time_on_ice": getattr(p, "time_on_ice", None),
                })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()

def main():
    players = load_players()
    if players.empty:
        print("[player_stats] no players available yet; writing empty headers")
        ensure_outdir(OUT)
        cols = ["player_id","name","team","games_played","goals","assists","points","shots","plus_minus","penalty_minutes","time_on_ice"]
        pd.DataFrame(columns=cols).to_csv(OUT, index=False)
        return 0

    df = fetch_stats(players)
    if df.empty:
        df = fetch_stats_sportsipy_fallback()

    if not df.empty:
        df = df.drop_duplicates(subset=["player_id"]).reset_index(drop=True)

    ensure_outdir(OUT)
    df.to_csv(OUT, index=False)
    print(f"[player_stats] wrote {len(df)} rows to {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
