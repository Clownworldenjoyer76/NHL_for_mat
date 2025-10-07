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

OUT = Path("outputs/player_stats.csv")

def load_players():
    candidates = [Path("outputs/players.csv"), Path("players.csv")]
    for p in candidates:
        if p.exists():
            try:
                df = pd.read_csv(p)
                if not df.empty:
                    return df
            except Exception:
                pass
    return pd.DataFrame()

def fetch_player_stat(person_id, season_code):
    url = f"https://statsapi.web.nhl.com/api/v1/people/{person_id}/stats"
    params = {"stats": "statsSingleSeason", "season": season_code}
    r = http_get(url, params=params)
    js = r.json()
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

def fetch_stats_nhlapi(players_df):
    season = current_season_code()
    rows = []
    for _, r in players_df.iterrows():
        pid = r.get("player_id")
        name = r.get("name")
        team = r.get("team")
        if pd.isna(pid):
            continue
        try:
            s = fetch_player_stat(int(pid), season)
            if not s:
                continue
            s.update({"player_id": pid, "name": name, "team": team})
            rows.append(s)
        except Exception:
            continue
        time.sleep(0.2)
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

    try:
        df_stats = fetch_stats_nhlapi(players)
    except Exception as e:
        print(f"[player_stats] NHL API failed, using sportsipy fallback: {e}")
        df_stats = fetch_stats_sportsipy_fallback()

    if not df_stats.empty:
        df_stats = df_stats.drop_duplicates(subset=["player_id"]).reset_index(drop=True)

    ensure_outdir(OUT)
    df_stats.to_csv(OUT, index=False)
    print(f"[player_stats] wrote {len(df_stats)} rows to {OUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
