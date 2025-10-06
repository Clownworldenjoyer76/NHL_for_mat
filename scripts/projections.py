#!/usr/bin/env python3
"""
Applies rink, goalie, and injury factors to projections.
Empty-safe: if no base projections or empty CSVs, writes headers and exits 0.
"""
import math
import pandas as pd
from pathlib import Path

# Candidate base projections inputs (first that exists & non-empty will be used)
BASE_CANDIDATES = [
    Path("outputs/projections.csv"),
    Path("projections.csv"),
    Path("data/projections.csv"),
]

RINKS = Path("outputs/rinks_used.csv")
GOALIES = Path("outputs/goalie_matrix_today.csv")
INJURIES = Path("outputs/injury_flags.csv")

OUT = Path("outputs/projections.csv")

def read_first_existing(paths):
    for p in paths:
        if p.exists() and p.is_file():
            try:
                df = pd.read_csv(p)
                if len(df.columns) == 0:
                    continue
                return df, p
            except Exception:
                continue
    return pd.DataFrame(), None

def compute_expected_team_sv(df_goalies):
    if df_goalies.empty or "team" not in df_goalies.columns or "sv_pct" not in df_goalies.columns:
        return {}, float("nan")
    league_sv = df_goalies["sv_pct"].dropna().mean()
    if "starter_prob" not in df_goalies.columns:
        df_goalies["starter_prob"] = 0.5
    team_sv = {}
    for team, grp in df_goalies.groupby("team"):
        sv = (grp["starter_prob"].fillna(0) * grp["sv_pct"].fillna(league_sv)).sum()
        prob_sum = grp["starter_prob"].fillna(0).sum()
        if prob_sum <= 0.01:
            sv = grp["sv_pct"].fillna(league_sv).mean()
        team_sv[team] = float(sv)
    return team_sv, float(league_sv) if pd.notna(league_sv) else 0.905

def main():
    base, used_path = read_first_existing(BASE_CANDIDATES)
    if base.empty:
        shell_cols = ["name","team","proj_points_raw","goalie_factor","rink_factor","injury_factor","proj_points_final"]
        pd.DataFrame(columns=shell_cols).to_csv(OUT, index=False)
        print("[projections] no base projections found; wrote empty headers")
        return 0

    if "team" not in base.columns:
        base["team"] = None
    # choose a raw points column heuristic
    if "proj_points_raw" not in base.columns:
        for alt in ("proj_points","points","expected_points"):
            if alt in base.columns:
                base = base.rename(columns={alt:"proj_points_raw"})
                break
        if "proj_points_raw" not in base.columns:
            base["proj_points_raw"] = 0.0

    # Load reference snapshots if present
    rinks = pd.read_csv(RINKS) if RINKS.exists() else pd.DataFrame()
    goalies = pd.read_csv(GOALIES) if GOALIES.exists() else pd.DataFrame()
    injuries = pd.read_csv(INJURIES) if INJURIES.exists() else pd.DataFrame()

    # Goalie factor per opponent
    team_sv, league_sv = compute_expected_team_sv(goalies)
    if "opponent" not in base.columns:
        base["opponent"] = None

    def gf(opp_team):
        sv = team_sv.get(opp_team)
        if sv is None or pd.isna(sv):
            return 1.0
        denom = max(0.880, min(0.960, sv))
        return float(league_sv) / denom if denom else 1.0

    base["goalie_factor"] = base["opponent"].map(gf).fillna(1.0)

    # Rink factor: home bias only
    def rf(row):
        if "home" in base.columns and bool(row.get("home")) and not rinks.empty and "team" in rinks.columns:
            rec = rinks.loc[rinks["team"] == row.get("team")]
            if not rec.empty and "home_rink_scoring_bias" in rec.columns:
                try:
                    v = float(rec.iloc[0]["home_rink_scoring_bias"])
                    if v > 0:
                        return v
                except Exception:
                    pass
        return 1.0

    base["rink_factor"] = base.apply(rf, axis=1)

    # Injury factor
    if not injuries.empty:
        def status_to_factor(s):
            s = str(s).strip().lower() if pd.notna(s) else "active"
            if s in {"out","ir"}: return 0.0
            if s in {"dtd","probable"}: return 0.8
            return 1.0

        if "player_id" in base.columns and "player_id" in injuries.columns:
            inj_map = {pid: status_to_factor(st) for pid, st in injuries[["player_id","status"]].itertuples(index=False)}
            base["injury_factor"] = base["player_id"].map(inj_map).fillna(1.0)
        else:
            # fallback on (name, team) lower
            if "player_name" in injuries.columns:
                injuries["_key"] = (injuries["player_name"].astype(str).str.lower().fillna("") + "|" + injuries.get("team","").astype(str))
            else:
                injuries["_key"] = (injuries.get("player_name","").astype(str).str.lower().fillna("") + "|" + injuries.get("team","").astype(str))
            if "name" not in base.columns:
                base["name"] = base.get("player_name", "")
            base["_key"] = base["name"].astype(str).str.lower().fillna("") + "|" + base.get("team","").astype(str)
            inj_map2 = {k: status_to_factor(s) for k, s in injuries[["_key","status"]].itertuples(index=False)}
            base["injury_factor"] = base["_key"].map(inj_map2).fillna(1.0)
            base.drop(columns=["_key"], errors="ignore", inplace=True)
    else:
        base["injury_factor"] = 1.0

    for c in ("goalie_factor","rink_factor","injury_factor","proj_points_raw"):
        if c not in base.columns:
            base[c] = 1.0 if c != "proj_points_raw" else 0.0

    base["proj_points_final"] = (
        pd.to_numeric(base["proj_points_raw"], errors="coerce").fillna(0.0)
        * pd.to_numeric(base["goalie_factor"], errors="coerce").fillna(1.0)
        * pd.to_numeric(base["rink_factor"], errors="coerce").fillna(1.0)
        * pd.to_numeric(base["injury_factor"], errors="coerce").fillna(1.0)
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    base.to_csv(OUT, index=False)
    print(f"[projections] wrote {len(base)} rows with factors to {OUT}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
