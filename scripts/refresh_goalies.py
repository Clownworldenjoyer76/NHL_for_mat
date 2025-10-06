#!/usr/bin/env python3
import math
import pandas as pd
from pathlib import Path

REF_PATHS = [
    Path("data/reference/goalies.csv"),
    Path("goalies.csv"),
    Path("/mnt/data/goalies.csv"),
]

OUT = Path("outputs/goalie_matrix_today.csv")
REQUIRED = ["player_id","goalie_name","team","sv_pct","starter_prob"]

def to_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(str(x).strip())
    except Exception:
        return None

def normalize_sv(x):
    v = to_float(x)
    if v is None:
        return None
    # allow 91.4 → 0.914
    if v > 1.5 and v <= 100.0:
        v = v / 100.0
    return max(0.80, min(0.97, v))  # plausible NHL bounds

def normalize_prob(x):
    v = to_float(x)
    if v is None:
        return 0.0
    return max(0.0, min(1.0, v))

def main():
    src = next((p for p in REF_PATHS if p.exists()), None)
    if not src:
        print("[goalies] no reference file found; writing empty snapshot")
        pd.DataFrame(columns=REQUIRED + ["sv_pct_ev","gsa_x","toi_minutes_rolling14","injury_status","asof"]).to_csv(OUT, index=False)
        return 0

    df = pd.read_csv(src)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        print(f"[goalies] missing required columns: {missing}; writing empty snapshot")
        pd.DataFrame(columns=REQUIRED + ["sv_pct_ev","gsa_x","toi_minutes_rolling14","injury_status","asof"]).to_csv(OUT, index=False)
        return 0

    df["sv_pct"] = df["sv_pct"].map(normalize_sv)
    if "sv_pct_ev" in df.columns:
        df["sv_pct_ev"] = df["sv_pct_ev"].map(normalize_sv)
    df["starter_prob"] = df["starter_prob"].map(normalize_prob)
    if "asof" not in df.columns:
        df["asof"] = pd.NaT
    df["asof_parsed"] = pd.to_datetime(df["asof"], errors="coerce")

    # dedupe by player_id keep most recent
    df = df.sort_values(["player_id","asof_parsed"]).drop_duplicates(subset=["player_id"], keep="last").drop(columns=["asof_parsed"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    league_sv = float(df["sv_pct"].mean()) if "sv_pct" in df.columns and not df["sv_pct"].isna().all() else float("nan")
    starters = int((df["starter_prob"] > 0.5).sum()) if "starter_prob" in df.columns else 0
    print(f"[goalies] {len(df)} rows → snapshot written; league_sv={league_sv:.3f} starters>.5={starters}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
