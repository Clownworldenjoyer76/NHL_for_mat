#!/usr/bin/env python3
import math
import pandas as pd
from pathlib import Path

REF_PATHS = [
    Path("data/reference/rinks.csv"),
    Path("rinks.csv"),
    Path("/mnt/data/rinks.csv"),
]

OUT = Path("outputs/rinks_used.csv")
REQUIRED = ["arena_id","team","arena_name","home_rink_scoring_bias"]

def coerce_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(str(x).strip())
    except Exception:
        return None

def main():
    src = next((p for p in REF_PATHS if p.exists()), None)
    if not src:
        print("[rinks] no reference file found; writing empty snapshot")
        pd.DataFrame(columns=REQUIRED + ["shot_coord_bias_x","shot_coord_bias_y","notes","asof"]).to_csv(OUT, index=False)
        return 0

    df = pd.read_csv(src)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        print(f"[rinks] missing required columns: {missing}; writing empty snapshot")
        pd.DataFrame(columns=REQUIRED + ["shot_coord_bias_x","shot_coord_bias_y","notes","asof"]).to_csv(OUT, index=False)
        return 0

    # normalize bias
    df["home_rink_scoring_bias"] = df["home_rink_scoring_bias"].map(coerce_float).clip(lower=0.7, upper=1.3)
    for c in ("shot_coord_bias_x","shot_coord_bias_y"):
        if c in df.columns:
            df[c] = df[c].map(coerce_float)
    if "asof" not in df.columns:
        df["asof"] = pd.NaT
    df["asof_parsed"] = pd.to_datetime(df["asof"], errors="coerce")

    # dedupe by arena_id, keep most recent
    df = df.sort_values(["arena_id","asof_parsed"]).drop_duplicates(subset=["arena_id"], keep="last").drop(columns=["asof_parsed"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    med = df["home_rink_scoring_bias"].median() if not df.empty else float("nan")
    print(f"[rinks] {len(df)} rows → snapshot written to {OUT}; median_bias={med:.3f}" if len(df) else "[rinks] empty snapshot written")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
