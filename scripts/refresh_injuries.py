#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

REF_PATHS = [
    Path("data/reference/injuries.csv"),
    Path("injuries.csv"),
    Path("/mnt/data/injuries.csv"),
]

OUT = Path("outputs/injury_flags.csv")
REQUIRED = ["player_id","player_name","team","status"]

MAP = {
    "out":"out","ir":"out","injured reserve":"out",
    "dtd":"dtd","day-to-day":"dtd","probable":"probable",
    "active":"active","ok":"active","healthy":"active"
}

def norm_status(x):
    if x is None:
        return "active"
    s = str(x).strip().lower()
    return MAP.get(s, s if s in {"out","ir","dtd","probable","active"} else "active")

def main():
    src = next((p for p in REF_PATHS if p.exists()), None)
    if not src:
        print("[injuries] no reference file found; writing empty snapshot")
        pd.DataFrame(columns=REQUIRED + ["detail","source","asof","status_norm"]).to_csv(OUT, index=False)
        return 0

    df = pd.read_csv(src)
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        print(f"[injuries] missing required columns: {missing}; writing empty snapshot")
        pd.DataFrame(columns=REQUIRED + ["detail","source","asof","status_norm"]).to_csv(OUT, index=False)
        return 0

    if "asof" not in df.columns:
        df["asof"] = pd.NaT
    df["asof_parsed"] = pd.to_datetime(df["asof"], errors="coerce")
    df["status_norm"] = df["status"].map(norm_status)

    # dedupe by player_id keep latest
    df = df.sort_values(["player_id","asof_parsed"]).drop_duplicates(subset=["player_id"], keep="last").drop(columns=["asof_parsed"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)

    counts = df["status_norm"].value_counts(dropna=False).to_dict()
    print(f"[injuries] {len(df)} rows → snapshot written; counts={counts}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
