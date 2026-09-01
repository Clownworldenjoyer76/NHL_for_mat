# docs/win/soccer/scripts/01_merge/market_model.py

#!/usr/bin/env python3

import csv
from pathlib import Path

MERGE_DIR = Path("docs/win/soccer/01_merge")
OUT_DIR = MERGE_DIR / "market_model"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_BASE = Path("config/soccer")

CONFIG_MAP = {
    "bundesliga": "bundesliga",
    "epl": "epl",
    "laliga": "la_liga",
    "ligue1": "ligue1",
    "seriea": "serie_a",
    "mls": "mls"
}

REVERSED_LEAGUES = {"bundesliga","epl","laliga","ligue1"}

DC_CACHE = {}


def load_dc_table(path):

    rows = []

    with open(path, newline="", encoding="utf-8") as f:

        reader = csv.DictReader(f)

        for r in reader:
            try:
                rows.append({
                    "lambda_home": float(r["lambda_home"]),
                    "lambda_away": float(r["lambda_away"]),
                    "home_win": float(r["home_win"]),
                    "draw": float(r["draw"]),
                    "away_win": float(r["away_win"]),
                    "over2_5": float(r["over2_5"]),
                    "under2_5": float(r["under2_5"]),
                    "over3_5": float(r["over3_5"]),
                    "under3_5": float(r["under3_5"]),
                    "btts_yes": float(r["btts_yes"])
                })
            except:
                continue

    return rows


def get_dc_table(market):

    if market in DC_CACHE:
        return DC_CACHE[market]

    dc_file = CONFIG_BASE / CONFIG_MAP[market] / "dc_soccer_pricing_engine.csv"

    if not dc_file.exists():
        return None

    table = load_dc_table(dc_file)

    DC_CACHE[market] = table

    return table


def interpolate(table, lh, la, k=6):

    distances = []

    for r in table:

        dist = (r["lambda_home"] - lh) ** 2 + (r["lambda_away"] - la) ** 2

        distances.append((dist, r))

    distances.sort(key=lambda x: x[0])

    nearest = distances[:k]

    weight_sum = 0

    h = d = a = o = u = o35 = u35 = b = 0

    for dist, r in nearest:

        w = 1 / (dist + 1e-9)

        weight_sum += w

        h += r["home_win"] * w
        d += r["draw"] * w
        a += r["away_win"] * w
        o += r["over2_5"] * w
        u += r["under2_5"] * w
        o35 += r["over3_5"] * w
        u35 += r["under3_5"] * w
        b += r["btts_yes"] * w

    return {
        "h": h / weight_sum,
        "d": d / weight_sum,
        "a": a / weight_sum,
        "o": o / weight_sum,
        "u": u / weight_sum,
        "o35": o35 / weight_sum,
        "u35": u35 / weight_sum,
        "b": b / weight_sum
    }


merge_files = list(MERGE_DIR.glob("soccer_*.csv"))

for merge_file in merge_files:

    outfile = OUT_DIR / merge_file.name

    processed_rows = []

    with open(merge_file, newline="", encoding="utf-8") as f:

        reader = csv.DictReader(f)

        orig_fields = reader.fieldnames

        add_fields = [
            "lambda_home","lambda_away",
            "over25_prob","under25_prob",
            "over35_prob","under35_prob",
            "btts_prob"
        ]

        fieldnames = [f for f in orig_fields if f not in add_fields] + add_fields

        for r in reader:

            market = r["market"]

            table = get_dc_table(market)

            if not table:
                continue

            try:

                lh = float(r["home_xg"])
                la = float(r["away_xg"])

                if market in REVERSED_LEAGUES:
                    res = interpolate(table, la, lh)
                else:
                    res = interpolate(table, lh, la)

                r.update({
                    "home_prob": res["h"],
                    "draw_prob": res["d"],
                    "away_prob": res["a"],
                    "lambda_home": lh,
                    "lambda_away": la,
                    "over25_prob": res["o"],
                    "under25_prob": res["u"],
                    "over35_prob": res["o35"],
                    "under35_prob": res["u35"],
                    "btts_prob": res["b"]
                })

                processed_rows.append(r)

            except:
                continue

    if processed_rows:

        with open(outfile,"w",newline="",encoding="utf-8") as f:

            writer = csv.DictWriter(f, fieldnames=fieldnames)

            writer.writeheader()

            writer.writerows(processed_rows)

        print(f"Wrote {outfile} ({len(processed_rows)} rows)")
