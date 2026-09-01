# docs/win/basketball/scripts/04_select/select_bets.py
#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

INPUT_DIR = Path("docs/win/basketball/03_edges")
OUTPUT_DIR = Path("docs/win/basketball/04_select")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    for csv_file in INPUT_DIR.glob("*.csv"):
        df = pd.read_csv(csv_file)
        fname = csv_file.name.lower()
        league = "NBA" if "nba" in fname else "NCAAB"

        results = []

        for _, row in df.iterrows():

            # =========================
            # TOTALS
            # =========================
            if "total" in fname:

                line = pd.to_numeric(row.get("total"), errors="coerce")
                proj = pd.to_numeric(row.get("total_projected_points"), errors="coerce")
                diff = abs(proj - line) if pd.notna(proj) and pd.notna(line) else 0

                if league == "NBA":

                    # IMPORTANT: Use edge_pct (relative edge) to avoid underdog/longshot bias
                    edges = {
                        "over": float(row.get("over_edge_pct", 0) or 0),
                        "under": float(row.get("under_edge_pct", 0) or 0),
                    }

                    # only allow positive edges
                    valid_edges = {k: v for k, v in edges.items() if pd.notna(v) and v > 0}
                    if not valid_edges:
                        continue

                    side = max(valid_edges, key=valid_edges.get)
                    edge = valid_edges[side]

                    # base threshold for NBA totals using edge_pct
                    edge_required = 0.07  # ~7% relative edge

                    # your existing logic, translated to edge_pct world
                    if pd.notna(line) and line <= 205 and side == "over":
                        edge_required -= 0.02  # slightly easier for low totals overs

                    if pd.notna(line) and line <= 205 and side == "under":
                        continue

                    if pd.notna(line) and line > 245:
                        continue

                    # sanity check projected total difference
                    if diff < 3:
                        continue

                    # guardrail: if edge_pct is absurdly high, it's usually bad inputs
                    if edge > 0.35:
                        continue

                    if edge >= edge_required:
                        new_row = row.copy()
                        new_row["market_type"] = "total"
                        new_row["bet_side"] = side
                        new_row["line"] = line
                        results.append(new_row)

                elif league == "NCAAB":

                    for side in ["over", "under"]:

                        edge = row.get(f"{side}_edge_decimal", 0)
                        edge = float(edge) if pd.notna(edge) else 0

                        if side == "over":
                            if line < 150:
                                if edge >= 0.50 and diff >= 4:
                                    new_row = row.copy()
                                    new_row["market_type"] = "total"
                                    new_row["bet_side"] = side
                                    new_row["line"] = line
                                    results.append(new_row)
                            elif line > 150:
                                if edge > 0.001 and diff >= 2:
                                    new_row = row.copy()
                                    new_row["market_type"] = "total"
                                    new_row["bet_side"] = side
                                    new_row["line"] = line
                                    results.append(new_row)

                        else:
                            if edge >= 0.10:
                                new_row = row.copy()
                                new_row["market_type"] = "total"
                                new_row["bet_side"] = side
                                new_row["line"] = line
                                results.append(new_row)

            # =========================
            # SPREADS
            # =========================
            elif "spread" in fname:

                if league == "NBA":

                    # IMPORTANT: Use edge_pct (relative edge)
                    edges = {
                        "home": float(row.get("home_edge_pct", 0) or 0),
                        "away": float(row.get("away_edge_pct", 0) or 0),
                    }

                    valid_edges = {k: v for k, v in edges.items() if pd.notna(v) and v > 0}
                    if not valid_edges:
                        continue

                    side = max(valid_edges, key=valid_edges.get)
                    edge = valid_edges[side]

                    # threshold in edge_pct terms
                    if edge >= 0.06:

                        spread_val = float(row.get(f"{side}_spread")) if pd.notna(row.get(f"{side}_spread")) else 0.0
                        spread_abs = abs(spread_val)
                        venue = side

                        if spread_abs > 15:
                            continue

                        # remove overpriced large home favorites
                        if spread_val <= -7.5 and venue == "home":
                            continue

                        if spread_abs <= 10.5:
                            new_row = row.copy()
                            new_row["market_type"] = "spread"
                            new_row["bet_side"] = side
                            new_row["line"] = spread_val
                            results.append(new_row)

                elif league == "NCAAB":

                    for side in ["home", "away"]:

                        edge = row.get(f"{side}_edge_decimal", 0)
                        spread = row.get(f"{side}_spread", 0)

                        edge = float(edge) if pd.notna(edge) else 0
                        spread_val = float(row.get(f"{side}_spread")) if pd.notna(row.get(f"{side}_spread")) else 0
                        spread_abs = abs(spread_val)

                        if edge >= 0.07 and spread_abs <= 20:
                            new_row = row.copy()
                            new_row["market_type"] = "spread"
                            new_row["bet_side"] = side
                            new_row["line"] = spread_val
                            results.append(new_row)

            # =========================
            # MONEYLINE
            # =========================
            elif "moneyline" in fname:

                if league == "NBA":

                    # IMPORTANT: Use edge_pct (relative edge) to stop big-dog bias
                    edges = {
                        "home": float(row.get("home_edge_pct", 0) or 0),
                        "away": float(row.get("away_edge_pct", 0) or 0),
                    }

                    valid_edges = {k: v for k, v in edges.items() if pd.notna(v) and v > 0}
                    if not valid_edges:
                        continue

                    side = max(valid_edges, key=valid_edges.get)
                    edge = valid_edges[side]

                    odds = float(row.get(f"{side}_juice_odds", 0) or 0)
                    venue = side
                    fav_ud = "favorite" if odds < 0 else "underdog"

                    # avoid overpriced home favorites
                    if fav_ud == "favorite" and venue == "home" and -180 <= odds <= -140:
                        continue

                    if fav_ud == "favorite" and odds <= -500:
                        continue

                    # avoid massive dogs
                    if fav_ud == "underdog" and odds >= 350:
                        continue

                    # edge_pct thresholds
                    if fav_ud == "favorite":
                        edge_required = 0.06
                    else:
                        edge_required = 0.06

                    if fav_ud == "favorite" and venue == "home":
                        edge_required = 0.08

                    # historically good band: away dogs +130 to +160
                    if fav_ud == "underdog" and venue == "away" and 130 <= odds <= 160:
                        edge_required = 0.05

                    # extra penalty for home sides generally
                    if venue == "home":
                        edge_required += 0.01

                    # guardrail: if edge_pct is absurdly high, it's often bad inputs
                    if edge > 0.35:
                        continue

                    if edge < edge_required:
                        continue

                    new_row = row.copy()
                    new_row["market_type"] = "moneyline"
                    new_row["bet_side"] = side
                    new_row["line"] = odds
                    results.append(new_row)

                elif league == "NCAAB":

                    for side in ["home", "away"]:

                        edge = row.get(f"{side}_edge_decimal", 0)
                        prob = row.get(f"{side}_prob", 0)

                        edge = float(edge) if pd.notna(edge) else 0
                        prob = float(prob) if pd.notna(prob) else 0

                        if edge >= 0.06 and prob >= 0.60:
                            new_row = row.copy()
                            new_row["market_type"] = "moneyline"
                            new_row["bet_side"] = side
                            new_row["line"] = 0
                            results.append(new_row)

        if results:
            pd.DataFrame(results).drop_duplicates().to_csv(
                OUTPUT_DIR / csv_file.name,
                index=False
            )

if __name__ == "__main__":
    main()
