#!/usr/bin/env python3
# docs/win/basketball/scripts/04_select/select_bets.py

import pandas as pd
from pathlib import Path
from datetime import datetime

INPUT_DIR = Path("docs/win/basketball/03_edges")
OUTPUT_DIR = Path("docs/win/basketball/04_select")
ERROR_DIR = Path("docs/win/basketball/errors/04_select")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

REPORT_FILE = ERROR_DIR / "select_report.txt"


###############################################################
######################## HELPERS ##############################
###############################################################

def f(x):
    try:
        if pd.isna(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def s(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def reset_report():
    with open(REPORT_FILE, "w", encoding="utf-8") as fh:
        fh.write("BASKETBALL 04_SELECT REPORT\n")
        fh.write("=" * 100 + "\n")


def report_line(text):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(REPORT_FILE, "a", encoding="utf-8") as fh:
        fh.write(f"[{ts}] {text}\n")


def game_label(row):
    away = s(row.get("away_team"))
    home = s(row.get("home_team"))
    game_id = s(row.get("game_id"))
    date = s(row.get("game_date"))
    return f"{date} | {away} @ {home} | game_id={game_id}"


def detect_market_from_filename(file_name):
    name = file_name.lower()
    if "moneyline" in name:
        return "moneyline"
    if "spread" in name:
        return "spread"
    if "total" in name:
        return "total"
    return ""


def blocked_ncaab_spread_line(line):
    if 1 <= line <= 3:
        return True
    if -10 <= line <= -5:
        return True
    if 1 <= abs(line) <= 7 and line > 0:
        return True
    return False


###############################################################
##################### STEP 1 NBA MONEYLINE ####################
###############################################################

def step1_nba_moneyline(row):
    home_edge = f(row.get("home_ml_edge_decimal"))
    away_edge = f(row.get("away_ml_edge_decimal"))

    home_ml = f(row.get("home_dk_moneyline_american"))
    away_ml = f(row.get("away_dk_moneyline_american"))

    home_cond1 = home_edge > 0.07
    home_cond2 = -180 <= home_ml <= 180

    away_cond1 = away_edge > 0.07
    away_cond2 = -180 <= away_ml <= 180

    if home_cond1 and home_cond2:
        return True, (
            f"PASS STEP 1 NBA MONEYLINE | HOME passed both conditions | "
            f"home_ml_edge_decimal={home_edge:.6f} > 0.07 and "
            f"home_dk_moneyline_american={home_ml:.2f} in [-180, 180]"
        )

    if away_cond1 and away_cond2:
        return True, (
            f"PASS STEP 1 NBA MONEYLINE | AWAY passed both conditions | "
            f"away_ml_edge_decimal={away_edge:.6f} > 0.07 and "
            f"away_dk_moneyline_american={away_ml:.2f} in [-180, 180]"
        )

    reasons = []
    reasons.append(
        f"HOME cond1 edge>0.07={home_cond1} (home_ml_edge_decimal={home_edge:.6f})"
    )
    reasons.append(
        f"HOME cond2 odds_in_range={home_cond2} (home_dk_moneyline_american={home_ml:.2f})"
    )
    reasons.append(
        f"AWAY cond1 edge>0.07={away_cond1} (away_ml_edge_decimal={away_edge:.6f})"
    )
    reasons.append(
        f"AWAY cond2 odds_in_range={away_cond2} (away_dk_moneyline_american={away_ml:.2f})"
    )

    return False, "FAIL STEP 1 NBA MONEYLINE | " + " | ".join(reasons)


###############################################################
##################### STEP 2 NBA SPREAD #######################
###############################################################

def step2_nba_spread(row):
    home_line = f(row.get("home_spread"))
    away_line = f(row.get("away_spread"))
    home_edge = f(row.get("home_spread_edge_decimal"))
    away_edge = f(row.get("away_spread_edge_decimal"))

    home_cond1 = home_edge >= 0.07
    away_cond1 = away_edge >= 0.07

    home_cond2 = -14.6 <= home_line <= 14.6 and not (-2 <= home_line <= 2)
    away_cond2 = -14.6 <= away_line <= 14.6 and not (-2 <= away_line <= 2)

    if home_cond1 and home_cond2:
        return True, (
            f"PASS STEP 2 NBA SPREAD | HOME passed | "
            f"home_spread_edge_decimal={home_edge:.6f} >= 0.07 and "
            f"home_spread={home_line:.2f} allowed"
        )

    if away_cond1 and away_cond2:
        return True, (
            f"PASS STEP 2 NBA SPREAD | AWAY passed | "
            f"away_spread_edge_decimal={away_edge:.6f} >= 0.07 and "
            f"away_spread={away_line:.2f} allowed"
        )

    reasons = []
    reasons.append(
        f"HOME edge_ok={home_cond1} (home_spread_edge_decimal={home_edge:.6f})"
    )
    reasons.append(
        f"HOME line_ok={home_cond2} (home_spread={home_line:.2f})"
    )
    reasons.append(
        f"AWAY edge_ok={away_cond1} (away_spread_edge_decimal={away_edge:.6f})"
    )
    reasons.append(
        f"AWAY line_ok={away_cond2} (away_spread={away_line:.2f})"
    )

    return False, "FAIL STEP 2 NBA SPREAD | " + " | ".join(reasons)


###############################################################
##################### STEP 3 NBA TOTAL ########################
###############################################################

def step3_nba_total(row):
    line = f(row.get("total"))
    proj = f(row.get("total_projected_points"))
    home_spread = abs(f(row.get("home_spread")))
    away_spread = abs(f(row.get("away_spread")))
    max_spread = max(home_spread, away_spread)

    over_edge = f(row.get("over_edge_decimal"))
    under_edge = f(row.get("under_edge_decimal"))
    proj_diff = abs(proj - line)

    if line > 245:
        return False, (
            f"FAIL STEP 3 NBA TOTAL | total limit failed | "
            f"total={line:.2f} > 245"
        ), ""

    if proj_diff < 3:
        return False, (
            f"FAIL STEP 3 NBA TOTAL | projection diff failed | "
            f"abs(total_projected_points - total)={proj_diff:.2f} < 3 | "
            f"total_projected_points={proj:.2f}, total={line:.2f}"
        ), ""

    if max_spread >= 13 and line >= 240:
        return False, (
            f"FAIL STEP 3 NBA TOTAL | blowout filter failed | "
            f"max(abs(home_spread), abs(away_spread))={max_spread:.2f} >= 13 "
            f"and total={line:.2f} >= 240"
        ), ""

    under_pass = False
    over_pass = False

    if line > 205 and 0.06 <= under_edge <= 0.40:
        under_pass = True

    if line <= 205:
        if 0.04 <= over_edge <= 0.35:
            over_pass = True
    else:
        if 0.06 <= over_edge <= 0.35:
            over_pass = True

    if over_pass and under_pass:
        return True, (
            f"PASS STEP 3 NBA TOTAL | BOTH passed | "
            f"total={line:.2f}, total_projected_points={proj:.2f}, "
            f"proj_diff={proj_diff:.2f}, max_spread={max_spread:.2f}, "
            f"over_edge_decimal={over_edge:.6f}, under_edge_decimal={under_edge:.6f}"
        ), "both"

    if over_pass:
        return True, (
            f"PASS STEP 3 NBA TOTAL | OVER passed | "
            f"total={line:.2f}, total_projected_points={proj:.2f}, "
            f"proj_diff={proj_diff:.2f}, max_spread={max_spread:.2f}, "
            f"over_edge_decimal={over_edge:.6f}, under_edge_decimal={under_edge:.6f}"
        ), "over"

    if under_pass:
        return True, (
            f"PASS STEP 3 NBA TOTAL | UNDER passed | "
            f"total={line:.2f}, total_projected_points={proj:.2f}, "
            f"proj_diff={proj_diff:.2f}, max_spread={max_spread:.2f}, "
            f"under_edge_decimal={under_edge:.6f}, over_edge_decimal={over_edge:.6f}"
        ), "under"

    return False, (
        f"FAIL STEP 3 NBA TOTAL | neither side passed | "
        f"total={line:.2f}, total_projected_points={proj:.2f}, "
        f"proj_diff={proj_diff:.2f}, max_spread={max_spread:.2f}, "
        f"over_edge_decimal={over_edge:.6f}, under_edge_decimal={under_edge:.6f}"
    ), ""


##############################################################################################################################
################### STEP 4 NCAAB MONEYLINE ####################
###############################################################

def step4_ncaab_moneyline(row):
    home_ml = f(row.get("home_dk_moneyline_american"))
    away_ml = f(row.get("away_dk_moneyline_american"))

    home_edge = f(row.get("home_ml_edge_decimal"))
    away_edge = f(row.get("away_ml_edge_decimal"))

    away_cond = away_ml > -150 and away_edge >= 0.08
    home_cond = home_ml > -200 and home_edge >= 0.05

    if away_cond:
        row["_bet_side"] = "away"
        row["_line"] = away_ml
        return True, f"PASS STEP 4 NCAAB MONEYLINE | away condition + edge passed"

    if home_cond:
        row["_bet_side"] = "home"
        row["_line"] = home_ml
        return True, f"PASS STEP 4 NCAAB MONEYLINE | home condition + edge passed"

    return False, f"FAIL STEP 4 NCAAB MONEYLINE"


###########################################################################################################
#################### STEP 5 NCAAB SPREAD ######################
###############################################################

def step5_ncaab_spread(row):
    home_line = f(row.get("home_spread"))
    away_line = f(row.get("away_spread"))

    home_blocked = blocked_ncaab_spread_line(home_line)
    away_blocked = blocked_ncaab_spread_line(away_line)

    if home_blocked or away_blocked:
        return False, (
            f"FAIL STEP 5 NCAAB SPREAD | blocked range hit | "
            f"home_spread={home_line:.2f}, away_spread={away_line:.2f}"
        )

    return True, (
        f"PASS STEP 5 NCAAB SPREAD | line allowed | "
        f"home_spread={home_line:.2f}, away_spread={away_line:.2f}"
    )


###############################################################
#################### STEP 6 NCAAB TOTAL #######################
###############################################################

def step6_ncaab_total(row):
    line = f(row.get("total"))
    over_edge = f(row.get("over_edge_decimal"))
    under_edge = f(row.get("under_edge_decimal"))

    over_pass = 145 <= line <= 155 and 0.12 <= over_edge <= 0.18
    under_pass = 141 <= line <= 150 and 0.10 <= under_edge <= 0.22

    if over_pass and under_pass:
        return True, (
            f"PASS STEP 6 NCAAB TOTAL | BOTH passed | "
            f"total={line:.2f}, over_edge_decimal={over_edge:.6f}, "
            f"under_edge_decimal={under_edge:.6f}"
        ), "both"

    if over_pass:
        return True, (
            f"PASS STEP 6 NCAAB TOTAL | OVER passed | "
            f"total={line:.2f}, over_edge_decimal={over_edge:.6f}"
        ), "over"

    if under_pass:
        return True, (
            f"PASS STEP 6 NCAAB TOTAL | UNDER passed | "
            f"total={line:.2f}, under_edge_decimal={under_edge:.6f}"
        ), "under"

    return False, (
        f"FAIL STEP 6 NCAAB TOTAL | neither side passed | "
        f"total={line:.2f}, over_edge_decimal={over_edge:.6f}, "
        f"under_edge_decimal={under_edge:.6f}"
    ), ""


###############################################################
#################### EDGE SELECTION ENGINE ####################
###############################################################

def process_file(csv_file):
    df = pd.read_csv(csv_file)

    if df.empty:
        report_line(f"FILE {csv_file.name} | INFO | empty input file")
        return

    fname = csv_file.name.lower()
    league = "NBA" if "nba" in fname else "NCAAB"
    market_type = detect_market_from_filename(csv_file.name)

    if not market_type:
        report_line(f"FILE {csv_file.name} | ERROR | could not detect market type from filename")
        return

    report_line(f"FILE {csv_file.name} | START | league={league} | market_type={market_type} | rows={len(df)}")

    selected_rows = []
    pass_count = 0
    fail_count = 0

    for _, row in df.iterrows():
        label = game_label(row)
        bet_side = ""

        if league == "NBA":
            if market_type == "moneyline":
                allowed, reason = step1_nba_moneyline(row)
            elif market_type == "spread":
                allowed, reason = step2_nba_spread(row)
            elif market_type == "total":
                allowed, reason, bet_side = step3_nba_total(row)
            else:
                allowed, reason = False, f"FAIL | unknown NBA market_type={market_type}"
        else:
            if market_type == "moneyline":
                allowed, reason = step4_ncaab_moneyline(row)
            elif market_type == "spread":
                allowed, reason = step5_ncaab_spread(row)
            elif market_type == "total":
                allowed, reason, bet_side = step6_ncaab_total(row)
            else:
                allowed, reason = False, f"FAIL | unknown NCAAB market_type={market_type}"

        if allowed:
            row_dict = row.to_dict()
            row_dict["market_type"] = market_type

            if market_type == "total":
                row_dict["bet_side"] = bet_side

            if market_type == "moneyline":
                row_dict["bet_side"] = row.get("_bet_side", "")
                row_dict["line"] = row.get("_line", "")

            selected_rows.append(row_dict)
            pass_count += 1
            report_line(f"PASS | {league} | {market_type} | {label} | {reason}")
        else:
            fail_count += 1
            report_line(f"FAIL | {league} | {market_type} | {label} | {reason}")

    if selected_rows:
        out_df = pd.DataFrame(selected_rows)
        out_path = OUTPUT_DIR / csv_file.name
        out_df.to_csv(out_path, index=False)
        report_line(
            f"FILE {csv_file.name} | DONE | selected_rows={len(out_df)} | passed={pass_count} | failed={fail_count} | output={out_path}"
        )
        print(f"Selected {len(out_df)} rows -> {out_path.name}")
    else:
        report_line(
            f"FILE {csv_file.name} | DONE | selected_rows=0 | passed={pass_count} | failed={fail_count} | no output file written"
        )
        print(f"Selected 0 rows -> {csv_file.name}")


###############################################################
############################ MAIN #############################
###############################################################

def main():
    reset_report()

    files = sorted(INPUT_DIR.glob("*.csv"))

    if not files:
        report_line("MAIN | INFO | No input files found")
        return

    for fpath in files:
        try:
            process_file(fpath)
        except Exception as e:
            report_line(f"FILE {fpath.name} | ERROR | {type(e).__name__}: {e}")

    report_line("MAIN | SUCCESS | Selection run complete")


if __name__ == "__main__":
    main()
