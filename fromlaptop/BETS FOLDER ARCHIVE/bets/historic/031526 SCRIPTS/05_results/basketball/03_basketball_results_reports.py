#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/basketball_results_reports.py

from datetime import datetime
from pathlib import Path

import pandas as pd

###############################################################
######################## PATH CONFIG ##########################
###############################################################

INTERMEDIATE_DIR = Path("docs/win/final_scores/intermediate")

DEEP_SUMMARY_BASE = Path("docs/win/final_scores/deeper_summaries")
NBA_DEEP_DIR = DEEP_SUMMARY_BASE / "nba"
NCAAB_DEEP_DIR = DEEP_SUMMARY_BASE / "ncaab"

NBA_MARKET_TALLY = Path("docs/win/final_scores/nba_market_tally.csv")
NCAAB_MARKET_TALLY = Path("docs/win/final_scores/ncaab_market_tally.csv")

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

EDGE_REPORT = ERROR_DIR / "basketball_edge_summary.txt"
REPORTS_ERROR_LOG = ERROR_DIR / "basketball_results_reports_errors.txt"
REPORTS_SUMMARY_LOG = ERROR_DIR / "basketball_results_reports_summary.txt"

###############################################################
######################## LOGGING ##############################
###############################################################

def reset_logs():
    REPORTS_ERROR_LOG.write_text("", encoding="utf-8")
    REPORTS_SUMMARY_LOG.write_text("", encoding="utf-8")


def log_error(msg):
    with open(REPORTS_ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {msg}\n")


def log_summary(msg):
    with open(REPORTS_SUMMARY_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {msg}\n")


###############################################################
######################## HELPERS ##############################
###############################################################

def safe_read(path):
    try:
        path = Path(path)

        if not path.exists():
            log_error(f"MISSING FILE | {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        if df is None or df.empty:
            log_error(f"EMPTY FILE | {path}")
            return pd.DataFrame()

        return df

    except Exception as e:
        log_error(f"READ ERROR | {path} | {e}")
        return pd.DataFrame()


def write_csv(df, path):
    try:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        log_summary(f"WROTE CSV | ROWS={len(df)} | OUT={path}")
    except Exception as e:
        log_error(f"WRITE CSV ERROR | {path} | {e}")


###############################################################
######################## SUMMARY CORE #########################
###############################################################

def summarize(df):
    if df is None or df.empty or "bet_result" not in df.columns:
        return 0, 0, 0, 0, 0.0

    wins = int((df["bet_result"] == "Win").sum())
    losses = int((df["bet_result"] == "Loss").sum())
    pushes = int((df["bet_result"] == "Push").sum())
    total = wins + losses + pushes
    win_pct = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0

    return wins, losses, pushes, total, round(win_pct, 4)


def aggregate_results(df, group_cols):
    if df is None or df.empty:
        return pd.DataFrame()

    rows = []

    grouped = df.groupby(group_cols, dropna=False)
    for keys, sub in grouped:
        wins, losses, pushes, total, win_pct = summarize(sub)

        if not isinstance(keys, tuple):
            keys = (keys,)

        row = {}
        for i, col in enumerate(group_cols):
            row[col] = keys[i]

        row["Win"] = wins
        row["Loss"] = losses
        row["Push"] = pushes
        row["Total"] = total
        row["Win_Pct"] = win_pct
        rows.append(row)

    return pd.DataFrame(rows)


###############################################################
######################## REPORT BUILDERS ######################
###############################################################

def build_moneyline_outputs(work, league, outdir):
    try:
        ml = work[work["market_type"] == "moneyline"].copy()
        if ml.empty:
            log_summary(f"{league} MONEYLINE EMPTY")
            return

        df1 = aggregate_results(
            ml[(ml["side_group"].isin(["HOME", "AWAY"])) & (ml["edge_bucket"] != "")],
            ["market", "side_group", "edge_bucket"],
        )
        write_csv(df1, outdir / "moneyline_edge_bucket_home_away_summary.csv")

        df2 = aggregate_results(
            ml[ml["edge_bucket"] != ""],
            ["market", "edge_bucket"],
        )
        write_csv(df2, outdir / "moneyline_edge_bucket_summary.csv")

        df3 = aggregate_results(
            ml[(ml["side_group"].isin(["HOME", "AWAY"])) & (ml["odds_bucket"] != "")],
            ["market", "side_group", "odds_bucket"],
        )
        write_csv(df3, outdir / "moneyline_odds_bucket_home_away_summary.csv")

        df4 = aggregate_results(
            ml[ml["odds_bucket"] != ""],
            ["market", "odds_bucket"],
        )
        write_csv(df4, outdir / "moneyline_odds_bucket_summary.csv")

        df5 = aggregate_results(
            ml[
                (ml["side_group"].isin(["HOME", "AWAY"]))
                & (ml["odds_bucket"] != "")
                & (ml["edge_bucket"] != "")
            ],
            ["market", "market_type", "side_group", "odds_bucket", "edge_bucket"],
        )
        write_csv(df5, outdir / "moneyline_summary.csv")

        log_summary(f"{league} MONEYLINE REPORTS COMPLETE | SOURCE_ROWS={len(ml)}")

    except Exception as e:
        log_error(f"{league} MONEYLINE REPORT BUILD ERROR | {e}")


def build_spread_outputs(work, league, outdir):
    try:
        sp = work[work["market_type"] == "spread"].copy()
        if sp.empty:
            log_summary(f"{league} SPREAD EMPTY")
            return

        df1 = aggregate_results(
            sp[(sp["side_group"].isin(["HOME", "AWAY"])) & (sp["edge_bucket"] != "")],
            ["market", "side_group", "edge_bucket"],
        )
        write_csv(df1, outdir / "spread_edge_bucket_home_away_summary.csv")

        df2 = aggregate_results(
            sp[sp["edge_bucket"] != ""],
            ["market", "edge_bucket"],
        )
        write_csv(df2, outdir / "spread_edge_bucket_summary.csv")

        df3 = aggregate_results(
            sp[(sp["side_group"].isin(["HOME", "AWAY"])) & (sp["spread_bucket"] != "")],
            ["market", "side_group", "spread_bucket"],
        )
        write_csv(df3, outdir / "spread_bands_bucket_home_away_summary.csv")

        df4 = aggregate_results(
            sp[sp["spread_bucket"] != ""],
            ["market", "spread_bucket"],
        )
        write_csv(df4, outdir / "spread_bands_bucket_summary.csv")

        df5 = aggregate_results(
            sp[
                (sp["side_group"].isin(["HOME", "AWAY"]))
                & (sp["spread_bucket"] != "")
                & (sp["edge_bucket"] != "")
            ],
            ["market", "market_type", "side_group", "spread_bucket", "edge_bucket"],
        )
        write_csv(df5, outdir / "spread_summary.csv")

        log_summary(f"{league} SPREAD REPORTS COMPLETE | SOURCE_ROWS={len(sp)}")

    except Exception as e:
        log_error(f"{league} SPREAD REPORT BUILD ERROR | {e}")


def build_total_outputs(work, league, outdir):
    try:
        tot = work[work["market_type"] == "total"].copy()
        if tot.empty:
            log_summary(f"{league} TOTAL EMPTY")
            return

        df1 = aggregate_results(
            tot[(tot["side_group"].isin(["OVER", "UNDER"])) & (tot["edge_bucket"] != "")],
            ["market", "side_group", "edge_bucket"],
        )
        write_csv(df1, outdir / "total_edge_bucket_home_away_summary.csv")

        df2 = aggregate_results(
            tot[tot["edge_bucket"] != ""],
            ["market", "edge_bucket"],
        )
        write_csv(df2, outdir / "total_edge_bucket_summary.csv")

        df3 = aggregate_results(
            tot[(tot["side_group"].isin(["OVER", "UNDER"])) & (tot["total_bucket"] != "")],
            ["market", "side_group", "total_bucket"],
        )
        write_csv(df3, outdir / "total_bands_bucket_home_away_summary.csv")

        df4 = aggregate_results(
            tot[tot["total_bucket"] != ""],
            ["market", "total_bucket"],
        )
        write_csv(df4, outdir / "total_bands_bucket_summary.csv")

        df5 = aggregate_results(
            tot[
                (tot["side_group"].isin(["OVER", "UNDER"]))
                & (tot["total_bucket"] != "")
                & (tot["edge_bucket"] != "")
            ],
            ["market", "market_type", "side_group", "total_bucket", "edge_bucket"],
        )
        write_csv(df5, outdir / "total_summary.csv")

        log_summary(f"{league} TOTAL REPORTS COMPLETE | SOURCE_ROWS={len(tot)}")

    except Exception as e:
        log_error(f"{league} TOTAL REPORT BUILD ERROR | {e}")


def build_market_tally(df, league):
    try:
        out_rows = []

        for market_type in ["moneyline", "spread", "total"]:
            sub = df[df["market_type"] == market_type]
            w, l, p, t, pct = summarize(sub)

            out_rows.append({
                "market": league,
                "market_type": market_type,
                "Win": w,
                "Loss": l,
                "Push": p,
                "Total": t,
                "Win_Pct": pct,
            })

        out = pd.DataFrame(out_rows)

        if league == "NBA":
            write_csv(out, NBA_MARKET_TALLY)
        else:
            write_csv(out, NCAAB_MARKET_TALLY)

        log_summary(f"{league} MARKET TALLY COMPLETE")

    except Exception as e:
        log_error(f"{league} MARKET TALLY ERROR | {e}")


###############################################################
######################## EDGE REPORT ##########################
###############################################################

def build_edge_report():
    try:
        rows = []

        for league, path in [
            ("NBA", INTERMEDIATE_DIR / "work_nba.csv"),
            ("NCAAB", INTERMEDIATE_DIR / "work_ncaab.csv"),
        ]:
            df = safe_read(path)
            if df.empty:
                continue

            win_edges = df.loc[df["bet_result"] == "Win", "selected_edge"].dropna().tolist()
            loss_edges = df.loc[df["bet_result"] == "Loss", "selected_edge"].dropna().tolist()

            win_avg = sum(win_edges) / len(win_edges) if win_edges else 0
            loss_avg = sum(loss_edges) / len(loss_edges) if loss_edges else 0

            rows.append("")
            rows.append(league)
            rows.append(f"Win edge avg: {win_avg:.4f}")
            rows.append(f"Loss edge avg: {loss_avg:.4f}")
            rows.append(f"Signal: {'CORRECT' if win_avg > loss_avg else 'INVERTED'}")

        with open(EDGE_REPORT, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(r + "\n")

        log_summary(f"WROTE EDGE REPORT | {EDGE_REPORT}")

    except Exception as e:
        log_error(f"EDGE REPORT ERROR | {e}")


###############################################################
######################## MAIN #################################
###############################################################

def main():
    reset_logs()
    log_summary("START basketball_results_reports.py")

    for league in ["NBA", "NCAAB"]:
        if league == "NBA":
            path = INTERMEDIATE_DIR / "work_nba.csv"
            outdir = NBA_DEEP_DIR
        else:
            path = INTERMEDIATE_DIR / "work_ncaab.csv"
            outdir = NCAAB_DEEP_DIR

        df = safe_read(path)

        if df.empty:
            log_error(f"{league} WORK FILE EMPTY | {path}")
            continue

        build_market_tally(df, league)
        build_moneyline_outputs(df, league, outdir)
        build_spread_outputs(df, league, outdir)
        build_total_outputs(df, league, outdir)

    build_edge_report()

    log_summary("END basketball_results_reports.py")
    print("Basketball reports generated.")


if __name__ == "__main__":
    main()
