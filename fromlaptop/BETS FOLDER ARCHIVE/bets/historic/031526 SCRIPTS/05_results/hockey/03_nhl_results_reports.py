#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/nhl_results_reports.py

from datetime import datetime
from pathlib import Path
import pandas as pd

INTERMEDIATE_DIR = Path("docs/win/final_scores/intermediate")

DEEP_DIR = Path("docs/win/final_scores/deeper_summaries/nhl")
DEEP_DIR.mkdir(parents=True, exist_ok=True)

MARKET_TALLY = Path("docs/win/final_scores/nhl_market_tally.csv")

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

ERROR_LOG = ERROR_DIR / "nhl_results_reports_errors.txt"
SUMMARY_LOG = ERROR_DIR / "nhl_results_reports_summary.txt"
EDGE_REPORT = ERROR_DIR / "nhl_edge_summary.txt"

def reset_logs():

    ERROR_LOG.write_text("",encoding="utf-8")
    SUMMARY_LOG.write_text("",encoding="utf-8")

def log_error(msg):

    with open(ERROR_LOG,"a") as f:
        f.write(f"[{datetime.now()}] {msg}\n")

def log_summary(msg):

    with open(SUMMARY_LOG,"a") as f:
        f.write(f"[{datetime.now()}] {msg}\n")

def safe_read(path):

    try:

        path=Path(path)

        if not path.exists():
            return pd.DataFrame()

        df=pd.read_csv(path)

        return df

    except Exception:
        return pd.DataFrame()

def summarize(df):

    wins=int((df["bet_result"]=="Win").sum())
    losses=int((df["bet_result"]=="Loss").sum())
    pushes=int((df["bet_result"]=="Push").sum())

    total=wins+losses+pushes

    winpct=(wins/(wins+losses)) if (wins+losses)>0 else 0

    return wins,losses,pushes,total,round(winpct,4)

def build_market_tally(df):

    rows=[]

    for market in ["moneyline","puck_line","total"]:

        sub=df[df["market_type"]==market]

        w,l,p,t,pct=summarize(sub)

        rows.append({
            "market":"NHL",
            "market_type":market,
            "Win":w,
            "Loss":l,
            "Push":p,
            "Total":t,
            "Win_Pct":pct
        })

    out=pd.DataFrame(rows)

    out.to_csv(MARKET_TALLY,index=False)

def build_edge_report(df):

    win_edges=df.loc[df["bet_result"]=="Win","selected_edge"].dropna()
    loss_edges=df.loc[df["bet_result"]=="Loss","selected_edge"].dropna()

    win_avg=win_edges.mean() if not win_edges.empty else 0
    loss_avg=loss_edges.mean() if not loss_edges.empty else 0

    with open(EDGE_REPORT,"w") as f:

        f.write("NHL\n")
        f.write(f"Win edge avg: {win_avg:.4f}\n")
        f.write(f"Loss edge avg: {loss_avg:.4f}\n")
        f.write(f"Signal: {'CORRECT' if win_avg>loss_avg else 'INVERTED'}\n")

def main():

    reset_logs()

    df=safe_read(INTERMEDIATE_DIR/"work_nhl.csv")

    if df.empty:
        log_error("WORK FILE EMPTY")
        return

    build_market_tally(df)

    build_edge_report(df)

    log_summary("NHL REPORTS COMPLETE")

    print("NHL reports generated.")

if __name__=="__main__":
    main()
