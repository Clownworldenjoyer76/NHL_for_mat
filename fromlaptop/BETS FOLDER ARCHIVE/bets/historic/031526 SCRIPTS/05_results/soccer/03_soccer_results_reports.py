#!/usr/bin/env python3
# docs/win/final_scores/scripts/05_results/soccer/03_soccer_results_reports.py

from datetime import datetime
from pathlib import Path
import pandas as pd

INTERMEDIATE = Path("docs/win/final_scores/intermediate/work_soccer.csv")

SUMMARY_DIR = Path("docs/win/final_scores/deeper_summaries/soccer")
SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/final_scores/errors")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

MARKET_TALLY = Path("docs/win/final_scores/soccer_market_tally.csv")

ERROR_LOG = ERROR_DIR / "soccer_results_reports_errors.txt"

###############################################################

def summarize(df):

    wins = int((df["bet_result"]=="Win").sum())
    losses = int((df["bet_result"]=="Loss").sum())
    pushes = int((df["bet_result"]=="Push").sum())

    total = wins+losses+pushes
    pct = wins/(wins+losses) if wins+losses>0 else 0

    return wins,losses,pushes,total,round(pct,4)

###############################################################

def aggregate(df,cols):

    rows=[]

    for keys,sub in df.groupby(cols):

        if not isinstance(keys,tuple):
            keys=(keys,)

        w,l,p,t,pct=summarize(sub)

        r={}

        for i,c in enumerate(cols):
            r[c]=keys[i]

        r["Win"]=w
        r["Loss"]=l
        r["Push"]=p
        r["Total"]=t
        r["Win_Pct"]=pct

        rows.append(r)

    return pd.DataFrame(rows)

###############################################################

def build_reports():

    if not INTERMEDIATE.exists():
        return

    df=pd.read_csv(INTERMEDIATE)

    if df.empty:
        return

    for market in ["result","total"]:

        sub=df[df["market_type"]==market]

        if sub.empty:
            continue

        out=aggregate(sub,["market","market_type","edge_bucket"])
        out.to_csv(SUMMARY_DIR/f"{market}_edge_bucket_summary.csv",index=False)

        out=aggregate(sub,["market","market_type","odds_bucket"])
        out.to_csv(SUMMARY_DIR/f"{market}_odds_bucket_summary.csv",index=False)

###############################################################

def build_market_tally():

    if not INTERMEDIATE.exists():
        return

    df=pd.read_csv(INTERMEDIATE)

    rows=[]

    for m in ["result","total"]:

        sub=df[df["market_type"]==m]

        w,l,p,t,pct=summarize(sub)

        rows.append({
            "market":"SOCCER",
            "market_type":m,
            "Win":w,
            "Loss":l,
            "Push":p,
            "Total":t,
            "Win_Pct":pct
        })

    pd.DataFrame(rows).to_csv(MARKET_TALLY,index=False)

###############################################################

def main():

    build_reports()
    build_market_tally()

    print("Soccer reports generated.")

if __name__=="__main__":
    main()
