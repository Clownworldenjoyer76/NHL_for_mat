#!/usr/bin/env python3
import sys
import os
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data/nhl")
OUT_DIR = Path("outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def summarize_csv(path: Path):
    try:
        df = pd.read_csv(path)
        return {
            "file": str(path.relative_to(DATA_DIR)),
            "rows": len(df),
            "cols": len(df.columns),
            "columns": ", ".join(map(str, df.columns[:20])) + ("..." if len(df.columns) > 20 else "")
        }
    except Exception as e:
        return {
            "file": str(path.relative_to(DATA_DIR)),
            "rows": None,
            "cols": None,
            "columns": f"ERROR: {e}"
        }

def main():
    if not DATA_DIR.exists():
        print(f"ERROR: {DATA_DIR} not found. Commit your unzipped data there.")
        sys.exit(1)

    csvs = sorted(DATA_DIR.rglob("*.csv"))
    tsvs = sorted(DATA_DIR.rglob("*.tsv"))

    summaries = []

    for p in csvs:
        summaries.append(summarize_csv(p))

    for p in tsvs:
        # read TSV
        try:
            df = pd.read_csv(p, sep="\t")
            summaries.append({
                "file": str(p.relative_to(DATA_DIR)),
                "rows": len(df),
                "cols": len(df.columns),
                "columns": ", ".join(map(str, df.columns[:20])) + ("..." if len(df.columns) > 20 else "")
            })
        except Exception as e:
            summaries.append({
                "file": str(p.relative_to(DATA_DIR)),
                "rows": None,
                "cols": None,
                "columns": f"ERROR: {e}"
            })

    # Write summary
    summary_df = pd.DataFrame(summaries)
    summary_csv = OUT_DIR / "nhl_file_summary.csv"
    summary_df.to_csv(summary_csv, index=False)

    print(f"Wrote summary to {summary_csv.resolve()}")
    print("Done.")

if __name__ == "__main__":
    main()
