#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/03_edges/compute_edges.py

import traceback
from datetime import datetime, UTC
from pathlib import Path

import numpy as np
import pandas as pd

INPUT_DIR = Path("docs/win/hockey/nhl/02_juice")
OUTPUT_DIR = Path("docs/win/hockey/nhl/03_edges")
ERROR_DIR = Path("docs/win/hockey/nhl/errors/03_edges")
LOG_FILE = ERROR_DIR / "compute_edges.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# LOGGING
# =========================

def _now():
    return datetime.now(UTC).isoformat()


def _log(msg: str, level: str = "INFO"):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {msg.rstrip()}\n")


def _write_summary(summary: dict, per_file: list) -> None:
    lines = [
        "",
        "=" * 60,
        f"SUMMARY  {_now()}",
        "=" * 60,
        f"  files_processed  : {summary['files_processed']}",
        f"  rows_processed   : {summary['rows_processed']}",
        f"  moneyline_files  : {summary['moneyline_files']}",
        f"  puck_line_files  : {summary['puck_line_files']}",
        f"  total_files      : {summary['total_files']}",
        f"  skipped          : {summary['skipped']}",
        f"  null_edges       : {summary['null_edges']}",
        f"  schema_errors    : {summary['schema_errors']}",
        f"  errors           : {summary['errors']}",
        "",
        f"  {'file':<48} {'market':<12} {'rows':>5} {'null_edges':>10} {'status':>12}",
    ]

    for pf in per_file:
        lines.append(
            f"  {pf['name']:<48} {pf['market']:<12} {pf['rows']:>5} "
            f"{pf['null_edges']:>10} {pf['status']:>12}"
        )

    status = (
        "SUCCESS"
        if summary["errors"] == 0 and summary["schema_errors"] == 0
        else "COMPLETED WITH ERRORS"
    )

    lines += ["", f"STATUS: {status}", "=" * 60]

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# =========================
# HELPERS
# =========================

def validate_columns(df, required_cols, file_path):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{file_path.name} missing required columns: {missing}")


def atomic_write_csv(df, output_path):
    tmp = output_path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(output_path)


def to_numeric(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def safe_edge_decimal(book_decimal, model_prob):
    d = pd.to_numeric(book_decimal, errors="coerce")
    p = pd.to_numeric(model_prob, errors="coerce")

    out = pd.Series(np.nan, index=p.index, dtype="float64")
    valid = d.notna() & p.notna() & np.isfinite(d) & np.isfinite(p) & (d > 1) & (p > 0) & (p < 1)

    out.loc[valid] = (p.loc[valid] * d.loc[valid]) - 1
    return out


def safe_edge_pct(book_decimal, model_prob):
    d = pd.to_numeric(book_decimal, errors="coerce")
    p = pd.to_numeric(model_prob, errors="coerce")

    out = pd.Series(np.nan, index=p.index, dtype="float64")
    valid = d.notna() & p.notna() & np.isfinite(d) & np.isfinite(p) & (d > 1) & (p > 0) & (p < 1)

    out.loc[valid] = p.loc[valid] - (1 / d.loc[valid])
    return out


def count_null_edges(df, cols):
    return sum(df[c].isna().sum() for c in cols if c in df.columns)


# =========================
# MONEYLINE
# =========================

def compute_moneyline_edges(df, file_path):
    required_cols = [
        "game_id",
        "away_normalized_prob_moneyline",
        "home_normalized_prob_moneyline",
        "away_dk_moneyline_decimal",
        "home_dk_moneyline_decimal",
    ]
    validate_columns(df, required_cols, file_path)

    df = to_numeric(df, required_cols[1:])

    df["away_model_prob_moneyline"] = df["away_normalized_prob_moneyline"]
    df["home_model_prob_moneyline"] = df["home_normalized_prob_moneyline"]

    df["away_edge_decimal_moneyline"] = safe_edge_decimal(
        df["away_dk_moneyline_decimal"],
        df["away_model_prob_moneyline"],
    )
    df["home_edge_decimal_moneyline"] = safe_edge_decimal(
        df["home_dk_moneyline_decimal"],
        df["home_model_prob_moneyline"],
    )

    df["away_edge_pct_moneyline"] = safe_edge_pct(
        df["away_dk_moneyline_decimal"],
        df["away_model_prob_moneyline"],
    )
    df["home_edge_pct_moneyline"] = safe_edge_pct(
        df["home_dk_moneyline_decimal"],
        df["home_model_prob_moneyline"],
    )

    null_edges = count_null_edges(
        df,
        [
            "away_edge_decimal_moneyline",
            "home_edge_decimal_moneyline",
            "away_edge_pct_moneyline",
            "home_edge_pct_moneyline",
        ],
    )

    return df, null_edges


# =========================
# PUCK LINE
# =========================

def compute_puck_line_edges(df, file_path):
    required_cols = [
        "game_id",
        "away_normalized_prob_puck_line",
        "home_normalized_prob_puck_line",
        "away_dk_puck_line_decimal",
        "home_dk_puck_line_decimal",
    ]
    validate_columns(df, required_cols, file_path)

    df = to_numeric(df, required_cols[1:])

    df["away_model_prob_puck_line"] = df["away_normalized_prob_puck_line"]
    df["home_model_prob_puck_line"] = df["home_normalized_prob_puck_line"]

    df["away_edge_decimal_puck_line"] = safe_edge_decimal(
        df["away_dk_puck_line_decimal"],
        df["away_model_prob_puck_line"],
    )
    df["home_edge_decimal_puck_line"] = safe_edge_decimal(
        df["home_dk_puck_line_decimal"],
        df["home_model_prob_puck_line"],
    )

    df["away_edge_pct_puck_line"] = safe_edge_pct(
        df["away_dk_puck_line_decimal"],
        df["away_model_prob_puck_line"],
    )
    df["home_edge_pct_puck_line"] = safe_edge_pct(
        df["home_dk_puck_line_decimal"],
        df["home_model_prob_puck_line"],
    )

    null_edges = count_null_edges(
        df,
        [
            "away_edge_decimal_puck_line",
            "home_edge_decimal_puck_line",
            "away_edge_pct_puck_line",
            "home_edge_pct_puck_line",
        ],
    )

    return df, null_edges


# =========================
# TOTAL
# =========================

def compute_total_edges(df, file_path):
    required_cols = [
        "game_id",
        "over_normalized_prob_total",
        "under_normalized_prob_total",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
    ]
    validate_columns(df, required_cols, file_path)

    df = to_numeric(df, required_cols[1:])

    df["over_model_prob_total"] = df["over_normalized_prob_total"]
    df["under_model_prob_total"] = df["under_normalized_prob_total"]

    df["over_edge_decimal_total"] = safe_edge_decimal(
        df["dk_total_over_decimal"],
        df["over_model_prob_total"],
    )
    df["under_edge_decimal_total"] = safe_edge_decimal(
        df["dk_total_under_decimal"],
        df["under_model_prob_total"],
    )

    df["over_edge_pct_total"] = safe_edge_pct(
        df["dk_total_over_decimal"],
        df["over_model_prob_total"],
    )
    df["under_edge_pct_total"] = safe_edge_pct(
        df["dk_total_under_decimal"],
        df["under_model_prob_total"],
    )

    null_edges = count_null_edges(
        df,
        [
            "over_edge_decimal_total",
            "under_edge_decimal_total",
            "over_edge_pct_total",
            "under_edge_pct_total",
        ],
    )

    return df, null_edges


# =========================
# DRIVER
# =========================

def process_pattern(pattern, compute_fn, market_label, summary, per_file):
    input_files = sorted(INPUT_DIR.glob(pattern))

    if not input_files:
        _log(f"No input files found for pattern: {pattern}", "WARN")
        return

    for input_path in input_files:
        pf = {
            "name": input_path.name,
            "market": market_label,
            "rows": 0,
            "null_edges": 0,
            "status": "ok",
        }

        _log(f"--- FILE: {input_path.name}  market={market_label}")

        try:
            df = pd.read_csv(input_path)

            if df.empty:
                _log(f"{input_path.name} empty — skipping")
                pf["status"] = "empty"
                summary["skipped"] += 1
                per_file.append(pf)
                continue

            pf["rows"] = len(df)
            summary["rows_processed"] += len(df)

            out_df, null_edges = compute_fn(df, input_path)

            pf["null_edges"] = null_edges
            summary["null_edges"] += null_edges

            if null_edges > 0:
                _log(f"{input_path.name} | {null_edges} null edge values", "WARN")

            output_path = OUTPUT_DIR / input_path.name
            atomic_write_csv(out_df, output_path)

            summary["files_processed"] += 1
            summary[f"{market_label}_files"] += 1

            _log(f"WROTE: {output_path} ({len(out_df)} rows, {null_edges} null edge values)")

        except ValueError as e:
            _log(f"{input_path.name} schema error: {e}", "ERROR")
            pf["status"] = "schema_error"
            summary["schema_errors"] += 1

        except Exception as e:
            _log(f"{input_path.name} FAILED: {e}\n{traceback.format_exc()}", "ERROR")
            pf["status"] = "error"
            summary["errors"] += 1

        per_file.append(pf)


# =========================
# MAIN
# =========================

def main():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== compute_edges RUN {_now()} ===\n")

    summary = {
        "files_processed": 0,
        "rows_processed": 0,
        "moneyline_files": 0,
        "puck_line_files": 0,
        "total_files": 0,
        "skipped": 0,
        "null_edges": 0,
        "schema_errors": 0,
        "errors": 0,
    }

    per_file = []

    _log(f"INPUT_DIR : {INPUT_DIR}")
    _log(f"OUTPUT_DIR: {OUTPUT_DIR}")

    for output_file in OUTPUT_DIR.glob("*.csv"):
        output_file.unlink()

    try:
        process_pattern(
            "*_NHL_moneyline.csv",
            compute_moneyline_edges,
            "moneyline",
            summary,
            per_file,
        )
        process_pattern(
            "*_NHL_puck_line.csv",
            compute_puck_line_edges,
            "puck_line",
            summary,
            per_file,
        )
        process_pattern(
            "*_NHL_total.csv",
            compute_total_edges,
            "total",
            summary,
            per_file,
        )

    except Exception as e:
        _log(f"FATAL: {e}\n{traceback.format_exc()}", "ERROR")
        _write_summary(summary, per_file)
        raise

    _write_summary(summary, per_file)
    print("compute_edges complete.")


if __name__ == "__main__":
    main()
