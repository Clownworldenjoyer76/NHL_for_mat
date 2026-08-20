#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/03_edges/compute_ev_kelly.py

import traceback
from datetime import datetime, UTC
from pathlib import Path

import numpy as np
import pandas as pd

INPUT_DIR = Path("docs/win/hockey/nhl/03_edges")
OUTPUT_DIR = Path("docs/win/hockey/nhl/03_edges/ev_kelly")
ERROR_DIR = Path("docs/win/hockey/nhl/errors/03_edges")
LOG_FILE = ERROR_DIR / "compute_ev_kelly.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# LOGGING
# =========================

def _now():
    return datetime.now(UTC).isoformat()


def _log(msg: str, level: str = "INFO"):
    with open(LOG_FILE, "a", encoding="utf-8") as log_f:
        log_f.write(f"{_now()} | {level:<5} | {msg.rstrip()}\n")


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
        f"  neg_kelly_clipped: {summary['neg_kelly_clipped']}",
        f"  schema_errors    : {summary['schema_errors']}",
        f"  errors           : {summary['errors']}",
        "",
        f"  {'file':<48} {'market':<12} {'rows':>5} {'neg_kelly':>10} {'status':>12}",
    ]

    for pf in per_file:
        lines.append(
            f"  {pf['name']:<48} {pf['market']:<12} {pf['rows']:>5} "
            f"{pf['neg_kelly']:>10} {pf['status']:>12}"
        )

    status = (
        "SUCCESS"
        if summary["errors"] == 0 and summary["schema_errors"] == 0
        else "COMPLETED WITH ERRORS"
    )

    lines += ["", f"STATUS: {status}", "=" * 60]

    with open(LOG_FILE, "a", encoding="utf-8") as log_f:
        log_f.write("\n".join(lines) + "\n")


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


def compute_ev(model_prob, book_decimal):
    p = pd.to_numeric(model_prob, errors="coerce")
    d = pd.to_numeric(book_decimal, errors="coerce")

    out = pd.Series(np.nan, index=p.index, dtype="float64")
    valid = (
        d.notna()
        & p.notna()
        & np.isfinite(d)
        & np.isfinite(p)
        & (d > 1)
        & (p > 0)
        & (p < 1)
    )

    out.loc[valid] = (p.loc[valid] * d.loc[valid]) - 1
    return out


def compute_kelly(model_prob, book_decimal, file_name=""):
    p = pd.to_numeric(model_prob, errors="coerce")
    d = pd.to_numeric(book_decimal, errors="coerce")

    b = d - 1
    q = 1 - p

    k = pd.Series(np.nan, index=p.index, dtype="float64")
    valid = (
        b.notna()
        & p.notna()
        & np.isfinite(b)
        & np.isfinite(p)
        & (b > 0)
        & (p > 0)
        & (p < 1)
    )

    k.loc[valid] = ((b.loc[valid] * p.loc[valid]) - q.loc[valid]) / b.loc[valid]

    neg = k[k.notna() & (k < 0)]

    if not neg.empty:
        _log(
            f"{file_name} | {len(neg)} negative Kelly values clipped to 0 "
            f"(min={neg.min():.4f})",
            "WARN",
        )

    k = k.clip(lower=0)

    return k, len(neg)


# =========================
# MONEYLINE
# =========================

def process_moneyline(df, file_path):
    required_cols = [
        "game_id",
        "away_model_prob_moneyline",
        "home_model_prob_moneyline",
        "away_dk_moneyline_decimal",
        "home_dk_moneyline_decimal",
        "away_edge_decimal_moneyline",
        "home_edge_decimal_moneyline",
        "away_edge_pct_moneyline",
        "home_edge_pct_moneyline",
    ]
    validate_columns(df, required_cols, file_path)

    df = to_numeric(df, required_cols[1:])

    df["away_ev_moneyline"] = compute_ev(
        df["away_model_prob_moneyline"],
        df["away_dk_moneyline_decimal"],
    )
    df["home_ev_moneyline"] = compute_ev(
        df["home_model_prob_moneyline"],
        df["home_dk_moneyline_decimal"],
    )

    away_kelly, a_neg = compute_kelly(
        df["away_model_prob_moneyline"],
        df["away_dk_moneyline_decimal"],
        file_path.name,
    )
    home_kelly, h_neg = compute_kelly(
        df["home_model_prob_moneyline"],
        df["home_dk_moneyline_decimal"],
        file_path.name,
    )

    df["away_kelly_moneyline"] = away_kelly
    df["home_kelly_moneyline"] = home_kelly

    return df, a_neg + h_neg


# =========================
# PUCK LINE
# =========================

def process_puck_line(df, file_path):
    required_cols = [
        "game_id",
        "away_model_prob_puck_line",
        "home_model_prob_puck_line",
        "away_dk_puck_line_decimal",
        "home_dk_puck_line_decimal",
        "away_edge_decimal_puck_line",
        "home_edge_decimal_puck_line",
        "away_edge_pct_puck_line",
        "home_edge_pct_puck_line",
    ]
    validate_columns(df, required_cols, file_path)

    df = to_numeric(df, required_cols[1:])

    df["away_ev_puck_line"] = compute_ev(
        df["away_model_prob_puck_line"],
        df["away_dk_puck_line_decimal"],
    )
    df["home_ev_puck_line"] = compute_ev(
        df["home_model_prob_puck_line"],
        df["home_dk_puck_line_decimal"],
    )

    away_kelly, a_neg = compute_kelly(
        df["away_model_prob_puck_line"],
        df["away_dk_puck_line_decimal"],
        file_path.name,
    )
    home_kelly, h_neg = compute_kelly(
        df["home_model_prob_puck_line"],
        df["home_dk_puck_line_decimal"],
        file_path.name,
    )

    df["away_kelly_puck_line"] = away_kelly
    df["home_kelly_puck_line"] = home_kelly

    return df, a_neg + h_neg


# =========================
# TOTAL
# =========================

def process_total(df, file_path):
    required_cols = [
        "game_id",
        "over_model_prob_total",
        "under_model_prob_total",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
        "over_edge_decimal_total",
        "under_edge_decimal_total",
        "over_edge_pct_total",
        "under_edge_pct_total",
    ]
    validate_columns(df, required_cols, file_path)

    df = to_numeric(df, required_cols[1:])

    df["over_ev_total"] = compute_ev(
        df["over_model_prob_total"],
        df["dk_total_over_decimal"],
    )
    df["under_ev_total"] = compute_ev(
        df["under_model_prob_total"],
        df["dk_total_under_decimal"],
    )

    over_kelly, o_neg = compute_kelly(
        df["over_model_prob_total"],
        df["dk_total_over_decimal"],
        file_path.name,
    )
    under_kelly, u_neg = compute_kelly(
        df["under_model_prob_total"],
        df["dk_total_under_decimal"],
        file_path.name,
    )

    df["over_kelly_total"] = over_kelly
    df["under_kelly_total"] = under_kelly

    return df, o_neg + u_neg


# =========================
# DRIVER
# =========================

def process_pattern(pattern, process_fn, market_label, summary, per_file):
    input_files = sorted(INPUT_DIR.glob(pattern))

    if not input_files:
        _log(f"No input files found for pattern: {pattern}", "WARN")
        return

    for input_path in input_files:
        pf = {
            "name": input_path.name,
            "market": market_label,
            "rows": 0,
            "neg_kelly": 0,
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

            out_df, neg_kelly = process_fn(df, input_path)

            pf["neg_kelly"] = neg_kelly
            summary["neg_kelly_clipped"] += neg_kelly

            output_path = OUTPUT_DIR / input_path.name
            atomic_write_csv(out_df, output_path)

            summary["files_processed"] += 1
            summary[f"{market_label}_files"] += 1

            _log(f"WROTE: {output_path} ({len(out_df)} rows, {neg_kelly} kelly values clipped)")

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
    with open(LOG_FILE, "w", encoding="utf-8") as log_f:
        log_f.write(f"=== compute_ev_kelly RUN {_now()} ===\n")

    summary = {
        "files_processed": 0,
        "rows_processed": 0,
        "moneyline_files": 0,
        "puck_line_files": 0,
        "total_files": 0,
        "skipped": 0,
        "neg_kelly_clipped": 0,
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
            process_moneyline,
            "moneyline",
            summary,
            per_file,
        )
        process_pattern(
            "*_NHL_puck_line.csv",
            process_puck_line,
            "puck_line",
            summary,
            per_file,
        )
        process_pattern(
            "*_NHL_total.csv",
            process_total,
            "total",
            summary,
            per_file,
        )

    except Exception as e:
        _log(f"FATAL: {e}\n{traceback.format_exc()}", "ERROR")
        _write_summary(summary, per_file)
        raise

    _write_summary(summary, per_file)
    print("compute_ev_kelly complete.")


if __name__ == "__main__":
    main()
