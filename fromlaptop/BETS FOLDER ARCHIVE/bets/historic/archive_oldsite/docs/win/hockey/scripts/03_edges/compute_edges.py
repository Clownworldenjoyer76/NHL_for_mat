#!/usr/bin/env python3
# docs/win/hockey/scripts/03_edges/compute_edges.py

import pandas as pd
from pathlib import Path
from datetime import datetime
import traceback

INPUT_DIR = Path("docs/win/hockey/02_juice")
OUTPUT_DIR = Path("docs/win/hockey/03_edges")
ERROR_DIR = Path("docs/win/hockey/errors/03_edges")
ERROR_LOG = ERROR_DIR / "compute_edges.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


def validate_columns(df: pd.DataFrame, required_cols: list[str]) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def decimal_to_american(series: pd.Series) -> pd.Series:
    dec = pd.to_numeric(series, errors="coerce")
    american = pd.Series(index=dec.index, dtype="float64")

    pos = dec >= 2
    neg = (dec < 2) & (dec > 1)

    american[pos] = (dec[pos] - 1) * 100
    american[neg] = -100 / (dec[neg] - 1)

    return american.round(0)


def safe_edge_decimal(dk_decimal: pd.Series, fair_decimal: pd.Series) -> pd.Series:
    """
    Returns expected value (EV) per $1 staked using market decimal price and fair probability:
      EV = p_fair * dk_decimal - 1
    Positive => +EV.
    """
    dk_num = pd.to_numeric(dk_decimal, errors="coerce")
    fair_num = pd.to_numeric(fair_decimal, errors="coerce")

    dk_ok = dk_num > 1
    fair_ok = fair_num > 1

    fair_prob = (1 / fair_num).where(fair_ok)
    ev = (fair_prob * dk_num - 1).where(dk_ok & fair_ok)
    return ev


def safe_edge_pct(dk_decimal: pd.Series, fair_decimal: pd.Series) -> pd.Series:
    """
    Returns probability edge:
      edge_pct = p_fair - p_market
    Positive => fair probability higher than implied market probability.
    """
    dk_num = pd.to_numeric(dk_decimal, errors="coerce")
    fair_num = pd.to_numeric(fair_decimal, errors="coerce")

    dk_ok = dk_num > 1
    fair_ok = fair_num > 1

    p_market = (1 / dk_num).where(dk_ok)
    p_fair = (1 / fair_num).where(fair_ok)

    edge = (p_fair - p_market).where(dk_ok & fair_ok)
    return edge


def upsert_dedup_by_game_id(new_df: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    if output_path.exists():
        existing = pd.read_csv(output_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
        if "game_id" in combined.columns:
            combined = combined.drop_duplicates(subset=["game_id"], keep="last")
        return combined
    return new_df


def atomic_write_csv(df: pd.DataFrame, output_path: Path) -> None:
    tmp = output_path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(output_path)


def compute_moneyline_edges(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "game_id",
        "home_dk_decimal_moneyline",
        "away_dk_decimal_moneyline",
        "home_juiced_decimal_moneyline",
        "away_juiced_decimal_moneyline",
    ]
    validate_columns(df, required)

    df["home_juiced_american_moneyline"] = decimal_to_american(df["home_juiced_decimal_moneyline"])
    df["away_juiced_american_moneyline"] = decimal_to_american(df["away_juiced_decimal_moneyline"])

    df["home_edge_decimal"] = safe_edge_decimal(
        df["home_dk_decimal_moneyline"],
        df["home_juiced_decimal_moneyline"],
    )
    df["home_edge_pct"] = safe_edge_pct(
        df["home_dk_decimal_moneyline"],
        df["home_juiced_decimal_moneyline"],
    )
    df["home_play"] = df["home_edge_decimal"] > 0

    df["away_edge_decimal"] = safe_edge_decimal(
        df["away_dk_decimal_moneyline"],
        df["away_juiced_decimal_moneyline"],
    )
    df["away_edge_pct"] = safe_edge_pct(
        df["away_dk_decimal_moneyline"],
        df["away_juiced_decimal_moneyline"],
    )
    df["away_play"] = df["away_edge_decimal"] > 0

    df = df.drop_duplicates(subset=["game_id"], keep="last")
    return df


def compute_puck_line_edges(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "game_id",
        "home_dk_puck_line_decimal",
        "away_dk_puck_line_decimal",
        "home_juiced_decimal_puck_line",
        "away_juiced_decimal_puck_line",
    ]
    validate_columns(df, required)

    df["home_juiced_american_puck_line"] = decimal_to_american(df["home_juiced_decimal_puck_line"])
    df["away_juiced_american_puck_line"] = decimal_to_american(df["away_juiced_decimal_puck_line"])

    df["home_edge_decimal"] = safe_edge_decimal(
        df["home_dk_puck_line_decimal"],
        df["home_juiced_decimal_puck_line"],
    )
    df["home_edge_pct"] = safe_edge_pct(
        df["home_dk_puck_line_decimal"],
        df["home_juiced_decimal_puck_line"],
    )
    df["home_play"] = df["home_edge_decimal"] > 0

    df["away_edge_decimal"] = safe_edge_decimal(
        df["away_dk_puck_line_decimal"],
        df["away_juiced_decimal_puck_line"],
    )
    df["away_edge_pct"] = safe_edge_pct(
        df["away_dk_puck_line_decimal"],
        df["away_juiced_decimal_puck_line"],
    )
    df["away_play"] = df["away_edge_decimal"] > 0

    df = df.drop_duplicates(subset=["game_id"], keep="last")
    return df


def compute_total_edges(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "game_id",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
        "juiced_total_over_decimal",
        "juiced_total_under_decimal",
    ]
    validate_columns(df, required)

    df["juiced_total_over_american"] = decimal_to_american(df["juiced_total_over_decimal"])
    df["juiced_total_under_american"] = decimal_to_american(df["juiced_total_under_decimal"])

    df["over_edge_decimal"] = safe_edge_decimal(
        df["dk_total_over_decimal"],
        df["juiced_total_over_decimal"],
    )
    df["over_edge_pct"] = safe_edge_pct(
        df["dk_total_over_decimal"],
        df["juiced_total_over_decimal"],
    )
    df["over_play"] = df["over_edge_decimal"] > 0

    df["under_edge_decimal"] = safe_edge_decimal(
        df["dk_total_under_decimal"],
        df["juiced_total_under_decimal"],
    )
    df["under_edge_pct"] = safe_edge_pct(
        df["dk_total_under_decimal"],
        df["juiced_total_under_decimal"],
    )
    df["under_play"] = df["under_edge_decimal"] > 0

    df = df.drop_duplicates(subset=["game_id"], keep="last")
    return df


def process_pattern(log, pattern: str, compute_fn, label: str, summary: dict) -> None:
    input_files = sorted(INPUT_DIR.glob(pattern))
    if not input_files:
        log.write(f"No input files found for pattern: {pattern}\n")
        return

    for input_path in input_files:
        df = pd.read_csv(input_path)
        out_df = compute_fn(df)

        output_path = OUTPUT_DIR / input_path.name
        final_df = upsert_dedup_by_game_id(out_df, output_path)
        atomic_write_csv(final_df, output_path)

        log.write(f"Wrote {output_path} | rows={len(final_df)}\n")
        summary["files_processed"] += 1
        summary["rows_processed"] += len(out_df)
        summary[f"{label}_files"] += 1


def main() -> None:
    with open(ERROR_LOG, "w") as log:
        log.write("=== NHL COMPUTE EDGES RUN ===\n")
        log.write(f"Timestamp: {datetime.utcnow().isoformat()}Z\n\n")

        summary = {
            "files_processed": 0,
            "rows_processed": 0,
            "moneyline_files": 0,
            "puck_line_files": 0,
            "total_files": 0,
        }

        try:
            process_pattern(log, "*_NHL_moneyline.csv", compute_moneyline_edges, "moneyline", summary)
            process_pattern(log, "*_NHL_puck_line.csv", compute_puck_line_edges, "puck_line", summary)
            process_pattern(log, "*_NHL_total.csv", compute_total_edges, "total", summary)

            log.write("\n=== SUMMARY ===\n")
            log.write(f"Files processed: {summary['files_processed']}\n")
            log.write(f"Rows processed (this run, pre-upsert): {summary['rows_processed']}\n")
            log.write(f"Moneyline files: {summary['moneyline_files']}\n")
            log.write(f"Puck line files: {summary['puck_line_files']}\n")
            log.write(f"Total files: {summary['total_files']}\n")

        except Exception as e:
            log.write("\n=== ERROR ===\n")
            log.write(str(e) + "\n\n")
            log.write(traceback.format_exc())


if __name__ == "__main__":
    main()
