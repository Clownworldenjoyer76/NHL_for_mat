#!/usr/bin/env python3
"""
Research-only provenance audit for active NHL Stage 02 probability-adjustment coefficients.

Reads:
  docs/win/hockey/nhl/config/juice/nhl_moneyline_juice.csv
  docs/win/hockey/nhl/config/juice/nhl_puck_line_juice.csv
  docs/win/hockey/nhl/config/juice/nhl_total_juice.csv

Historical sources:
  fromlaptop/BETS FOLDER ARCHIVE/bets/config/hockey/nhl/

Writes only:
  docs/win/hockey/nhl/research/stage02_coefficient_provenance/

No production config or Stage 02 script is modified.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd


NHL_REL = Path("docs/win/hockey/nhl")
ARCHIVE_REL = Path("fromlaptop/BETS FOLDER ARCHIVE/bets/config/hockey/nhl")

OUTPUT_COLUMNS = [
    "market",
    "current_band",
    "band_min",
    "band_max",
    "side",
    "venue",
    "fav_ud",
    "current_value",
    "historical_source_filepath",
    "historical_config_filepath",
    "historical_source_value",
    "historical_sample_size",
    "historical_wins",
    "historical_losses",
    "historical_pushes",
    "historical_profit",
    "historical_roi",
    "historical_win_rate",
    "historical_extra_juice",
    "mathematical_transformation",
    "provenance_classification",
    "direct_roi_or_profit_flag",
    "probabilistically_unsupported_flag",
    "independent_oos_probability_validation_found",
    "provenance_note",
]


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve()]
    seen: set[Path] = set()
    for start in starts:
        for candidate in [start, *start.parents]:
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / NHL_REL).is_dir() and (candidate / ARCHIVE_REL).is_dir():
                return candidate
    raise RuntimeError(
        f"Could not find repository root containing both {NHL_REL} and {ARCHIVE_REL}"
    )


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def num(value):
    try:
        out = float(value)
    except Exception:
        return np.nan
    return out if math.isfinite(out) else np.nan


def text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def close(a, b, atol=5e-5) -> bool:
    a = num(a)
    b = num(b)
    return bool(np.isfinite(a) and np.isfinite(b) and abs(a - b) <= atol)


def classify(active, roi, profit, win_rate, hist_extra):
    if np.isfinite(num(roi)) and close(active, roi):
        return (
            "ROI",
            "active_value = historical_roi",
            True,
            True,
            "Exact numeric match to historical ROI.",
        )
    if np.isfinite(num(roi)) and close(active, -num(roi)):
        return (
            "negative ROI",
            "active_value = -historical_roi",
            True,
            True,
            "Exact numeric match to sign-reversed historical ROI.",
        )
    if np.isfinite(num(profit)) and close(active, profit):
        return (
            "profit",
            "active_value = historical_profit",
            True,
            True,
            "Exact numeric match to historical profit.",
        )
    if np.isfinite(num(win_rate)) and close(active, win_rate):
        return (
            "empirical win rate",
            "active_value = historical_win_rate",
            False,
            True,
            "Exact numeric match to empirical win rate; no proper calibration fit established.",
        )
    if np.isfinite(num(hist_extra)) and close(active, hist_extra):
        return (
            "unknown provenance",
            "active_value = archived extra_juice; upstream derivation not demonstrated",
            False,
            True,
            "Active value copies archived extra_juice, but no exact ROI/profit/win-rate/calibration transformation is supported by the located source metrics.",
        )
    return (
        "unknown provenance",
        "no supported mathematical transformation located",
        False,
        True,
        "No exact supported derivation located in the requested historical source directory.",
    )


def clean_ml_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["band", "fav_ud", "venue"]:
        out[col] = out[col].map(text)
    for col in ["bets", "wins", "profit", "win_pct", "roi"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out[
        out["fav_ud"].isin(["favorite", "underdog"])
        & out["venue"].isin(["home", "away"])
        & out["bets"].notna()
        & out["wins"].notna()
    ].copy()
    return out


def ml_rows(root: Path) -> list[dict]:
    current_path = root / NHL_REL / "config/juice/nhl_moneyline_juice.csv"
    hist_config_path = root / ARCHIVE_REL / "nhl_moneyline_juice.csv"
    hist_metrics_path = root / ARCHIVE_REL / "nhl_data_ml_bands.csv"

    current = read_csv(current_path)
    hist_config = read_csv(hist_config_path)
    metrics = clean_ml_metrics(read_csv(hist_metrics_path))

    rows = []
    for _, r in current.iterrows():
        band = text(r["band"])
        fav_ud = text(r["fav_ud"])
        venue = text(r["venue"])
        active = num(r["model_calibration_adjustment"])

        hc = hist_config[
            (hist_config["band"].map(text) == band)
            & (hist_config["fav_ud"].map(text) == fav_ud)
            & (hist_config["venue"].map(text) == venue)
        ]
        hm = metrics[
            (metrics["band"] == band)
            & (metrics["fav_ud"] == fav_ud)
            & (metrics["venue"] == venue)
        ]

        hist_extra = num(hc.iloc[0]["extra_juice"]) if len(hc) == 1 else np.nan
        m = hm.iloc[0] if len(hm) == 1 else None

        bets = num(m["bets"]) if m is not None else np.nan
        wins = num(m["wins"]) if m is not None else np.nan
        profit = num(m["profit"]) if m is not None else np.nan
        win_rate = num(m["win_pct"]) if m is not None else np.nan
        roi = num(m["roi"]) if m is not None else np.nan

        losses = bets - wins if np.isfinite(bets) and np.isfinite(wins) else np.nan
        pushes = 0.0 if np.isfinite(bets) else np.nan

        classification, transform, direct, unsupported, note = classify(
            active, roi, profit, win_rate, hist_extra
        )

        if m is not None and classification in {"ROI", "negative ROI", "profit", "empirical win rate"}:
            source_path = hist_metrics_path
            source_value = {
                "ROI": roi,
                "negative ROI": roi,
                "profit": profit,
                "empirical win rate": win_rate,
            }[classification]
        elif len(hc) == 1:
            source_path = hist_config_path
            source_value = hist_extra
        else:
            source_path = hist_metrics_path if m is not None else hist_config_path
            source_value = np.nan

        rows.append(
            {
                "market": "moneyline",
                "current_band": band,
                "band_min": num(r["band_min"]),
                "band_max": num(r["band_max"]),
                "side": venue,
                "venue": venue,
                "fav_ud": fav_ud,
                "current_value": active,
                "historical_source_filepath": source_path.relative_to(root).as_posix(),
                "historical_config_filepath": hist_config_path.relative_to(root).as_posix(),
                "historical_source_value": source_value,
                "historical_sample_size": bets,
                "historical_wins": wins,
                "historical_losses": losses,
                "historical_pushes": pushes,
                "historical_profit": profit,
                "historical_roi": roi,
                "historical_win_rate": win_rate,
                "historical_extra_juice": hist_extra,
                "mathematical_transformation": transform,
                "provenance_classification": classification,
                "direct_roi_or_profit_flag": direct,
                "probabilistically_unsupported_flag": unsupported,
                "independent_oos_probability_validation_found": False,
                "provenance_note": note,
            }
        )
    return rows


def puck_rows(root: Path) -> list[dict]:
    current_path = root / NHL_REL / "config/juice/nhl_puck_line_juice.csv"
    hist_config_path = root / ARCHIVE_REL / "nhl_puck_line_juice.csv"
    hist_metrics_path = root / ARCHIVE_REL / "nhl_puck_runline_bands.csv"

    current = read_csv(current_path)
    hist_config = read_csv(hist_config_path)
    metrics = read_csv(hist_metrics_path)

    for col in ["band", "fav_ud", "venue"]:
        metrics[col] = metrics[col].map(text)
    for col in ["bets", "wins", "pushes", "profit", "decisions", "win_pct", "roi"]:
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce")

    rows = []
    for _, r in current.iterrows():
        band = text(r["band"])
        fav_ud = text(r["fav_ud"])
        venue = text(r["venue"])
        active = num(r["model_calibration_adjustment"])

        hc = hist_config[
            (hist_config["band"].map(text) == band)
            & (hist_config["fav_ud"].map(text) == fav_ud)
            & (hist_config["venue"].map(text) == venue)
        ]

        hm = metrics[
            (metrics["fav_ud"] == fav_ud)
            & (metrics["venue"] == venue)
        ]

        hist_extra = num(hc.iloc[0]["extra_juice"]) if len(hc) == 1 else np.nan
        m = hm.iloc[0] if len(hm) == 1 else None

        bets = num(m["bets"]) if m is not None else np.nan
        wins = num(m["wins"]) if m is not None else np.nan
        pushes = num(m["pushes"]) if m is not None else np.nan
        decisions = num(m["decisions"]) if m is not None else np.nan
        profit = num(m["profit"]) if m is not None else np.nan
        win_rate = num(m["win_pct"]) if m is not None else np.nan
        roi = num(m["roi"]) if m is not None else np.nan
        losses = (
            decisions - wins
            if np.isfinite(decisions) and np.isfinite(wins)
            else np.nan
        )

        classification, transform, direct, unsupported, note = classify(
            active, roi, profit, win_rate, hist_extra
        )

        if m is not None and classification in {"ROI", "negative ROI", "profit", "empirical win rate"}:
            source_path = hist_metrics_path
            source_value = {
                "ROI": roi,
                "negative ROI": roi,
                "profit": profit,
                "empirical win rate": win_rate,
            }[classification]
        elif len(hc) == 1:
            source_path = hist_config_path
            source_value = hist_extra
        else:
            source_path = hist_metrics_path if m is not None else hist_config_path
            source_value = np.nan

        if classification == "unknown provenance" and m is not None:
            note += (
                f" Historical ROI={roi:.4f}; active={active:.4f}. "
                "The values have related scale/sign in some rows but are not an exact supported transform."
            )

        rows.append(
            {
                "market": "puck_line",
                "current_band": band,
                "band_min": num(r["band_min"]),
                "band_max": num(r["band_max"]),
                "side": venue,
                "venue": venue,
                "fav_ud": fav_ud,
                "current_value": active,
                "historical_source_filepath": source_path.relative_to(root).as_posix(),
                "historical_config_filepath": hist_config_path.relative_to(root).as_posix(),
                "historical_source_value": source_value,
                "historical_sample_size": bets,
                "historical_wins": wins,
                "historical_losses": losses,
                "historical_pushes": pushes,
                "historical_profit": profit,
                "historical_roi": roi,
                "historical_win_rate": win_rate,
                "historical_extra_juice": hist_extra,
                "mathematical_transformation": transform,
                "provenance_classification": classification,
                "direct_roi_or_profit_flag": direct,
                "probabilistically_unsupported_flag": unsupported,
                "independent_oos_probability_validation_found": False,
                "provenance_note": note,
            }
        )
    return rows


def total_metric_band(line: float) -> str:
    if line <= 4.5:
        return "0 to 4.5"
    if line <= 5.5:
        return "5 to 5.5"
    if line <= 6.5:
        return "6 to 6.5"
    return "7 to 100"


def total_rows(root: Path) -> list[dict]:
    current_path = root / NHL_REL / "config/juice/nhl_total_juice.csv"
    hist_config_path = root / ARCHIVE_REL / "nhl_total_juice.csv"
    hist_metrics_path = root / ARCHIVE_REL / "nhl_totals_bands.csv"

    current = read_csv(current_path)
    hist_config = read_csv(hist_config_path)
    metrics = read_csv(hist_metrics_path)

    metrics["band"] = metrics["band"].map(text)
    metrics["side"] = metrics["side"].map(text)
    for col in ["bets", "wins", "profit", "win_pct", "roi"]:
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce")

    rows = []
    for _, r in current.iterrows():
        band = text(r["band"])
        side = text(r["side"])
        line = num(r["band_min"])
        active = num(r["model_calibration_adjustment"])

        hc = hist_config[
            (hist_config["band"].map(text) == band)
            & (hist_config["side"].map(text) == side)
        ]
        metric_band = total_metric_band(line)
        hm = metrics[
            (metrics["band"] == metric_band)
            & (metrics["side"] == side)
        ]

        hist_extra = num(hc.iloc[0]["extra_juice"]) if len(hc) == 1 else np.nan
        m = hm.iloc[0] if len(hm) == 1 else None

        bets = num(m["bets"]) if m is not None else np.nan
        wins = num(m["wins"]) if m is not None else np.nan
        profit = num(m["profit"]) if m is not None else np.nan
        win_rate = num(m["win_pct"]) if m is not None else np.nan
        roi = num(m["roi"]) if m is not None else np.nan

        # The located historical totals table does not contain losses or pushes.
        losses = np.nan
        pushes = np.nan

        classification, transform, direct, unsupported, note = classify(
            active, roi, profit, win_rate, hist_extra
        )

        if m is not None and classification in {"ROI", "negative ROI", "profit", "empirical win rate"}:
            source_path = hist_metrics_path
            source_value = {
                "ROI": roi,
                "negative ROI": roi,
                "profit": profit,
                "empirical win rate": win_rate,
            }[classification]
        elif len(hc) == 1:
            source_path = hist_config_path
            source_value = hist_extra
        else:
            source_path = hist_metrics_path if m is not None else hist_config_path
            source_value = np.nan

        if classification == "unknown provenance":
            if m is not None:
                note += (
                    f" Historical aggregate band={metric_band}, ROI={roi:.4f}; "
                    f"active={active:.4f}. No exact supported transform was found."
                )
            if len(hc) == 0:
                note += " No matching archived extra_juice row exists for this exact active total line."
            note += " Historical totals source omits separate losses/pushes, so those fields remain unavailable."

        rows.append(
            {
                "market": "total",
                "current_band": band,
                "band_min": num(r["band_min"]),
                "band_max": num(r["band_max"]),
                "side": side,
                "venue": "",
                "fav_ud": "",
                "current_value": active,
                "historical_source_filepath": source_path.relative_to(root).as_posix(),
                "historical_config_filepath": hist_config_path.relative_to(root).as_posix(),
                "historical_source_value": source_value,
                "historical_sample_size": bets,
                "historical_wins": wins,
                "historical_losses": losses,
                "historical_pushes": pushes,
                "historical_profit": profit,
                "historical_roi": roi,
                "historical_win_rate": win_rate,
                "historical_extra_juice": hist_extra,
                "mathematical_transformation": transform,
                "provenance_classification": classification,
                "direct_roi_or_profit_flag": direct,
                "probabilistically_unsupported_flag": unsupported,
                "independent_oos_probability_validation_found": False,
                "provenance_note": note,
            }
        )
    return rows


def main() -> None:
    root = find_repo_root()
    output_dir = root / NHL_REL / "research/stage02_coefficient_provenance"
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = ml_rows(root) + puck_rows(root) + total_rows(root)
    audit = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    # Stable ordering.
    audit = audit.sort_values(
        ["market", "band_min", "venue", "side", "fav_ud"],
        kind="stable",
    ).reset_index(drop=True)

    provenance_csv = output_dir / "stage02_coefficient_provenance.csv"
    audit.to_csv(provenance_csv, index=False)

    unsupported = audit[audit["probabilistically_unsupported_flag"] == True].copy()
    unsupported_csv = output_dir / "probabilistically_unsupported_coefficients.csv"
    unsupported.to_csv(unsupported_csv, index=False)

    class_counts = (
        audit.groupby(["market", "provenance_classification"], dropna=False)
        .size()
        .reset_index(name="coefficient_count")
    )
    class_counts_csv = output_dir / "provenance_classification_counts.csv"
    class_counts.to_csv(class_counts_csv, index=False)

    direct_count = int(audit["direct_roi_or_profit_flag"].sum())
    unknown_count = int((audit["provenance_classification"] == "unknown provenance").sum())

    summary = [
        "NHL STAGE 02 COEFFICIENT PROVENANCE AUDIT",
        "=========================================",
        "research_only=true",
        "production_files_modified=false",
        f"active_coefficients={len(audit)}",
        f"moneyline_coefficients={(audit['market'] == 'moneyline').sum()}",
        f"puck_line_coefficients={(audit['market'] == 'puck_line').sum()}",
        f"total_coefficients={(audit['market'] == 'total').sum()}",
        f"direct_roi_or_profit_coefficients={direct_count}",
        f"unknown_provenance_coefficients={unknown_count}",
        "proper_probability_calibration_coefficients=0",
        "independent_oos_probability_validation_found=false",
        "",
        "Stage 02 active transformation:",
        "adjusted_decimal = fair_decimal * (1 - model_calibration_adjustment)",
        "adjusted_probability_raw = 1 / adjusted_decimal",
        "opposing adjusted probabilities are then normalized to sum to 1",
        "",
        "Interpretation:",
        "Any coefficient classified as ROI, negative ROI, or profit is probabilistically unsupported",
        "unless independently validated out-of-sample using proper probability metrics.",
        "No such independent validation was located by this provenance audit.",
        "",
        "Artifacts:",
        str(provenance_csv),
        str(unsupported_csv),
        str(class_counts_csv),
    ]
    summary_path = output_dir / "summary.txt"
    summary_path.write_text("\n".join(summary) + "\n", encoding="utf-8")

    print("STAGE 02 PROVENANCE AUDIT COMPLETE")
    print(f"Rows: {len(audit)}")
    print(f"Direct ROI/profit-derived: {direct_count}")
    print(f"Unknown provenance: {unknown_count}")
    print(f"Output: {output_dir}")
    print()
    print(class_counts.to_string(index=False))


if __name__ == "__main__":
    main()
