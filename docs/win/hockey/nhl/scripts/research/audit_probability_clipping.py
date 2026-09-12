#!/usr/bin/env python3
"""Research-only audit of NHL probability clipping thresholds.

This script never edits production files. It runs isolated historical replays for
current [0.01, 0.99] clipping and epsilon candidates 1e-4, 1e-5, 1e-6, then
writes auditable comparison artifacts under:

    docs/win/hockey/nhl/research/probability_clipping_audit/

Production promotion is intentionally out of scope.
"""

from __future__ import annotations

import importlib.util
import math
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson, skellam


NHL_REL = Path("docs/win/hockey/nhl")
SEASON = "2025_2026"
EPSILONS = [0.01, 1e-4, 1e-5, 1e-6]
BASELINE_EPSILON = 0.01
HOLDOUT_FRACTION = 0.20
OUTPUT_REL = Path("research/probability_clipping_audit")


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve()]
    seen: set[Path] = set()
    for start in starts:
        for candidate in [start, *start.parents]:
            if candidate in seen:
                continue
            seen.add(candidate)
            if (candidate / NHL_REL).is_dir():
                return candidate
    raise RuntimeError(f"Could not find repository root containing {NHL_REL.as_posix()}")


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def label_for_epsilon(epsilon: float) -> str:
    if math.isclose(epsilon, 0.01, rel_tol=0.0, abs_tol=0.0):
        return "current_0.01"
    return f"epsilon_{epsilon:.0e}".replace("e-0", "e-")


def numeric(value):
    try:
        parsed = float(value)
    except Exception:
        return np.nan
    return parsed if np.isfinite(parsed) else np.nan


def read_csv_required(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"Missing {label}: {path}")
    return pd.read_csv(path, dtype={"game_id": str})


def load_final_scores(archive_dir: Path) -> pd.DataFrame:
    files = sorted(archive_dir.glob("*_NHL_final_scores.csv"))
    if not files:
        raise RuntimeError(f"No archived final-score files found in {archive_dir}")
    parts = [pd.read_csv(path, dtype={"game_id": str}) for path in files]
    scores = pd.concat(parts, ignore_index=True)
    scores["game_id"] = scores["game_id"].astype(str).str.strip()
    return scores


def build_master_frame(nhl: Path) -> pd.DataFrame:
    season_master = nhl / "season_master" / SEASON
    games = read_csv_required(season_master / "games.csv", "season games")
    preds = read_csv_required(season_master / "predictions.csv", "season predictions")
    book = read_csv_required(season_master / "sportsbook.csv", "season sportsbook")
    scores = load_final_scores(nhl / "archive/2025_26/05_final_scores/final_scores")

    for frame in (games, preds, book, scores):
        frame["game_id"] = frame["game_id"].astype(str).str.strip()

    required_games = ["game_id", "game_date", "away_team", "home_team"]
    required_preds = [
        "game_id",
        "away_projected_goals",
        "home_projected_goals",
        "total_projected_goals",
    ]
    required_book = [
        "game_id",
        "away_puck_line",
        "home_puck_line",
        "total",
        "away_dk_puck_line_american",
        "home_dk_puck_line_american",
        "away_dk_puck_line_decimal",
        "home_dk_puck_line_decimal",
        "dk_total_over_american",
        "dk_total_under_american",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
    ]
    required_scores = ["game_id", "away_score", "home_score", "total_score"]

    for frame, cols, label in [
        (games, required_games, "games.csv"),
        (preds, required_preds, "predictions.csv"),
        (book, required_book, "sportsbook.csv"),
        (scores, required_scores, "final scores"),
    ]:
        missing = [col for col in cols if col not in frame.columns]
        if missing:
            raise RuntimeError(f"{label} missing required columns: {missing}")

    master = games[required_games].merge(
        preds[required_preds], on="game_id", how="left", validate="one_to_one"
    )
    master = master.merge(
        book[required_book], on="game_id", how="left", validate="one_to_one"
    )
    master = master.merge(
        scores[required_scores], on="game_id", how="left", validate="one_to_one"
    )
    return master


def puck_result(side: str, home_score, away_score, home_line, away_line) -> str:
    hs = numeric(home_score)
    aws = numeric(away_score)
    line = numeric(home_line if side == "home" else away_line)
    if not all(np.isfinite(v) for v in (hs, aws, line)):
        return "unknown"
    lhs = hs + line if side == "home" else aws + line
    rhs = aws if side == "home" else hs
    if lhs > rhs:
        return "win"
    if lhs < rhs:
        return "loss"
    return "push"


def total_result(side: str, total_score, line) -> str:
    score = numeric(total_score)
    line_v = numeric(line)
    if not all(np.isfinite(v) for v in (score, line_v)):
        return "unknown"
    if score == line_v:
        return "push"
    if side == "over":
        return "win" if score > line_v else "loss"
    return "win" if score < line_v else "loss"


def compute_raw_extremes(master: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for _, row in master.iterrows():
        game_id = str(row["game_id"])
        game_date = str(row["game_date"])
        home_pg = numeric(row["home_projected_goals"])
        away_pg = numeric(row["away_projected_goals"])
        total_pg = numeric(row["total_projected_goals"])
        home_line = numeric(row["home_puck_line"])
        away_line = numeric(row["away_puck_line"])
        total_line = numeric(row["total"])

        if home_pg > 0 and away_pg > 0:
            puck_specs = [
                (
                    "home",
                    home_line,
                    home_pg,
                    away_pg,
                    row["home_dk_puck_line_american"],
                    row["home_dk_puck_line_decimal"],
                ),
                (
                    "away",
                    away_line,
                    away_pg,
                    home_pg,
                    row["away_dk_puck_line_american"],
                    row["away_dk_puck_line_decimal"],
                ),
            ]
            for side, line, team_pg, opp_pg, american, decimal in puck_specs:
                if not np.isfinite(line):
                    continue
                threshold = math.floor(-line)
                raw_prob = 1.0 - skellam.cdf(threshold, team_pg, opp_pg)
                if raw_prob < 0.01 or raw_prob > 0.99:
                    rows.append(
                        {
                            "game_date": game_date,
                            "game_id": game_id,
                            "market": "puck_line",
                            "side": side,
                            "line": line,
                            "raw_probability": float(raw_prob),
                            "raw_win_probability": float(raw_prob),
                            "raw_loss_probability": float(1.0 - raw_prob),
                            "push_probability": 0.0,
                            "extreme_band": "<0.01" if raw_prob < 0.01 else ">0.99",
                            "sportsbook_american": numeric(american),
                            "sportsbook_decimal": numeric(decimal),
                            "result": puck_result(
                                side,
                                row["home_score"],
                                row["away_score"],
                                home_line,
                                away_line,
                            ),
                        }
                    )

        if total_pg > 0 and np.isfinite(total_line):
            if float(total_line).is_integer():
                integer_line = int(total_line)
                under_win = float(poisson.cdf(integer_line - 1, total_pg))
                push_prob = float(poisson.pmf(integer_line, total_pg))
                over_win = float(1.0 - poisson.cdf(integer_line, total_pg))
                no_push = under_win + over_win
                if no_push <= 0:
                    continue
                under_raw = under_win / no_push
                over_raw = over_win / no_push
            else:
                cutoff = math.floor(total_line)
                under_win = float(poisson.cdf(cutoff, total_pg))
                over_win = float(1.0 - under_win)
                push_prob = 0.0
                under_raw = under_win
                over_raw = over_win

            total_specs = [
                (
                    "over",
                    over_raw,
                    over_win,
                    under_win,
                    row["dk_total_over_american"],
                    row["dk_total_over_decimal"],
                ),
                (
                    "under",
                    under_raw,
                    under_win,
                    over_win,
                    row["dk_total_under_american"],
                    row["dk_total_under_decimal"],
                ),
            ]
            for side, raw_prob, win_prob, loss_prob, american, decimal in total_specs:
                if raw_prob < 0.01 or raw_prob > 0.99:
                    rows.append(
                        {
                            "game_date": game_date,
                            "game_id": game_id,
                            "market": "total",
                            "side": side,
                            "line": total_line,
                            "raw_probability": float(raw_prob),
                            "raw_win_probability": float(win_prob),
                            "raw_loss_probability": float(loss_prob),
                            "push_probability": float(push_prob),
                            "extreme_band": "<0.01" if raw_prob < 0.01 else ">0.99",
                            "sportsbook_american": numeric(american),
                            "sportsbook_decimal": numeric(decimal),
                            "result": total_result(side, row["total_score"], total_line),
                        }
                    )

    columns = [
        "game_date",
        "game_id",
        "market",
        "side",
        "line",
        "raw_probability",
        "raw_win_probability",
        "raw_loss_probability",
        "push_probability",
        "extreme_band",
        "sportsbook_american",
        "sportsbook_decimal",
        "result",
    ]
    return pd.DataFrame(rows, columns=columns)


def patch_stage01_clipping(path: Path, epsilon: float) -> None:
    text = path.read_text(encoding="utf-8")
    expected_counts = {
        "min(max(probability, 0.01), 0.99)": 2,
        "min(max(over_prob, 0.01), 0.99)": 1,
        "min(max(under_prob, 0.01), 0.99)": 1,
    }
    for needle, expected in expected_counts.items():
        actual = text.count(needle)
        if actual != expected:
            raise RuntimeError(
                f"Unexpected production clipping contract in {path}: "
                f"{needle!r} expected {expected}, found {actual}"
            )

    upper = 1.0 - epsilon
    text = text.replace(
        "min(max(probability, 0.01), 0.99)",
        f"min(max(probability, {epsilon!r}), {upper!r})",
    )
    text = text.replace(
        "min(max(over_prob, 0.01), 0.99)",
        f"min(max(over_prob, {epsilon!r}), {upper!r})",
    )
    text = text.replace(
        "min(max(under_prob, 0.01), 0.99)",
        f"min(max(under_prob, {epsilon!r}), {upper!r})",
    )
    path.write_text(text, encoding="utf-8")


def concat_csvs(directory: Path, pattern: str) -> pd.DataFrame:
    files = sorted(directory.glob(pattern))
    if not files:
        return pd.DataFrame()
    parts = [pd.read_csv(path, dtype={"game_id": str}) for path in files]
    out = pd.concat(parts, ignore_index=True)
    if "game_id" in out.columns:
        out["game_id"] = out["game_id"].astype(str).str.strip()
    return out


def long_variant_metrics(stage01_dir: Path, ev_dir: Path, label: str) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    puck01 = concat_csvs(stage01_dir, "*_NHL_puck_line.csv")
    puck03 = concat_csvs(ev_dir, "*_NHL_puck_line.csv")
    if not puck01.empty and not puck03.empty:
        fair = puck01[
            [
                "game_id",
                "game_date",
                "away_fair_decimal_puck_line",
                "home_fair_decimal_puck_line",
            ]
        ].copy()
        ev = puck03[
            [
                "game_id",
                "away_model_prob_puck_line",
                "home_model_prob_puck_line",
                "away_edge_pct_puck_line",
                "home_edge_pct_puck_line",
                "away_ev_puck_line",
                "home_ev_puck_line",
                "away_kelly_puck_line",
                "home_kelly_puck_line",
            ]
        ].copy()
        merged = fair.merge(ev, on="game_id", how="inner", validate="one_to_one")
        for side in ("away", "home"):
            part = pd.DataFrame(
                {
                    "variant": label,
                    "game_date": merged["game_date"].astype(str),
                    "game_id": merged["game_id"].astype(str),
                    "market": "puck_line",
                    "side": side,
                    "fair_decimal_odds": pd.to_numeric(
                        merged[f"{side}_fair_decimal_puck_line"], errors="coerce"
                    ),
                    "model_prob": pd.to_numeric(
                        merged[f"{side}_model_prob_puck_line"], errors="coerce"
                    ),
                    "edge": pd.to_numeric(
                        merged[f"{side}_edge_pct_puck_line"], errors="coerce"
                    ),
                    "ev": pd.to_numeric(
                        merged[f"{side}_ev_puck_line"], errors="coerce"
                    ),
                    "kelly": pd.to_numeric(
                        merged[f"{side}_kelly_puck_line"], errors="coerce"
                    ),
                }
            )
            rows.append(part)

    total01 = concat_csvs(stage01_dir, "*_NHL_total.csv")
    total03 = concat_csvs(ev_dir, "*_NHL_total.csv")
    if not total01.empty and not total03.empty:
        fair = total01[
            [
                "game_id",
                "game_date",
                "over_fair_decimal_total",
                "under_fair_decimal_total",
            ]
        ].copy()
        ev = total03[
            [
                "game_id",
                "over_model_prob_total",
                "under_model_prob_total",
                "over_edge_pct_total",
                "under_edge_pct_total",
                "over_ev_total",
                "under_ev_total",
                "over_kelly_total",
                "under_kelly_total",
            ]
        ].copy()
        merged = fair.merge(ev, on="game_id", how="inner", validate="one_to_one")
        for side in ("over", "under"):
            part = pd.DataFrame(
                {
                    "variant": label,
                    "game_date": merged["game_date"].astype(str),
                    "game_id": merged["game_id"].astype(str),
                    "market": "total",
                    "side": side,
                    "fair_decimal_odds": pd.to_numeric(
                        merged[f"{side}_fair_decimal_total"], errors="coerce"
                    ),
                    "model_prob": pd.to_numeric(
                        merged[f"{side}_model_prob_total"], errors="coerce"
                    ),
                    "edge": pd.to_numeric(
                        merged[f"{side}_edge_pct_total"], errors="coerce"
                    ),
                    "ev": pd.to_numeric(merged[f"{side}_ev_total"], errors="coerce"),
                    "kelly": pd.to_numeric(
                        merged[f"{side}_kelly_total"], errors="coerce"
                    ),
                }
            )
            rows.append(part)

    if not rows:
        raise RuntimeError(f"No downstream puck-line or total metrics produced for {label}")
    return pd.concat(rows, ignore_index=True)


def selected_rows(select_dir: Path, label: str) -> pd.DataFrame:
    selected = concat_csvs(select_dir, "*_NHL.csv")
    columns = ["variant", "game_date", "game_id", "market_type", "bet_side", "line"]
    if selected.empty:
        return pd.DataFrame(columns=columns)
    required = ["game_date", "game_id", "market_type", "bet_side", "line"]
    missing = [col for col in required if col not in selected.columns]
    if missing:
        raise RuntimeError(f"Selector output missing columns: {missing}")
    selected = selected[selected["market_type"].isin(["puck_line", "total"])].copy()
    selected.insert(0, "variant", label)
    return selected[columns]


def run_variant(repo_root: Path, nhl: Path, replay, epsilon: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    label = label_for_epsilon(epsilon)
    temp_root = Path(tempfile.mkdtemp(prefix=f"nhl_clip_audit_{label}_"))
    fake_nhl = temp_root / NHL_REL

    scripts = [
        Path("scripts/01_merge/build_juice_files.py"),
        Path("scripts/02_juice/apply_moneyline_juice.py"),
        Path("scripts/02_juice/apply_puck_line_juice.py"),
        Path("scripts/02_juice/apply_total_juice.py"),
        Path("scripts/03_edges/compute_edges.py"),
        Path("scripts/03_edges/compute_ev_kelly.py"),
        Path("scripts/04_select/hockey_select_bets.py"),
    ]
    juice_configs = [
        Path("config/juice/nhl_moneyline_juice.csv"),
        Path("config/juice/nhl_puck_line_juice.csv"),
        Path("config/juice/nhl_total_juice.csv"),
    ]

    success = False
    try:
        for rel in scripts:
            replay.copy_file(nhl / rel, fake_nhl / rel)
        for rel in juice_configs:
            replay.copy_file(nhl / rel, fake_nhl / rel)
        replay.copy_file(
            nhl / "config/markets_backtest.yaml",
            fake_nhl / "config/markets.yaml",
        )

        replay.build_full_season_merged_inputs(
            nhl / "season_master" / SEASON,
            fake_nhl / "01_merge",
        )

        patch_stage01_clipping(
            fake_nhl / "scripts/01_merge/build_juice_files.py",
            epsilon,
        )

        for rel in scripts[:6]:
            replay.run_script(temp_root, rel)

        replay.prepare_selector_inputs(
            fake_nhl / "03_edges/ev_kelly",
            fake_nhl / "03_edges/secondary_signals",
        )
        replay.copy_final_scores_and_build_status(
            nhl / "archive/2025_26/05_final_scores/final_scores",
            fake_nhl / "05_final_scores",
        )
        replay.run_script(temp_root, scripts[6])

        metrics = long_variant_metrics(
            fake_nhl / "01_merge/01_merguiced",
            fake_nhl / "03_edges/ev_kelly",
            label,
        )
        selected = selected_rows(fake_nhl / "04_select", label)
        success = True
        return metrics, selected
    except Exception:
        print(f"Variant failed; temporary workspace retained: {temp_root}")
        raise
    finally:
        if success and temp_root.exists():
            shutil.rmtree(temp_root, ignore_errors=True)


def holdout_start_date(master: pd.DataFrame) -> str:
    dates = sorted(master["game_date"].astype(str).dropna().unique().tolist())
    if len(dates) < 5:
        raise RuntimeError("Not enough unique game dates for chronological holdout")
    start_idx = int(math.floor(len(dates) * (1.0 - HOLDOUT_FRACTION)))
    start_idx = min(max(start_idx, 1), len(dates) - 1)
    return dates[start_idx]


def with_scope(df: pd.DataFrame, holdout_start: str) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("full_season", df),
        (
            "chronological_holdout",
            df[df["game_date"].astype(str) >= holdout_start].copy(),
        ),
    ]


def downstream_changes(all_metrics: pd.DataFrame, holdout_start: str) -> pd.DataFrame:
    baseline_label = label_for_epsilon(BASELINE_EPSILON)
    baseline = all_metrics[all_metrics["variant"] == baseline_label].copy()
    keys = ["game_date", "game_id", "market", "side"]
    metric_cols = ["fair_decimal_odds", "edge", "ev", "kelly"]
    rows: list[dict] = []

    for variant in sorted(all_metrics["variant"].unique()):
        candidate = all_metrics[all_metrics["variant"] == variant].copy()
        merged = baseline[keys + metric_cols].merge(
            candidate[keys + metric_cols],
            on=keys,
            how="inner",
            suffixes=("_baseline", "_candidate"),
            validate="one_to_one",
        )
        for scope, scoped in with_scope(merged, holdout_start):
            for market in ("puck_line", "total"):
                market_df = scoped[scoped["market"] == market]
                for metric in metric_cols:
                    a = pd.to_numeric(market_df[f"{metric}_baseline"], errors="coerce")
                    b = pd.to_numeric(market_df[f"{metric}_candidate"], errors="coerce")
                    valid = a.notna() & b.notna()
                    diff = (b[valid] - a[valid]).abs()
                    changed = ~np.isclose(
                        a[valid].to_numpy(dtype=float),
                        b[valid].to_numpy(dtype=float),
                        rtol=0.0,
                        atol=1e-12,
                        equal_nan=True,
                    )
                    rows.append(
                        {
                            "variant": variant,
                            "scope": scope,
                            "market": market,
                            "metric": metric,
                            "rows_compared": int(valid.sum()),
                            "changed_rows": int(np.count_nonzero(changed)),
                            "mean_abs_change": float(diff.mean()) if len(diff) else np.nan,
                            "max_abs_change": float(diff.max()) if len(diff) else np.nan,
                        }
                    )
    return pd.DataFrame(rows)


def outcome_frame(master: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in master.iterrows():
        game_id = str(row["game_id"])
        game_date = str(row["game_date"])
        for side in ("home", "away"):
            result = puck_result(
                side,
                row["home_score"],
                row["away_score"],
                row["home_puck_line"],
                row["away_puck_line"],
            )
            rows.append(
                {
                    "game_date": game_date,
                    "game_id": game_id,
                    "market": "puck_line",
                    "side": side,
                    "result": result,
                }
            )
        for side in ("over", "under"):
            result = total_result(side, row["total_score"], row["total"])
            rows.append(
                {
                    "game_date": game_date,
                    "game_id": game_id,
                    "market": "total",
                    "side": side,
                    "result": result,
                }
            )
    return pd.DataFrame(rows)


def calibration_table(
    all_metrics: pd.DataFrame,
    outcomes: pd.DataFrame,
    holdout_start: str,
) -> pd.DataFrame:
    merged = all_metrics.merge(
        outcomes,
        on=["game_date", "game_id", "market", "side"],
        how="left",
        validate="many_to_one",
    )
    rows: list[dict] = []
    tiny = np.finfo(float).eps

    for variant in sorted(merged["variant"].unique()):
        vdf = merged[merged["variant"] == variant]
        for scope, scoped in with_scope(vdf, holdout_start):
            for market in ("puck_line", "total"):
                mdf = scoped[scoped["market"] == market]
                for side in sorted(mdf["side"].dropna().unique()):
                    sdf = mdf[mdf["side"] == side].copy()
                    sdf = sdf[sdf["result"].isin(["win", "loss"])]
                    p = pd.to_numeric(sdf["model_prob"], errors="coerce")
                    valid = p.notna() & np.isfinite(p)
                    p = p[valid].astype(float)
                    if p.empty:
                        rows.append(
                            {
                                "variant": variant,
                                "scope": scope,
                                "market": market,
                                "side": side,
                                "n": 0,
                                "brier": np.nan,
                                "log_loss": np.nan,
                                "mean_predicted": np.nan,
                                "observed_win_rate": np.nan,
                                "calibration_bias": np.nan,
                            }
                        )
                        continue
                    y = (sdf.loc[p.index, "result"] == "win").astype(float).to_numpy()
                    p_arr = p.to_numpy(dtype=float)
                    p_log = np.clip(p_arr, tiny, 1.0 - tiny)
                    brier = float(np.mean((p_arr - y) ** 2))
                    log_loss = float(
                        -np.mean(y * np.log(p_log) + (1.0 - y) * np.log(1.0 - p_log))
                    )
                    mean_pred = float(np.mean(p_arr))
                    observed = float(np.mean(y))
                    rows.append(
                        {
                            "variant": variant,
                            "scope": scope,
                            "market": market,
                            "side": side,
                            "n": int(len(p_arr)),
                            "brier": brier,
                            "log_loss": log_loss,
                            "mean_predicted": mean_pred,
                            "observed_win_rate": observed,
                            "calibration_bias": mean_pred - observed,
                        }
                    )

    result = pd.DataFrame(rows)
    baseline_label = label_for_epsilon(BASELINE_EPSILON)
    base = result[result["variant"] == baseline_label][
        ["scope", "market", "side", "brier", "log_loss", "calibration_bias"]
    ].rename(
        columns={
            "brier": "baseline_brier",
            "log_loss": "baseline_log_loss",
            "calibration_bias": "baseline_calibration_bias",
        }
    )
    result = result.merge(base, on=["scope", "market", "side"], how="left")
    result["delta_brier_vs_current"] = result["brier"] - result["baseline_brier"]
    result["delta_log_loss_vs_current"] = result["log_loss"] - result["baseline_log_loss"]
    result["delta_abs_bias_vs_current"] = (
        result["calibration_bias"].abs() - result["baseline_calibration_bias"].abs()
    )
    return result


def selected_comparison(all_selected: pd.DataFrame, holdout_start: str) -> pd.DataFrame:
    baseline_label = label_for_epsilon(BASELINE_EPSILON)
    rows: list[dict] = []

    def keyset(df: pd.DataFrame) -> set[tuple[str, str, str, str]]:
        if df.empty:
            return set()
        return {
            (
                str(row.game_id),
                str(row.market_type),
                str(row.bet_side),
                "" if pd.isna(row.line) else str(row.line),
            )
            for row in df.itertuples(index=False)
        }

    base_all = all_selected[all_selected["variant"] == baseline_label]
    for variant in sorted(all_selected["variant"].unique()):
        candidate_all = all_selected[all_selected["variant"] == variant]
        for scope, base_scope in with_scope(base_all, holdout_start):
            candidate_scope = (
                candidate_all
                if scope == "full_season"
                else candidate_all[candidate_all["game_date"].astype(str) >= holdout_start]
            )
            for market in ("puck_line", "total"):
                b = keyset(base_scope[base_scope["market_type"] == market])
                c = keyset(candidate_scope[candidate_scope["market_type"] == market])
                rows.append(
                    {
                        "variant": variant,
                        "scope": scope,
                        "market": market,
                        "baseline_selected": len(b),
                        "candidate_selected": len(c),
                        "added_vs_current": len(c - b),
                        "removed_vs_current": len(b - c),
                    }
                )
    return pd.DataFrame(rows)


def main() -> None:
    repo_root = find_repo_root()
    nhl = repo_root / NHL_REL
    output_dir = nhl / OUTPUT_REL
    output_dir.mkdir(parents=True, exist_ok=True)

    replay_path = nhl / "scripts/backtest/run_2025_2026_replay.py"
    replay = load_module(replay_path, "nhl_full_season_replay")

    master = build_master_frame(nhl)
    holdout_start = holdout_start_date(master)

    extremes = compute_raw_extremes(master)
    extremes.to_csv(output_dir / "raw_probability_extremes.csv", index=False)
    if extremes.empty:
        extreme_summary = pd.DataFrame(
            columns=["market", "side", "extreme_band", "count"]
        )
    else:
        extreme_summary = (
            extremes.groupby(["market", "side", "extreme_band"], dropna=False)
            .size()
            .reset_index(name="count")
        )
    extreme_summary.to_csv(output_dir / "raw_probability_extreme_counts.csv", index=False)

    metric_parts: list[pd.DataFrame] = []
    selected_parts: list[pd.DataFrame] = []
    for epsilon in EPSILONS:
        label = label_for_epsilon(epsilon)
        print(f"\n=== CLIPPING VARIANT: {label} ===")
        metrics, selected = run_variant(repo_root, nhl, replay, epsilon)
        metric_parts.append(metrics)
        selected_parts.append(selected)

    all_metrics = pd.concat(metric_parts, ignore_index=True)
    all_selected = pd.concat(selected_parts, ignore_index=True)
    all_metrics.to_csv(output_dir / "variant_side_metrics.csv", index=False)
    all_selected.to_csv(output_dir / "selected_bets_by_variant.csv", index=False)

    changes = downstream_changes(all_metrics, holdout_start)
    changes.to_csv(output_dir / "downstream_metric_changes.csv", index=False)

    outcomes = outcome_frame(master)
    calibration = calibration_table(all_metrics, outcomes, holdout_start)
    calibration.to_csv(output_dir / "calibration_comparison.csv", index=False)

    selected_cmp = selected_comparison(all_selected, holdout_start)
    selected_cmp.to_csv(output_dir / "selected_bet_changes.csv", index=False)

    holdout_rows = int((master["game_date"].astype(str) >= holdout_start).sum())
    summary_lines = [
        "NHL PROBABILITY CLIPPING AUDIT",
        "==============================",
        "research_only=true",
        "production_files_modified=false",
        "production_promotion_performed=false",
        f"season={SEASON}",
        f"games={len(master)}",
        f"holdout_fraction={HOLDOUT_FRACTION:.2f}",
        f"chronological_holdout_start={holdout_start}",
        f"chronological_holdout_games={holdout_rows}",
        "current_clip=[0.01,0.99]",
        "candidate_epsilons=1e-4,1e-5,1e-6",
        "stage01_clip_instances=4",
        "selection_config=markets_backtest.yaml",
        f"raw_extreme_rows={len(extremes)}",
        "decision=RESEARCH_ONLY_REVIEW_REQUIRED",
        "note=No clipping threshold is promoted automatically; review chronological holdout evidence first.",
        "",
        "Artifacts:",
        str(output_dir / "raw_probability_extremes.csv"),
        str(output_dir / "raw_probability_extreme_counts.csv"),
        str(output_dir / "variant_side_metrics.csv"),
        str(output_dir / "downstream_metric_changes.csv"),
        str(output_dir / "calibration_comparison.csv"),
        str(output_dir / "selected_bets_by_variant.csv"),
        str(output_dir / "selected_bet_changes.csv"),
    ]
    (output_dir / "summary.txt").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print("\nAUDIT COMPLETE")
    print(f"Output: {output_dir}")
    print(f"Raw extreme rows: {len(extremes)}")
    print(f"Chronological holdout starts: {holdout_start}")
    print("Production clipping remains unchanged.")


if __name__ == "__main__":
    main()
