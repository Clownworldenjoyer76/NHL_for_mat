#!/usr/bin/env python3
"""
Research-only independent audit of historical NHL side-specific probability bias.

The three stored aggregate files under research/calibration are NOT used to
construct metrics. They are loaded only after independent reconstruction so
their values can be compared against independently calculated results.

Independent source data:
  docs/win/hockey/nhl/archive/2025_26/01_merge/*_NHL_merged.csv
  docs/win/hockey/nhl/archive/2025_26/05_final_scores/final_scores/*_NHL_final_scores.csv
  docs/win/hockey/nhl/config/juice/*.csv

Production Stage 01/02 probability math is independently reproduced in this
script. No production files are modified.
"""

from __future__ import annotations

import math
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
from scipy.stats import norm, poisson, skellam

warnings.filterwarnings("ignore", category=PerformanceWarning)

NHL_REL = Path("docs/win/hockey/nhl")
ARCHIVE_MERGED_REL = NHL_REL / "archive/2025_26/01_merge"
ARCHIVE_SCORES_REL = NHL_REL / "archive/2025_26/05_final_scores/final_scores"
CONFIG_REL = NHL_REL / "config/juice"
STORED_REL = NHL_REL / "research/calibration"
OUTPUT_REL = NHL_REL / "research/side_specific_probability_bias_audit"

EPS = 1e-15
CI_LEVEL = 0.95
MIN_CONCENTRATION_SAMPLE = 30

PRIMARY_GROUPS = [
    ("moneyline", "home"),
    ("moneyline", "away"),
    ("puck_line", "home"),
    ("puck_line", "away"),
    ("total", "over"),
    ("total", "under"),
]


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
    raise RuntimeError(f"Could not find repository root containing {NHL_REL}")


def parse_date(value):
    ts = pd.to_datetime(str(value).strip().replace("_", "-"), errors="coerce")
    return None if pd.isna(ts) else ts.normalize()


def to_float(value):
    try:
        x = float(value)
    except Exception:
        return np.nan
    return x if math.isfinite(x) else np.nan


def american_to_decimal(value):
    a = to_float(value)
    if not np.isfinite(a) or a == 0:
        return np.nan
    return 1.0 + a / 100.0 if a > 0 else 1.0 + 100.0 / abs(a)


def resolve_decimal(decimal_value, american_value):
    d = to_float(decimal_value)
    if np.isfinite(d) and d > 1.0:
        return d
    return american_to_decimal(american_value)


def current_configs(root: Path):
    files = {
        "moneyline": root / CONFIG_REL / "nhl_moneyline_juice.csv",
        "puck_line": root / CONFIG_REL / "nhl_puck_line_juice.csv",
        "total": root / CONFIG_REL / "nhl_total_juice.csv",
    }
    out = {}
    for market, path in files.items():
        if not path.exists():
            raise RuntimeError(f"Missing config: {path}")
        df = pd.read_csv(path)
        for col in ["band_min", "band_max", "model_calibration_adjustment"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        out[market] = df
    return out


def adjustment(configs, market, side, american=np.nan, line=np.nan):
    df = configs[market]
    if market == "moneyline":
        if not np.isfinite(american):
            return np.nan
        fav_ud = "favorite" if american < 0 else "underdog"
        hit = df[
            (df["band_min"] <= american)
            & (american <= df["band_max"])
            & (df["fav_ud"].astype(str).str.strip() == fav_ud)
            & (df["venue"].astype(str).str.strip() == side)
        ]
    elif market == "puck_line":
        if not np.isfinite(line):
            return np.nan
        fav_ud = "favorite" if line < 0 else "underdog"
        hit = df[
            (df["band_min"] <= line)
            & (line <= df["band_max"])
            & (df["fav_ud"].astype(str).str.strip() == fav_ud)
            & (df["venue"].astype(str).str.strip() == side)
        ]
    elif market == "total":
        if not np.isfinite(line):
            return np.nan
        hit = df[
            (df["band_min"] <= line)
            & (line <= df["band_max"])
            & (df["side"].astype(str).str.strip() == side)
        ]
    else:
        raise ValueError(market)

    if len(hit) != 1:
        return np.nan
    return to_float(hit.iloc[0]["model_calibration_adjustment"])


def production_adjust_pair(p1, p2, a1, a2):
    vals = [p1, p2, a1, a2]
    if not all(np.isfinite(v) for v in vals):
        return None
    if not (0 < p1 < 1 and 0 < p2 < 1):
        return None

    fair1 = 1.0 / p1
    fair2 = 1.0 / p2
    d1 = fair1 * (1.0 - a1)
    d2 = fair2 * (1.0 - a2)

    # Match Stage 02 quarantine behavior.
    if not (np.isfinite(d1) and np.isfinite(d2) and d1 > 1.0 and d2 > 1.0):
        return None

    q1 = 1.0 / d1
    q2 = 1.0 / d2
    denom = q1 + q2
    if not np.isfinite(denom) or denom <= 0:
        return None
    return float(q1 / denom), float(q2 / denom)


def home_puck_probability(home_line, home_goals, away_goals):
    if not all(np.isfinite(v) for v in [home_line, home_goals, away_goals]):
        return np.nan
    if home_goals <= 0 or away_goals <= 0:
        return np.nan
    threshold = math.floor(-home_line)
    p = 1.0 - skellam.cdf(threshold, home_goals, away_goals)
    if not np.isfinite(p):
        return np.nan
    return float(min(max(p, 0.01), 0.99))


def away_puck_probability(away_line, away_goals, home_goals):
    if not all(np.isfinite(v) for v in [away_line, away_goals, home_goals]):
        return np.nan
    if away_goals <= 0 or home_goals <= 0:
        return np.nan
    threshold = math.floor(-away_line)
    p = 1.0 - skellam.cdf(threshold, away_goals, home_goals)
    if not np.isfinite(p):
        return np.nan
    return float(min(max(p, 0.01), 0.99))


def total_probabilities(total_line, projected_total):
    if not np.isfinite(total_line) or not np.isfinite(projected_total) or projected_total <= 0:
        return None

    total_line = float(total_line)
    if total_line.is_integer():
        push_total = int(total_line)
        under_win = float(poisson.cdf(push_total - 1, projected_total))
        push = float(poisson.pmf(push_total, projected_total))
        over_win = float(1.0 - poisson.cdf(push_total, projected_total))
        no_push = under_win + over_win
        if no_push <= 0:
            return None
        under = under_win / no_push
        over = over_win / no_push
    else:
        cutoff = math.floor(total_line)
        under_win = float(poisson.cdf(cutoff, projected_total))
        over_win = 1.0 - under_win
        push = 0.0
        under = under_win
        over = over_win

    if not all(np.isfinite(v) for v in [over, under, push]):
        return None

    # Match Stage 01 clipping rule exactly.
    over = float(min(max(over, 0.01), 0.99))
    under = float(min(max(under, 0.01), 0.99))
    return over, under, push


def load_merged(root: Path):
    files = sorted((root / ARCHIVE_MERGED_REL).glob("*_NHL_merged.csv"))
    if not files:
        raise RuntimeError("No archived merged inputs found.")

    parts = []
    required = [
        "game_id", "game_date",
        "home_prob_moneyline", "away_prob_moneyline",
        "home_projected_goals", "away_projected_goals", "total_projected_goals",
        "home_puck_line", "away_puck_line", "total",
        "home_dk_moneyline_american", "away_dk_moneyline_american",
        "home_dk_moneyline_decimal", "away_dk_moneyline_decimal",
        "home_dk_puck_line_american", "away_dk_puck_line_american",
        "home_dk_puck_line_decimal", "away_dk_puck_line_decimal",
        "dk_total_over_american", "dk_total_under_american",
        "dk_total_over_decimal", "dk_total_under_decimal",
    ]

    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing columns: {missing}")
        x = df[required].copy()
        x["source_input_file"] = path.name
        parts.append(x)

    out = pd.concat(parts, ignore_index=True)
    out["game_id"] = out["game_id"].astype(str).str.strip()
    out["game_date"] = out["game_date"].map(parse_date)
    out = out[out["game_date"].notna() & out["game_id"].ne("")].copy()

    dup = out[out.duplicated("game_id", keep=False)]
    if not dup.empty:
        compare = [c for c in out.columns if c != "source_input_file"]
        bad = []
        for game_id, g in dup.groupby("game_id"):
            if len(g[compare].drop_duplicates()) > 1:
                bad.append(game_id)
        if bad:
            raise RuntimeError(f"Conflicting merged game_ids: {bad[:20]}")
        out = out.drop_duplicates("game_id", keep="first")

    return out


def load_scores(root: Path):
    files = sorted((root / ARCHIVE_SCORES_REL).glob("*_NHL_final_scores.csv"))
    if not files:
        raise RuntimeError("No archived final scores found.")

    parts = []
    for path in files:
        df = pd.read_csv(path, dtype={"game_id": str})
        required = ["game_id", "away_score", "home_score"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(f"{path} missing columns: {missing}")
        x = df[required].copy()
        x["source_score_file"] = path.name
        parts.append(x)

    out = pd.concat(parts, ignore_index=True)
    out["game_id"] = out["game_id"].astype(str).str.strip()
    out["away_score"] = pd.to_numeric(out["away_score"], errors="coerce")
    out["home_score"] = pd.to_numeric(out["home_score"], errors="coerce")
    out = out[
        out["game_id"].ne("")
        & out["away_score"].notna()
        & out["home_score"].notna()
    ].copy()

    dup = out[out.duplicated("game_id", keep=False)]
    if not dup.empty:
        bad = []
        for game_id, g in dup.groupby("game_id"):
            if len(g[["away_score", "home_score"]].drop_duplicates()) > 1:
                bad.append(game_id)
        if bad:
            raise RuntimeError(f"Conflicting final-score game_ids: {bad[:20]}")
        out = out.drop_duplicates("game_id", keep="first")

    return out


def side_result(market, side, line, home_score, away_score):
    if market == "moneyline":
        if home_score == away_score:
            return "Push"
        if side == "home":
            return "Win" if home_score > away_score else "Loss"
        return "Win" if away_score > home_score else "Loss"

    if market == "puck_line":
        if side == "home":
            diff = home_score - away_score + line
        else:
            diff = away_score - home_score + line
        if abs(diff) < 1e-9:
            return "Push"
        return "Win" if diff > 0 else "Loss"

    if market == "total":
        diff = home_score + away_score - line
        if abs(diff) < 1e-9:
            return "Push"
        if side == "over":
            return "Win" if diff > 0 else "Loss"
        return "Win" if diff < 0 else "Loss"

    raise ValueError(market)


def odds_band(a):
    if not np.isfinite(a):
        return "missing"
    if a <= -200:
        return "<=-200"
    if a <= -150:
        return "-199 to -150"
    if a <= -120:
        return "-149 to -120"
    if a <= -100:
        return "-119 to -100"
    if a < 100:
        return "nonstandard"
    if a <= 119:
        return "+100 to +119"
    if a <= 149:
        return "+120 to +149"
    if a <= 199:
        return "+150 to +199"
    return ">=+200"


ODDS_BAND_ORDER = {
    "<=-200": 0,
    "-199 to -150": 1,
    "-149 to -120": 2,
    "-119 to -100": 3,
    "nonstandard": 4,
    "+100 to +119": 5,
    "+120 to +149": 6,
    "+150 to +199": 7,
    ">=+200": 8,
    "missing": 9,
}


def season_half(date):
    # Reproduce stored audit convention:
    # NHL season starting Jul-Dec = first half; Jan-Jun = second half.
    return "first_half" if date.month >= 7 else "second_half"


def build_side_rows(base: pd.DataFrame, configs):
    rows = []

    for r in base.to_dict("records"):
        game_id = str(r["game_id"])
        date = r["game_date"]
        hs = to_float(r["home_score"])
        aas = to_float(r["away_score"])

        # MONEYLINE
        hp = to_float(r["home_prob_moneyline"])
        ap = to_float(r["away_prob_moneyline"])
        h_amer = to_float(r["home_dk_moneyline_american"])
        a_amer = to_float(r["away_dk_moneyline_american"])
        h_dec = resolve_decimal(r["home_dk_moneyline_decimal"], h_amer)
        a_dec = resolve_decimal(r["away_dk_moneyline_decimal"], a_amer)
        ha = adjustment(configs, "moneyline", "home", american=h_amer)
        aa = adjustment(configs, "moneyline", "away", american=a_amer)
        prod = production_adjust_pair(hp, ap, ha, aa)
        if prod and h_dec > 1 and a_dec > 1:
            for side, prob, amer, dec in [
                ("home", prod[0], h_amer, h_dec),
                ("away", prod[1], a_amer, a_dec),
            ]:
                result = side_result("moneyline", side, np.nan, hs, aas)
                rows.append({
                    "game_id": game_id,
                    "game_date": date,
                    "market_type": "moneyline",
                    "bet_side": side,
                    "model_prob": prob,
                    "odds_american": amer,
                    "odds_decimal": dec,
                    "line": np.nan,
                    "favorite_underdog": "favorite" if amer < 0 else "underdog",
                    "odds_band": odds_band(amer),
                    "season_half": season_half(date),
                    "bet_result": result,
                    "source_input_file": r["source_input_file"],
                    "source_score_file": r["source_score_file"],
                })

        # PUCK LINE
        hl = to_float(r["home_puck_line"])
        al = to_float(r["away_puck_line"])
        hg = to_float(r["home_projected_goals"])
        ag = to_float(r["away_projected_goals"])
        hp = home_puck_probability(hl, hg, ag)
        ap = away_puck_probability(al, ag, hg)
        h_amer = to_float(r["home_dk_puck_line_american"])
        a_amer = to_float(r["away_dk_puck_line_american"])
        h_dec = resolve_decimal(r["home_dk_puck_line_decimal"], h_amer)
        a_dec = resolve_decimal(r["away_dk_puck_line_decimal"], a_amer)
        ha = adjustment(configs, "puck_line", "home", line=hl)
        aa = adjustment(configs, "puck_line", "away", line=al)
        prod = production_adjust_pair(hp, ap, ha, aa)
        if prod and h_dec > 1 and a_dec > 1:
            for side, prob, amer, dec, line in [
                ("home", prod[0], h_amer, h_dec, hl),
                ("away", prod[1], a_amer, a_dec, al),
            ]:
                result = side_result("puck_line", side, line, hs, aas)
                rows.append({
                    "game_id": game_id,
                    "game_date": date,
                    "market_type": "puck_line",
                    "bet_side": side,
                    "model_prob": prob,
                    "odds_american": amer,
                    "odds_decimal": dec,
                    "line": line,
                    "favorite_underdog": "favorite" if line < 0 else "underdog",
                    "odds_band": odds_band(amer),
                    "season_half": season_half(date),
                    "bet_result": result,
                    "source_input_file": r["source_input_file"],
                    "source_score_file": r["source_score_file"],
                })

        # TOTAL
        line = to_float(r["total"])
        projected = to_float(r["total_projected_goals"])
        probs = total_probabilities(line, projected)
        o_amer = to_float(r["dk_total_over_american"])
        u_amer = to_float(r["dk_total_under_american"])
        o_dec = resolve_decimal(r["dk_total_over_decimal"], o_amer)
        u_dec = resolve_decimal(r["dk_total_under_decimal"], u_amer)
        if probs:
            op, up, _ = probs
            oa = adjustment(configs, "total", "over", line=line)
            ua = adjustment(configs, "total", "under", line=line)
            prod = production_adjust_pair(op, up, oa, ua)
        else:
            prod = None

        if prod and o_dec > 1 and u_dec > 1:
            for side, prob, amer, dec in [
                ("over", prod[0], o_amer, o_dec),
                ("under", prod[1], u_amer, u_dec),
            ]:
                result = side_result("total", side, line, hs, aas)
                rows.append({
                    "game_id": game_id,
                    "game_date": date,
                    "market_type": "total",
                    "bet_side": side,
                    "model_prob": prob,
                    "odds_american": amer,
                    "odds_decimal": dec,
                    "line": line,
                    "favorite_underdog": "",
                    "odds_band": odds_band(amer),
                    "season_half": season_half(date),
                    "bet_result": result,
                    "source_input_file": r["source_input_file"],
                    "source_score_file": r["source_score_file"],
                })

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No independently reconstructed side rows.")
    return out.sort_values(["game_date", "game_id", "market_type", "bet_side"]).reset_index(drop=True)


def wilson_interval(wins, decisions, level=CI_LEVEL):
    if decisions <= 0:
        return np.nan, np.nan
    z = norm.ppf(1.0 - (1.0 - level) / 2.0)
    phat = wins / decisions
    denom = 1.0 + z * z / decisions
    center = (phat + z * z / (2.0 * decisions)) / denom
    half = z * math.sqrt(
        phat * (1.0 - phat) / decisions + z * z / (4.0 * decisions * decisions)
    ) / denom
    return center - half, center + half


def metrics(df):
    settled = df[df["bet_result"].isin(["Win", "Loss", "Push"])].copy()
    decisions = settled[settled["bet_result"].isin(["Win", "Loss"])].copy()

    sample_size = len(settled)
    decision_count = len(decisions)
    wins = int((settled["bet_result"] == "Win").sum())
    losses = int((settled["bet_result"] == "Loss").sum())
    pushes = int((settled["bet_result"] == "Push").sum())

    if decision_count:
        y = (decisions["bet_result"] == "Win").astype(float).to_numpy()
        p = decisions["model_prob"].astype(float).to_numpy()
        p_safe = np.clip(p, EPS, 1.0 - EPS)
        realized = float(y.mean())
        expected = float(p.mean())
        gap = realized - expected
        brier = float(np.mean((p - y) ** 2))
        ll = float(-np.mean(y * np.log(p_safe) + (1.0 - y) * np.log(1.0 - p_safe)))
        ci_low, ci_high = wilson_interval(wins, decision_count)
        gap_ci_low = ci_low - expected
        gap_ci_high = ci_high - expected
    else:
        realized = expected = gap = brier = ll = np.nan
        ci_low = ci_high = gap_ci_low = gap_ci_high = np.nan

    return {
        "sample_size": int(sample_size),
        "decision_count": int(decision_count),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "expected_win_rate": expected,
        "realized_win_rate": realized,
        "calibration_gap": gap,
        "brier_score": brier,
        "log_loss": ll,
        "realized_win_rate_ci95_low": ci_low,
        "realized_win_rate_ci95_high": ci_high,
        "calibration_gap_ci95_low": gap_ci_low,
        "calibration_gap_ci95_high": gap_ci_high,
        "expected_inside_realized_ci95": (
            bool(ci_low <= expected <= ci_high)
            if np.isfinite(expected) and np.isfinite(ci_low) and np.isfinite(ci_high)
            else False
        ),
    }


def group_metrics(df, group_cols, label_col=None):
    rows = []
    for keys, g in df.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {col: value for col, value in zip(group_cols, keys)}
        row.update(metrics(g))
        rows.append(row)
    return pd.DataFrame(rows)


def reliability_bucket(p):
    p = float(p)
    low = math.floor(p * 10.0) / 10.0
    if low >= 1.0:
        low = 0.9
    high = low + 0.1
    return f"{low:.1f}-{high:.1f}"


def build_reliability(side_rows):
    x = side_rows.copy()
    x["probability_bucket"] = x["model_prob"].map(reliability_bucket)
    out = group_metrics(x, ["market_type", "bet_side", "probability_bucket"])
    return out.sort_values(["market_type", "bet_side", "probability_bucket"]).reset_index(drop=True)


def build_time_split(side_rows):
    rows = []

    for (market, side), g in side_rows.groupby(["market_type", "bet_side"]):
        row = {
            "split_type": "season",
            "split_value": "2025-26",
            "market_type": market,
            "bet_side": side,
        }
        row.update(metrics(g))
        rows.append(row)

    for (half, market, side), g in side_rows.groupby(
        ["season_half", "market_type", "bet_side"]
    ):
        row = {
            "split_type": "season_half",
            "split_value": f"2025-26_{half}",
            "market_type": market,
            "bet_side": side,
        }
        row.update(metrics(g))
        rows.append(row)

    return pd.DataFrame(rows).sort_values(
        ["split_type", "split_value", "market_type", "bet_side"]
    ).reset_index(drop=True)


def compare_stored(independent, stored, keys, artifact_name):
    common_metrics = [
        ("sample_size", "sample_size"),
        ("decision_count", "decision_count"),
        ("wins", "wins"),
        ("losses", "losses"),
        ("pushes", "pushes"),
        ("realized_win_rate", "realized_win_rate"),
        ("expected_win_rate", "expected_probability"),
        ("calibration_gap", "calibration_gap"),
        ("brier_score", "brier_score"),
        ("log_loss", "log_loss"),
    ]

    merged = independent.merge(stored, on=keys, how="outer", suffixes=("_independent", "_stored"), indicator=True)
    out_rows = []

    for _, row in merged.iterrows():
        base = {k: row[k] for k in keys}
        base["artifact"] = artifact_name
        base["row_match_status"] = row["_merge"]

        for ind_col, stored_col in common_metrics:
            ind_name = f"{ind_col}_independent"
            stored_name = f"{stored_col}_stored"
            if ind_name not in row.index or stored_name not in row.index:
                continue
            ind_val = to_float(row[ind_name])
            stored_val = to_float(row[stored_name])
            diff = ind_val - stored_val if np.isfinite(ind_val) and np.isfinite(stored_val) else np.nan
            out_rows.append({
                **base,
                "metric": ind_col,
                "independent_value": ind_val,
                "stored_value": stored_val,
                "difference": diff,
                "absolute_difference": abs(diff) if np.isfinite(diff) else np.nan,
                "matches_1e_9": bool(abs(diff) <= 1e-9) if np.isfinite(diff) else False,
            })

    return pd.DataFrame(out_rows)


def persistence_diagnostics(time_split):
    halves = time_split[time_split["split_type"] == "season_half"].copy()
    rows = []

    for market, side in PRIMARY_GROUPS:
        first = halves[
            (halves["market_type"] == market)
            & (halves["bet_side"] == side)
            & (halves["split_value"] == "2025-26_first_half")
        ]
        second = halves[
            (halves["market_type"] == market)
            & (halves["bet_side"] == side)
            & (halves["split_value"] == "2025-26_second_half")
        ]
        if first.empty or second.empty:
            continue
        g1 = float(first.iloc[0]["calibration_gap"])
        g2 = float(second.iloc[0]["calibration_gap"])

        if g1 == 0 or g2 == 0:
            behavior = "zero_in_one_half"
        elif math.copysign(1, g1) == math.copysign(1, g2):
            behavior = "persists_same_direction"
        else:
            behavior = "reverses_direction"

        rows.append({
            "market_type": market,
            "bet_side": side,
            "first_half_gap": g1,
            "second_half_gap": g2,
            "time_behavior": behavior,
            "absolute_gap_change": abs(g2) - abs(g1),
        })

    return pd.DataFrame(rows)


def concentration_diagnostics(odds_metrics, line_metrics):
    rows = []

    for market, side in PRIMARY_GROUPS:
        od = odds_metrics[
            (odds_metrics["market_type"] == market)
            & (odds_metrics["bet_side"] == side)
            & (odds_metrics["decision_count"] >= MIN_CONCENTRATION_SAMPLE)
        ].copy()

        if not od.empty:
            top = od.loc[od["calibration_gap"].abs().idxmax()]
            odds_band_value = top["odds_band"]
            odds_gap = top["calibration_gap"]
            odds_n = int(top["decision_count"])
        else:
            odds_band_value = ""
            odds_gap = np.nan
            odds_n = 0

        ld = line_metrics[
            (line_metrics["market_type"] == market)
            & (line_metrics["bet_side"] == side)
            & (line_metrics["decision_count"] >= MIN_CONCENTRATION_SAMPLE)
        ].copy()

        if not ld.empty:
            top_line = ld.loc[ld["calibration_gap"].abs().idxmax()]
            line_value = top_line["line"]
            line_gap = top_line["calibration_gap"]
            line_n = int(top_line["decision_count"])
        else:
            line_value = np.nan
            line_gap = np.nan
            line_n = 0

        rows.append({
            "market_type": market,
            "bet_side": side,
            "largest_abs_gap_odds_band": odds_band_value,
            "odds_band_gap": odds_gap,
            "odds_band_decisions": odds_n,
            "largest_abs_gap_line_value": line_value,
            "line_value_gap": line_gap,
            "line_value_decisions": line_n,
            "min_decisions_for_concentration_flag": MIN_CONCENTRATION_SAMPLE,
        })

    return pd.DataFrame(rows)


def main():
    root = find_repo_root()
    output_dir = root / OUTPUT_REL
    output_dir.mkdir(parents=True, exist_ok=True)

    configs = current_configs(root)
    merged = load_merged(root)
    scores = load_scores(root)

    base = merged.merge(scores, on="game_id", how="inner", validate="one_to_one")
    side_rows = build_side_rows(base, configs)

    # Independent summaries built BEFORE stored aggregate CSVs are loaded.
    market_side = group_metrics(side_rows, ["market_type", "bet_side"])
    market_side = market_side.sort_values(["market_type", "bet_side"]).reset_index(drop=True)

    reliability = build_reliability(side_rows)
    time_split = build_time_split(side_rows)

    favorites_underdogs = group_metrics(
        side_rows[side_rows["favorite_underdog"].ne("")],
        ["market_type", "bet_side", "favorite_underdog"],
    ).sort_values(["market_type", "bet_side", "favorite_underdog"]).reset_index(drop=True)

    odds_metrics = group_metrics(
        side_rows,
        ["market_type", "bet_side", "odds_band"],
    )
    odds_metrics["odds_band_order"] = odds_metrics["odds_band"].map(ODDS_BAND_ORDER)
    odds_metrics = odds_metrics.sort_values(
        ["market_type", "bet_side", "odds_band_order"]
    ).drop(columns=["odds_band_order"]).reset_index(drop=True)

    line_source = side_rows[
        side_rows["market_type"].isin(["puck_line", "total"]) & side_rows["line"].notna()
    ].copy()
    line_metrics = group_metrics(
        line_source,
        ["market_type", "bet_side", "line"],
    ).sort_values(["market_type", "bet_side", "line"]).reset_index(drop=True)

    total_bands = line_metrics[line_metrics["market_type"] == "total"].copy()

    persistence = persistence_diagnostics(time_split)
    concentration = concentration_diagnostics(odds_metrics, line_metrics)
    bias_diagnostics = persistence.merge(
        concentration,
        on=["market_type", "bet_side"],
        how="outer",
    )

    # Only now read stored aggregate artifacts for independent verification.
    stored_market_side = pd.read_csv(root / STORED_REL / "market_side_summary.csv")
    stored_reliability = pd.read_csv(root / STORED_REL / "reliability_buckets.csv")
    stored_time = pd.read_csv(root / STORED_REL / "season_time_split.csv")

    compare_market = compare_stored(
        market_side,
        stored_market_side,
        ["market_type", "bet_side"],
        "market_side_summary.csv",
    )
    compare_reliability = compare_stored(
        reliability,
        stored_reliability,
        ["market_type", "bet_side", "probability_bucket"],
        "reliability_buckets.csv",
    )
    compare_time = compare_stored(
        time_split,
        stored_time,
        ["split_type", "split_value", "market_type", "bet_side"],
        "season_time_split.csv",
    )

    stored_comparison = pd.concat(
        [compare_market, compare_reliability, compare_time],
        ignore_index=True,
        sort=False,
    )

    # Coverage and validation summary.
    comparison_numeric = stored_comparison[
        stored_comparison["row_match_status"].eq("both")
        & stored_comparison["absolute_difference"].notna()
    ].copy()
    max_abs_diff = (
        float(comparison_numeric["absolute_difference"].max())
        if not comparison_numeric.empty
        else np.nan
    )
    exact_count = int(comparison_numeric["matches_1e_9"].sum())
    compared_count = len(comparison_numeric)

    market_side.to_csv(output_dir / "independent_market_side_summary.csv", index=False)
    reliability.to_csv(output_dir / "independent_reliability_buckets.csv", index=False)
    time_split.to_csv(output_dir / "independent_season_time_split.csv", index=False)
    favorites_underdogs.to_csv(output_dir / "favorites_underdogs.csv", index=False)
    odds_metrics.to_csv(output_dir / "sportsbook_odds_bands.csv", index=False)
    line_metrics.to_csv(output_dir / "line_value_calibration.csv", index=False)
    total_bands.to_csv(output_dir / "total_band_calibration.csv", index=False)
    persistence.to_csv(output_dir / "time_persistence.csv", index=False)
    concentration.to_csv(output_dir / "bias_concentration.csv", index=False)
    bias_diagnostics.to_csv(output_dir / "bias_diagnostics.csv", index=False)
    stored_comparison.to_csv(output_dir / "stored_aggregate_comparison.csv", index=False)
    side_rows.to_csv(output_dir / "independently_reconstructed_side_rows.csv", index=False)

    summary = [
        "NHL HISTORICAL SIDE-SPECIFIC PROBABILITY BIAS AUDIT",
        "===================================================",
        "research_only=true",
        "production_files_modified=false",
        "stored_aggregate_values_used_as_inputs=false",
        "stored_aggregate_values_used_only_for_post-calculation_comparison=true",
        f"historical_games_joined={base['game_id'].nunique()}",
        f"reconstructed_side_rows={len(side_rows)}",
        f"stored_numeric_values_compared={compared_count}",
        f"stored_numeric_values_matching_1e-9={exact_count}",
        f"stored_comparison_max_absolute_difference={max_abs_diff}",
        f"confidence_interval=Wilson {int(CI_LEVEL*100)}% interval for realized win rate",
        "calibration_gap_ci=Wilson realized-rate bounds minus mean expected probability",
        "season_half_definition=first_half Jul-Dec; second_half Jan-Jun (matches stored audit convention)",
        f"concentration_min_decisions={MIN_CONCENTRATION_SAMPLE}",
        "",
        "TIME PERSISTENCE",
    ]

    for r in persistence.to_dict("records"):
        summary.append(
            f"{r['market_type']} {r['bet_side']}: "
            f"first_gap={r['first_half_gap']:.6f} | "
            f"second_gap={r['second_half_gap']:.6f} | "
            f"{r['time_behavior']}"
        )

    summary += ["", "LARGEST ABSOLUTE BIAS CONCENTRATIONS (n>=30)"]
    for r in concentration.to_dict("records"):
        line_text = (
            f"{r['largest_abs_gap_line_value']} gap={r['line_value_gap']:.6f} n={r['line_value_decisions']}"
            if np.isfinite(to_float(r["line_value_gap"]))
            else "n/a"
        )
        odds_text = (
            f"{r['largest_abs_gap_odds_band']} gap={r['odds_band_gap']:.6f} n={r['odds_band_decisions']}"
            if np.isfinite(to_float(r["odds_band_gap"]))
            else "n/a"
        )
        summary.append(
            f"{r['market_type']} {r['bet_side']}: odds={odds_text} | line={line_text}"
        )

    summary += [
        "",
        "Artifacts:",
        str(output_dir / "independent_market_side_summary.csv"),
        str(output_dir / "independent_reliability_buckets.csv"),
        str(output_dir / "independent_season_time_split.csv"),
        str(output_dir / "favorites_underdogs.csv"),
        str(output_dir / "sportsbook_odds_bands.csv"),
        str(output_dir / "line_value_calibration.csv"),
        str(output_dir / "total_band_calibration.csv"),
        str(output_dir / "time_persistence.csv"),
        str(output_dir / "bias_concentration.csv"),
        str(output_dir / "bias_diagnostics.csv"),
        str(output_dir / "stored_aggregate_comparison.csv"),
        str(output_dir / "independently_reconstructed_side_rows.csv"),
    ]

    (output_dir / "summary.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")

    print("SIDE-SPECIFIC PROBABILITY BIAS AUDIT COMPLETE")
    print(f"Output: {output_dir}")
    print()
    print("Primary side summary:")
    print(
        market_side[
            [
                "market_type", "bet_side", "decision_count",
                "expected_win_rate", "realized_win_rate",
                "calibration_gap", "brier_score", "log_loss",
                "realized_win_rate_ci95_low", "realized_win_rate_ci95_high",
            ]
        ].to_string(index=False)
    )
    print()
    print("Time behavior:")
    print(persistence.to_string(index=False))
    print()
    print(
        f"Stored numeric values compared: {compared_count} | "
        f"matching within 1e-9: {exact_count} | "
        f"max abs difference: {max_abs_diff}"
    )


if __name__ == "__main__":
    main()
