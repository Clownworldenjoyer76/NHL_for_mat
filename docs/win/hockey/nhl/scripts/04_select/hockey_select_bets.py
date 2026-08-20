#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/04_select/hockey_select_bets.py

import sys
import traceback
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
import yaml


INPUT_DIR = Path("docs/win/hockey/nhl/03_edges/ev_kelly")
OUTPUT_DIR = Path("docs/win/hockey/nhl/04_select")
CONFIG_PATH = Path("docs/win/hockey/nhl/config/markets.yaml")

ERROR_DIR = Path("docs/win/hockey/nhl/errors/04_select")
LOG_FILE = ERROR_DIR / "hockey_select_bets.txt"

LEAGUE_CODE = "NHL"

BLOCKED_PATH_PARTS = {
    "05_final_scores",
    "graded",
    "results",
    "reports",
}

OUTPUT_COLUMNS = [
    "sport",
    "league",
    "game_date",
    "game_time",
    "game_id",
    "away_team",
    "home_team",
    "market_type",
    "bet_side",
    "line",
    "take_bet",
    "dk_odds_american",
    "dk_odds_decimal",
    "model_prob",
    "edge",
    "ev",
    "kelly",
]


def _now():
    return datetime.now(UTC).isoformat()


def _log(msg: str, level: str = "INFO"):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {msg.rstrip()}\n")


def fail(msg: str):
    _log(msg, "ERROR")
    raise SystemExit(msg)


def path_parts(path: Path) -> set:
    return set(path.as_posix().split("/"))


def assert_read_path(path: Path):
    parts = path_parts(path)
    blocked = sorted(parts & BLOCKED_PATH_PARTS)
    if blocked:
        fail(f"Blocked read path contains forbidden folder(s): {path} | blocked={blocked}")

    allowed_roots = [
        INPUT_DIR.as_posix(),
        CONFIG_PATH.as_posix(),
    ]

    p = path.as_posix()
    if not (p.startswith(INPUT_DIR.as_posix() + "/") or p == CONFIG_PATH.as_posix()):
        fail(f"Blocked read path outside allowed Stage 04 inputs/config: {path}")


def assert_write_path(path: Path):
    parts = path_parts(path)
    blocked = sorted(parts & BLOCKED_PATH_PARTS)
    if blocked:
        fail(f"Blocked write path contains forbidden folder(s): {path} | blocked={blocked}")

    p = path.as_posix()
    allowed_output = OUTPUT_DIR.as_posix()
    allowed_log = ERROR_DIR.as_posix()

    if not (p.startswith(allowed_output + "/") or p.startswith(allowed_log + "/")):
        fail(f"Blocked write path outside allowed Stage 04 output/log folders: {path}")


def ensure_dirs():
    assert_write_path(OUTPUT_DIR / "dummy.csv")
    assert_write_path(LOG_FILE)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)


def reset_log():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== NHL hockey_select_bets RUN {_now()} ===\n")


def load_config():
    assert_read_path(CONFIG_PATH)

    if not CONFIG_PATH.exists():
        fail(f"Config file not found: {CONFIG_PATH}")

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        fail(f"Malformed YAML in {CONFIG_PATH}: {e}")

    try:
        return raw["markets"]["nhl"]
    except Exception as e:
        fail(f"Missing expected config path markets -> nhl in {CONFIG_PATH}: {e}")


def fv(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def sv(x):
    if pd.isna(x):
        return ""
    return str(x)


def in_range(val, ranges):
    if val is None:
        return False
    if ranges is None:
        return True
    return any(float(lo) <= val <= float(hi) for lo, hi in ranges)


def check_side_rules(
    *,
    rules: dict,
    odds,
    line,
    prob,
    edge,
    ev,
    kelly,
    check_line: bool,
):
    if not rules.get("enabled", False):
        return False

    if not in_range(odds, rules.get("odds_bands", [])):
        return False

    if check_line and not in_range(line, rules.get("line_bands", [])):
        return False

    if not in_range(prob, rules.get("prob_bands", [])):
        return False

    if not in_range(edge, rules.get("edge_bands", None)):
        return False

    if not in_range(ev, rules.get("ev_bands", [])):
        return False

    if not in_range(kelly, rules.get("kelly_bands", [])):
        return False

    return True


def require_columns(df: pd.DataFrame, cols: list[str], market_type: str, path: Path):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        fail(f"{market_type} missing required column(s): {missing} | file={path}")


def read_market_file(path: Path, market_type: str):
    if not path.exists():
        return None

    assert_read_path(path)

    try:
        df = pd.read_csv(path)
    except Exception as e:
        fail(f"Failed reading {market_type} file: {path} | {e}")

    if "game_id" not in df.columns:
        fail(f"{market_type} file missing game_id: {path}")

    if df["game_id"].isna().any():
        fail(f"{market_type} file has blank game_id: {path}")

    dupes = df[df.duplicated(subset=["game_id"], keep=False)]
    if not dupes.empty:
        ids = sorted(dupes["game_id"].astype(str).unique().tolist())
        fail(f"multiple {market_type} rows for one game_id | file={path} | game_id={ids}")

    return df


def get_base_meta(row):
    return {
        "sport": sv(row.get("sport")),
        "league": sv(row.get("league")),
        "game_date": sv(row.get("game_date")),
        "game_time": sv(row.get("game_time")),
        "game_id": row.get("game_id"),
        "away_team": sv(row.get("away_team")),
        "home_team": sv(row.get("home_team")),
    }


def apply_pick_preference(candidates: list[dict], pick_preference: str, slate_key: str, game_id, market_type: str):
    if not candidates:
        return []

    if pick_preference == "all":
        return candidates

    if pick_preference == "best_ev":
        max_ev = max(c["ev"] for c in candidates)
        winners = [c for c in candidates if c["ev"] == max_ev]
    elif pick_preference == "best_prob":
        max_prob = max(c["model_prob"] for c in candidates)
        winners = [c for c in candidates if c["model_prob"] == max_prob]
    else:
        fail(
            f"Invalid pick_preference for {market_type}: {pick_preference} | "
            f"slate={slate_key} | game_id={game_id}"
        )

    if len(winners) > 1:
        fail(
            f"pick_preference tie for {market_type} | preference={pick_preference} | "
            f"slate={slate_key} | game_id={game_id}"
        )

    return winners


def process_moneyline(row, config, slate_key):
    market_config = config.get("moneyline", {})
    if not market_config.get("enabled", False):
        return []

    require_keys = ["home", "away"]
    for key in require_keys:
        if key not in market_config:
            fail(f"moneyline config missing side: {key}")

    candidates = []
    meta = get_base_meta(row)

    for side in ["home", "away"]:
        side_rules = market_config[side]

        odds = fv(row.get(f"{side}_dk_moneyline_american"))
        dec = fv(row.get(f"{side}_dk_moneyline_decimal"))
        prob = fv(row.get(f"{side}_model_prob_moneyline"))
        edge = fv(row.get(f"{side}_edge_decimal_moneyline"))
        ev = fv(row.get(f"{side}_ev_moneyline"))
        kelly = fv(row.get(f"{side}_kelly_moneyline"))

        if not check_side_rules(
            rules=side_rules,
            odds=odds,
            line=None,
            prob=prob,
            edge=edge,
            ev=ev,
            kelly=kelly,
            check_line=False,
        ):
            continue

        candidates.append({
            **meta,
            "market_type": "moneyline",
            "bet_side": side,
            "line": "",
            "take_bet": f"{side}_moneyline",
            "dk_odds_american": odds,
            "dk_odds_decimal": dec,
            "model_prob": prob,
            "edge": edge,
            "ev": ev,
            "kelly": kelly,
        })

    return apply_pick_preference(
        candidates,
        market_config.get("pick_preference", "all"),
        slate_key,
        row.get("game_id"),
        "moneyline",
    )


def process_puck_line(row, config, slate_key):
    market_config = config.get("puck_line", {})
    if not market_config.get("enabled", False):
        return []

    require_keys = ["home", "away"]
    for key in require_keys:
        if key not in market_config:
            fail(f"puck_line config missing side: {key}")

    candidates = []
    meta = get_base_meta(row)

    for side in ["home", "away"]:
        side_rules = market_config[side]

        odds = fv(row.get(f"{side}_dk_puck_line_american"))
        dec = fv(row.get(f"{side}_dk_puck_line_decimal"))
        line = fv(row.get(f"{side}_puck_line"))
        prob = fv(row.get(f"{side}_model_prob_puck_line"))
        edge = fv(row.get(f"{side}_edge_decimal_puck_line"))
        ev = fv(row.get(f"{side}_ev_puck_line"))
        kelly = fv(row.get(f"{side}_kelly_puck_line"))

        if not check_side_rules(
            rules=side_rules,
            odds=odds,
            line=line,
            prob=prob,
            edge=edge,
            ev=ev,
            kelly=kelly,
            check_line=True,
        ):
            continue

        candidates.append({
            **meta,
            "market_type": "puck_line",
            "bet_side": side,
            "line": line,
            "take_bet": f"{side}_puck_line",
            "dk_odds_american": odds,
            "dk_odds_decimal": dec,
            "model_prob": prob,
            "edge": edge,
            "ev": ev,
            "kelly": kelly,
        })

    return apply_pick_preference(
        candidates,
        market_config.get("pick_preference", "all"),
        slate_key,
        row.get("game_id"),
        "puck_line",
    )


def process_total(row, config, slate_key):
    market_config = config.get("total", {})
    if not market_config.get("enabled", False):
        return []

    require_keys = ["over", "under"]
    for key in require_keys:
        if key not in market_config:
            fail(f"total config missing side: {key}")

    candidates = []
    meta = get_base_meta(row)

    for side in ["over", "under"]:
        side_rules = market_config[side]

        odds = fv(row.get(f"dk_total_{side}_american"))
        dec = fv(row.get(f"dk_total_{side}_decimal"))
        line = fv(row.get("total"))
        prob = fv(row.get(f"{side}_model_prob_total"))
        edge = fv(row.get(f"{side}_edge_decimal_total"))
        ev = fv(row.get(f"{side}_ev_total"))
        kelly = fv(row.get(f"{side}_kelly_total"))

        if not check_side_rules(
            rules=side_rules,
            odds=odds,
            line=line,
            prob=prob,
            edge=edge,
            ev=ev,
            kelly=kelly,
            check_line=True,
        ):
            continue

        candidates.append({
            **meta,
            "market_type": "total",
            "bet_side": side,
            "line": line,
            "take_bet": f"{side}_total",
            "dk_odds_american": odds,
            "dk_odds_decimal": dec,
            "model_prob": prob,
            "edge": edge,
            "ev": ev,
            "kelly": kelly,
        })

    return apply_pick_preference(
        candidates,
        market_config.get("pick_preference", "all"),
        slate_key,
        row.get("game_id"),
        "total",
    )


def wipe_outputs():
    for old in OUTPUT_DIR.glob("*.csv"):
        assert_write_path(old)
        old.unlink()


def find_slates():
    assert_read_path(INPUT_DIR / "dummy.csv")

    files = sorted(INPUT_DIR.glob("*_NHL_*.csv"))
    slates = {}

    for fpath in files:
        assert_read_path(fpath)

        name = fpath.name

        if name.endswith("_NHL_moneyline.csv"):
            slate_key = name.replace("_NHL_moneyline.csv", "")
            slates.setdefault(slate_key, {})["moneyline"] = fpath
        elif name.endswith("_NHL_puck_line.csv"):
            slate_key = name.replace("_NHL_puck_line.csv", "")
            slates.setdefault(slate_key, {})["puck_line"] = fpath
        elif name.endswith("_NHL_total.csv"):
            slate_key = name.replace("_NHL_total.csv", "")
            slates.setdefault(slate_key, {})["total"] = fpath

    return slates


def validate_market_columns(df, market_type, path):
    base_cols = [
        "sport",
        "league",
        "game_date",
        "game_time",
        "game_id",
        "away_team",
        "home_team",
    ]

    if market_type == "moneyline":
        cols = base_cols + [
            "away_dk_moneyline_american",
            "home_dk_moneyline_american",
            "away_dk_moneyline_decimal",
            "home_dk_moneyline_decimal",
            "away_model_prob_moneyline",
            "home_model_prob_moneyline",
            "away_edge_decimal_moneyline",
            "home_edge_decimal_moneyline",
            "away_ev_moneyline",
            "home_ev_moneyline",
            "away_kelly_moneyline",
            "home_kelly_moneyline",
        ]
    elif market_type == "puck_line":
        cols = base_cols + [
            "away_puck_line",
            "home_puck_line",
            "away_dk_puck_line_american",
            "home_dk_puck_line_american",
            "away_dk_puck_line_decimal",
            "home_dk_puck_line_decimal",
            "away_model_prob_puck_line",
            "home_model_prob_puck_line",
            "away_edge_decimal_puck_line",
            "home_edge_decimal_puck_line",
            "away_ev_puck_line",
            "home_ev_puck_line",
            "away_kelly_puck_line",
            "home_kelly_puck_line",
        ]
    elif market_type == "total":
        cols = base_cols + [
            "total",
            "dk_total_over_american",
            "dk_total_under_american",
            "dk_total_over_decimal",
            "dk_total_under_decimal",
            "over_model_prob_total",
            "under_model_prob_total",
            "over_edge_decimal_total",
            "under_edge_decimal_total",
            "over_ev_total",
            "under_ev_total",
            "over_kelly_total",
            "under_kelly_total",
        ]
    else:
        fail(f"Unknown market_type during validation: {market_type}")

    require_columns(df, cols, market_type, path)


def row_for_game(df, game_id, market_type):
    if df is None:
        return None

    match = df[df["game_id"].astype(str) == str(game_id)]

    if len(match) > 1:
        fail(f"multiple {market_type} rows for one game_id | game_id={game_id}")

    if len(match) == 0:
        return None

    return match.iloc[0]


def process_slate(slate_key, paths, config):
    _log(f"--- SLATE: {slate_key}")

    ml_path = paths.get("moneyline")
    pl_path = paths.get("puck_line")
    td_path = paths.get("total")

    ml_df = read_market_file(ml_path, "moneyline") if ml_path else None
    pl_df = read_market_file(pl_path, "puck_line") if pl_path else None
    td_df = read_market_file(td_path, "total") if td_path else None

    if ml_df is not None:
        validate_market_columns(ml_df, "moneyline", ml_path)
    else:
        _log(f"{slate_key} missing moneyline file — skipping moneyline only", "WARN")

    if pl_df is not None:
        validate_market_columns(pl_df, "puck_line", pl_path)
    else:
        _log(f"{slate_key} missing puck_line file — skipping puck_line only", "WARN")

    if td_df is not None:
        validate_market_columns(td_df, "total", td_path)
    else:
        _log(f"{slate_key} missing total file — skipping total only", "WARN")

    game_ids = set()

    for df in [ml_df, pl_df, td_df]:
        if df is not None:
            game_ids.update(df["game_id"].astype(str).tolist())

    game_ids = sorted(game_ids)

    final_rows = []

    for game_id in game_ids:
        ml_row = row_for_game(ml_df, game_id, "moneyline")
        pl_row = row_for_game(pl_df, game_id, "puck_line")
        td_row = row_for_game(td_df, game_id, "total")

        if ml_row is not None:
            final_rows.extend(process_moneyline(ml_row, config, slate_key))

        if pl_row is not None:
            final_rows.extend(process_puck_line(pl_row, config, slate_key))

        if td_row is not None:
            final_rows.extend(process_total(td_row, config, slate_key))

    out_path = OUTPUT_DIR / f"{slate_key}_NHL.csv"
    assert_write_path(out_path)

    df_out = pd.DataFrame(final_rows, columns=OUTPUT_COLUMNS)
    df_out.to_csv(out_path, index=False)

    ml_count = int((df_out["market_type"] == "moneyline").sum()) if not df_out.empty else 0
    pl_count = int((df_out["market_type"] == "puck_line").sum()) if not df_out.empty else 0
    td_count = int((df_out["market_type"] == "total").sum()) if not df_out.empty else 0

    _log(
        f"WROTE: {out_path} | bets={len(df_out)} | "
        f"moneyline={ml_count} | puck_line={pl_count} | total={td_count}"
    )

    return {
        "slate": slate_key,
        "bets": len(df_out),
        "moneyline": ml_count,
        "puck_line": pl_count,
        "total": td_count,
    }


def write_summary(summary_rows):
    total_slates = len(summary_rows)
    total_bets = sum(r["bets"] for r in summary_rows)
    total_ml = sum(r["moneyline"] for r in summary_rows)
    total_pl = sum(r["puck_line"] for r in summary_rows)
    total_td = sum(r["total"] for r in summary_rows)

    lines = [
        "",
        "=" * 60,
        f"SUMMARY {_now()}",
        "=" * 60,
        f"slates_written : {total_slates}",
        f"total_bets     : {total_bets}",
        f"moneyline_bets : {total_ml}",
        f"puck_line_bets : {total_pl}",
        f"total_bets_mkt : {total_td}",
        "",
        f"{'slate':<20} {'bets':>6} {'ml':>6} {'pl':>6} {'total':>6}",
    ]

    for r in summary_rows:
        lines.append(
            f"{r['slate']:<20} {r['bets']:>6} "
            f"{r['moneyline']:>6} {r['puck_line']:>6} {r['total']:>6}"
        )

    lines.extend([
        "",
        "STATUS: SUCCESS",
        "=" * 60,
    ])

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    ensure_dirs()
    reset_log()

    try:
        config = load_config()

        _log(f"INPUT_DIR: {INPUT_DIR}")
        _log(f"OUTPUT_DIR: {OUTPUT_DIR}")
        _log(f"CONFIG_PATH: {CONFIG_PATH}")
        _log(f"LOG_FILE: {LOG_FILE}")

        wipe_outputs()

        slates = find_slates()
        _log(f"Slates found: {len(slates)}")

        summary_rows = []

        for slate_key in sorted(slates):
            summary_rows.append(process_slate(slate_key, slates[slate_key], config))

        write_summary(summary_rows)

        print("hockey_select_bets complete.")

    except SystemExit:
        raise
    except Exception as e:
        try:
            _log(f"FATAL: {e}\n{traceback.format_exc()}", "ERROR")
        except Exception:
            pass
        raise SystemExit(1)


if __name__ == "__main__":
    main()
