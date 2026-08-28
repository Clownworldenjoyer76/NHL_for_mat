#!/usr/bin/env python3
# tests/hockey/nhl/test_nhl_pipeline.py

from __future__ import annotations

import copy
import importlib.util
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[6]
NHL_ROOT = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"


CORE_PATHS = [
    "docs/win/hockey/nhl/config/markets.yaml",
    "docs/win/hockey/nhl/config/mapping/team_map_nhl.csv",
    "docs/win/hockey/nhl/config/juice/nhl_moneyline_juice.csv",
    "docs/win/hockey/nhl/config/juice/nhl_puck_line_juice.csv",
    "docs/win/hockey/nhl/config/juice/nhl_total_juice.csv",
    "docs/win/hockey/nhl/scripts/00_intake/build_games.py",
    "docs/win/hockey/nhl/scripts/00_intake/transform_hockey_odds.py",
    "docs/win/hockey/nhl/scripts/00_intake/transform_hockey.py",
    "docs/win/hockey/nhl/scripts/02_juice/validate_juice_config.py",
    "docs/win/hockey/nhl/scripts/02_juice/apply_moneyline_juice.py",
    "docs/win/hockey/nhl/scripts/02_juice/apply_puck_line_juice.py",
    "docs/win/hockey/nhl/scripts/02_juice/apply_total_juice.py",
    "docs/win/hockey/nhl/scripts/03_edges/compute_edges.py",
    "docs/win/hockey/nhl/scripts/03_edges/compute_ev_kelly.py",
    "docs/win/hockey/nhl/scripts/04_select/validate_markets_config.py",
    "docs/win/hockey/nhl/scripts/04_select/hockey_select_bets.py",
    "docs/win/hockey/nhl/scripts/05_final_scores/01_nhl_results_grade.py",
    "docs/win/hockey/nhl/scripts/05_final_scores/03_nhl_results_reports.py",
]


def load_repo_module(relative_path: str):
    """
    Load a repository script as a uniquely named module.

    The NHL pipeline scripts are executable scripts rather than a Python
    package, so tests load them directly from their checked-in file paths.
    """
    path = REPO_ROOT / relative_path

    if not path.exists():
        pytest.fail(f"Required repository module does not exist: {relative_path}")

    module_name = (
        "nhl_pipeline_test_"
        + path.stem.replace("-", "_")
        + "_"
        + uuid.uuid4().hex
    )

    spec = importlib.util.spec_from_file_location(module_name, path)

    if spec is None or spec.loader is None:
        pytest.fail(f"Unable to load module spec for: {relative_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def write_reconciled_csv(
    path: Path,
    rows: list[dict[str, str]],
) -> None:
    columns = [
        "game_id",
        "sportsbook_event_id",
        "sport",
        "league",
        "game_date",
        "game_time",
        "home_team",
        "away_team",
    ]

    pd.DataFrame(
        rows,
        columns=columns,
    ).to_csv(
        path,
        index=False,
    )


def valid_reconciled_row(
    *,
    game_id: str = "2025020001",
    sportsbook_event_id: str = "book-1",
) -> dict[str, str]:
    return {
        "game_id": game_id,
        "sportsbook_event_id": sportsbook_event_id,
        "sport": "hockey",
        "league": "nhl",
        "game_date": "2026_01_01",
        "game_time": "19:00",
        "home_team": "Boston Bruins",
        "away_team": "New York Rangers",
    }


def permissive_side_rules() -> dict:
    return {
        "enabled": True,
        "prob_bands": [[0.0, 1.0]],
        "odds_bands": [[-10000.0, 10000.0]],
        "ev_bands": [[-1.0, 999.9999]],
        "kelly_bands": [[0.0, 999.9999]],
    }


def synthetic_moneyline_row() -> dict:
    return {
        "sport": "hockey",
        "league": "nhl",
        "game_date": "2026_01_01",
        "game_time": "19:00",
        "game_id": "2025020001",
        "away_team": "New York Rangers",
        "home_team": "Boston Bruins",
        "home_dk_moneyline_american": -110.0,
        "away_dk_moneyline_american": 105.0,
        "home_dk_moneyline_decimal": 1.91,
        "away_dk_moneyline_decimal": 2.05,
        "home_model_prob_moneyline": 0.55,
        "away_model_prob_moneyline": 0.50,
        "home_edge_pct_moneyline": 0.0264,
        "away_edge_pct_moneyline": 0.0122,
        "home_ev_moneyline": 0.0500,
        "away_ev_moneyline": 0.0250,
        "home_kelly_moneyline": 0.10,
        "away_kelly_moneyline": 0.05,
    }


# =====================================================================
# PATH / REPOSITORY LAYOUT
# =====================================================================

@pytest.mark.parametrize("relative_path", CORE_PATHS)
def test_required_nhl_pipeline_paths_exist(relative_path: str) -> None:
    assert (REPO_ROOT / relative_path).is_file(), relative_path


def test_nhl_root_is_canonical_docs_win_path() -> None:
    expected = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"
    assert NHL_ROOT == expected
    assert NHL_ROOT.is_dir()


# =====================================================================
# TEAM MAPPING / TRANSFORM HOCKEY
# Regression fixture: missing mappings.
# =====================================================================

def test_team_map_has_stable_official_identity_and_source_aliases() -> None:
    mapping_path = NHL_ROOT / "config" / "mapping" / "team_map_nhl.csv"
    df = pd.read_csv(mapping_path, dtype=str).fillna("")

    required = {
        "league",
        "source",
        "alias",
        "canonical_team",
        "nhl_team_id",
        "nhl_abbrev",
    }
    assert required.issubset(df.columns)

    official = df[
        (df["league"].str.lower() == "nhl")
        & (df["source"].str.lower() == "official_nhl")
    ].copy()

    assert len(official) == 32
    assert official["nhl_team_id"].ne("").all()
    assert official["nhl_team_id"].str.fullmatch(r"\d+").all()
    assert official["nhl_team_id"].nunique() == 32
    assert official["nhl_abbrev"].str.fullmatch(r"[A-Z]{3}").all()

    sources = set(df["source"].str.lower())
    assert "dratings" in sources
    assert "sportsbook" in sources


def test_transform_hockey_records_missing_mapping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # transform_hockey.py resets a relative log at import time. Import it
    # from a temporary working directory so the test does not touch repo logs.
    monkeypatch.chdir(tmp_path)

    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/00_intake/transform_hockey.py"
    )

    no_map_records: list[dict] = []

    result = module.normalize_team(
        "Mystery Expansion Club (10-2-1)",
        {},
        no_map_records,
        "fixture.csv",
    )

    assert result == "Mystery Expansion Club"
    assert len(no_map_records) == 1
    assert no_map_records[0]["source_file"] == "fixture.csv"
    assert no_map_records[0]["raw_team"] == "Mystery Expansion Club (10-2-1)"
    assert no_map_records[0]["stripped_team"] == "Mystery Expansion Club"
    assert (
        no_map_records[0]["normalized_attempt"]
        == "mystery expansion club"
    )


# =====================================================================
# BUILD GAMES
# Regression fixtures: blank game IDs and duplicate games.
# =====================================================================

def test_build_games_accepts_valid_canonical_game_id(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/00_intake/build_games.py"
    )
    module.LOG_PATH = tmp_path / "build_games.log"

    input_path = tmp_path / "NHL_2026_01_01.csv"
    write_reconciled_csv(
        input_path,
        [valid_reconciled_row()],
    )

    game_date, rows = module.read_reconciled_file(
        input_path,
        [],
    )

    assert game_date == "2026_01_01"
    assert len(rows) == 1
    assert rows[0]["game_id"] == "2025020001"


def test_build_games_rejects_blank_game_id(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/00_intake/build_games.py"
    )
    module.LOG_PATH = tmp_path / "build_games.log"

    row = valid_reconciled_row()
    row["game_id"] = ""

    input_path = tmp_path / "NHL_2026_01_01.csv"
    write_reconciled_csv(
        input_path,
        [row],
    )

    with pytest.raises(SystemExit):
        module.read_reconciled_file(
            input_path,
            [],
        )

    log_text = module.LOG_PATH.read_text(
        encoding="utf-8"
    )
    assert "missing values for: game_id" in log_text


def test_build_games_rejects_duplicate_official_game_id(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/00_intake/build_games.py"
    )
    module.LOG_PATH = tmp_path / "build_games.log"

    first = valid_reconciled_row(
        game_id="2025020001",
        sportsbook_event_id="book-1",
    )
    second = valid_reconciled_row(
        game_id="2025020001",
        sportsbook_event_id="book-2",
    )
    second["home_team"] = "Toronto Maple Leafs"
    second["away_team"] = "Montreal Canadiens"

    input_path = tmp_path / "NHL_2026_01_01.csv"
    write_reconciled_csv(
        input_path,
        [first, second],
    )

    with pytest.raises(SystemExit):
        module.read_reconciled_file(
            input_path,
            [],
        )

    log_text = module.LOG_PATH.read_text(
        encoding="utf-8"
    )
    assert "duplicate official game_id: 2025020001" in log_text


# =====================================================================
# SPORTSBOOK ODDS TRANSFORM
# Regression fixture: unsupported totals.
# =====================================================================

def test_transform_hockey_odds_rejects_unsupported_total_only() -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/00_intake/transform_hockey_odds.py"
    )

    rows = [
        {
            "hdp": 8.5,
            "over": 1.91,
            "under": 1.91,
        },
        {
            "hdp": 5.0,
            "over": 1.95,
            "under": 1.95,
        },
    ]

    assert module.pick_total_row_closest_odds(rows) == {}


def test_transform_hockey_odds_uses_supported_total_when_mixed() -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/00_intake/transform_hockey_odds.py"
    )

    unsupported = {
        "hdp": 8.5,
        "over": 1.90,
        "under": 1.90,
    }
    supported = {
        "hdp": 6.5,
        "over": 1.92,
        "under": 1.93,
    }

    selected = module.pick_total_row_closest_odds(
        [
            unsupported,
            supported,
        ]
    )

    assert selected == supported
    assert module.TOTAL_MIN == 5.5
    assert module.TOTAL_MAX == 7.5


# =====================================================================
# JUICE / CALIBRATION CONFIG
# There is no checked-in build_juice_files.py in the current Stage 02
# script directory. The source of truth is the three config CSVs plus
# validate_juice_config.py, so these tests lock those files and validator.
#
# Regression fixture: invalid calibration output.
# =====================================================================

def test_current_juice_config_files_pass_validator(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/02_juice/validate_juice_config.py"
    )
    module.LOG_FILE = tmp_path / "validate_juice_config.log"

    # main() validates all three checked-in config files together.
    module.main()

    log_text = module.LOG_FILE.read_text(
        encoding="utf-8"
    )
    assert "STATUS: SUCCESS" in log_text


def test_invalid_calibration_adjustment_is_rejected(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/02_juice/validate_juice_config.py"
    )
    module.LOG_FILE = tmp_path / "validate_juice_config.log"

    invalid_path = tmp_path / "invalid_moneyline_juice.csv"

    pd.DataFrame(
        [
            {
                "band": "fixture",
                "band_min": -200,
                "band_max": -100,
                "fav_ud": "favorite",
                "venue": "home",
                "model_calibration_adjustment": "not-a-number",
            }
        ]
    ).to_csv(
        invalid_path,
        index=False,
    )

    errors: list[str] = []

    loaded = module.load_config(
        invalid_path,
        module.MONEYLINE_REQUIRED,
        errors,
    )

    assert loaded is not None
    assert errors
    assert any(
        "INVALID NUMERIC VALUES" in message
        for message in errors
    )


# =====================================================================
# APPLY JUICE
# =====================================================================

def test_moneyline_juice_application_normalizes_probabilities(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/02_juice/apply_moneyline_juice.py"
    )

    module.OUTPUT_DIR = tmp_path / "out"
    module.ERROR_DIR = tmp_path / "errors"
    module.LOG_FILE = tmp_path / "apply_moneyline_juice.log"

    module.OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )
    module.ERROR_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    input_path = tmp_path / "2026_01_01_NHL_moneyline.csv"

    row = {
        col: ""
        for col in module.REQUIRED_INPUT_COLUMNS
    }
    row.update(
        {
            "sport": "hockey",
            "league": "nhl",
            "game_date": "2026_01_01",
            "game_time": "19:00",
            "game_id": "2025020001",
            "away_team": "New York Rangers",
            "home_team": "Boston Bruins",
            "away_prob_moneyline": 0.55,
            "home_prob_moneyline": 0.45,
            "away_fair_decimal_moneyline": 1 / 0.55,
            "home_fair_decimal_moneyline": 1 / 0.45,
            "away_dk_moneyline_american": -120,
            "home_dk_moneyline_american": 110,
            "away_dk_moneyline_decimal": 1.83,
            "home_dk_moneyline_decimal": 2.10,
        }
    )

    pd.DataFrame(
        [row],
        columns=module.REQUIRED_INPUT_COLUMNS,
    ).to_csv(
        input_path,
        index=False,
    )

    juice_df = pd.DataFrame(
        [
            {
                "band": "away_favorite",
                "band_min": -200,
                "band_max": -100,
                "fav_ud": "favorite",
                "venue": "away",
                "model_calibration_adjustment": 0.05,
            },
            {
                "band": "home_underdog",
                "band_min": 100,
                "band_max": 200,
                "fav_ud": "underdog",
                "venue": "home",
                "model_calibration_adjustment": 0.02,
            },
        ]
    )

    applied, skipped_bad, skipped_noband = module.process_file(
        input_path,
        juice_df,
    )

    assert applied == 1
    assert skipped_bad == 0
    assert skipped_noband == 0

    output_path = module.OUTPUT_DIR / input_path.name
    output = pd.read_csv(output_path)

    assert len(output) == 1

    away = float(
        output.loc[
            0,
            "away_normalized_prob_moneyline",
        ]
    )
    home = float(
        output.loc[
            0,
            "home_normalized_prob_moneyline",
        ]
    )

    assert away + home == pytest.approx(
        1.0,
        abs=1e-12,
    )
    assert 0.0 < away < 1.0
    assert 0.0 < home < 1.0


@pytest.mark.parametrize(
    "relative_path, config_filename",
    [
        (
            "docs/win/hockey/nhl/scripts/02_juice/apply_moneyline_juice.py",
            "nhl_moneyline_juice.csv",
        ),
        (
            "docs/win/hockey/nhl/scripts/02_juice/apply_puck_line_juice.py",
            "nhl_puck_line_juice.csv",
        ),
        (
            "docs/win/hockey/nhl/scripts/02_juice/apply_total_juice.py",
            "nhl_total_juice.csv",
        ),
    ],
)
def test_apply_juice_scripts_use_model_calibration_adjustment(
    relative_path: str,
    config_filename: str,
) -> None:
    module = load_repo_module(relative_path)

    assert (
        "model_calibration_adjustment"
        in module.REQUIRED_CONFIG_COLUMNS
    )
    assert module.JUICE_FILE.name == config_filename


# =====================================================================
# EDGE CALCULATIONS
# =====================================================================

def test_compute_edges_formula() -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/03_edges/compute_edges.py"
    )

    result = module.safe_edge_pct(
        pd.Series([2.0]),
        pd.Series([0.60]),
    )

    # model_prob - implied_prob = 0.60 - (1 / 2.0) = 0.10
    assert float(result.iloc[0]) == pytest.approx(
        0.10,
        abs=1e-12,
    )


@pytest.mark.parametrize(
    "decimal, probability",
    [
        (1.0, 0.60),
        (2.0, 0.0),
        (2.0, 1.0),
        (np.nan, 0.60),
        (2.0, np.nan),
    ],
)
def test_compute_edges_invalid_inputs_return_nan(
    decimal,
    probability,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/03_edges/compute_edges.py"
    )

    result = module.safe_edge_pct(
        pd.Series([decimal]),
        pd.Series([probability]),
    )

    assert pd.isna(result.iloc[0])


# =====================================================================
# EV / KELLY
# =====================================================================

def test_compute_ev_and_kelly_formulas(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/03_edges/compute_ev_kelly.py"
    )
    module.LOG_FILE = tmp_path / "compute_ev_kelly.log"

    probability = pd.Series([0.60])
    decimal = pd.Series([2.0])

    ev = module.compute_ev(
        probability,
        decimal,
    )
    kelly, negative_count = module.compute_kelly(
        probability,
        decimal,
        "fixture.csv",
    )

    # EV = p*d - 1 = 0.60*2 - 1 = 0.20
    assert float(ev.iloc[0]) == pytest.approx(
        0.20,
        abs=1e-12,
    )

    # Full Kelly for p=.60 and decimal=2.0 is .20.
    assert float(kelly.iloc[0]) == pytest.approx(
        0.20,
        abs=1e-12,
    )
    assert negative_count == 0


def test_negative_kelly_is_clipped_to_zero(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/03_edges/compute_ev_kelly.py"
    )
    module.LOG_FILE = tmp_path / "compute_ev_kelly.log"

    kelly, negative_count = module.compute_kelly(
        pd.Series([0.40]),
        pd.Series([2.0]),
        "fixture.csv",
    )

    assert float(kelly.iloc[0]) == 0.0
    assert negative_count == 1


# =====================================================================
# MARKETS CONFIG
# =====================================================================

def test_current_markets_yaml_passes_validator(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/04_select/validate_markets_config.py"
    )
    module.LOG_FILE = tmp_path / "validate_markets_config.log"

    errors: list[str] = []
    nhl_config = module.load_config(errors)

    assert nhl_config is not None

    module.validate_config(
        nhl_config,
        errors,
    )

    assert errors == []


def test_markets_validator_rejects_invalid_pick_preference(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/04_select/validate_markets_config.py"
    )
    module.LOG_FILE = tmp_path / "validate_markets_config.log"

    errors: list[str] = []
    nhl_config = module.load_config(errors)

    assert nhl_config is not None
    assert errors == []

    invalid = copy.deepcopy(nhl_config)
    invalid["moneyline"]["pick_preference"] = "not-valid"

    module.validate_config(
        invalid,
        errors,
    )

    assert any(
        "pick_preference INVALID" in message
        for message in errors
    )


# =====================================================================
# BET SELECTION
# Regression fixture: dual selections and blank game IDs.
# =====================================================================

def test_dual_moneyline_selections_allowed_when_pick_preference_all() -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/04_select/hockey_select_bets.py"
    )

    rules = permissive_side_rules()

    config = {
        "moneyline": {
            "enabled": True,
            "pick_preference": "all",
            "home": copy.deepcopy(rules),
            "away": copy.deepcopy(rules),
        }
    }

    rejections: dict = {}

    selected = module.process_moneyline(
        synthetic_moneyline_row(),
        config,
        "fixture_slate",
        rejections,
    )

    assert len(selected) == 2
    assert {
        row["bet_side"]
        for row in selected
    } == {
        "home",
        "away",
    }
    assert rejections == {}


def test_best_ev_reduces_dual_moneyline_selection_to_one() -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/04_select/hockey_select_bets.py"
    )

    rules = permissive_side_rules()

    config = {
        "moneyline": {
            "enabled": True,
            "pick_preference": "best_ev",
            "home": copy.deepcopy(rules),
            "away": copy.deepcopy(rules),
        }
    }

    row = synthetic_moneyline_row()
    row["home_ev_moneyline"] = 0.08
    row["away_ev_moneyline"] = 0.03

    rejections: dict = {}

    selected = module.process_moneyline(
        row,
        config,
        "fixture_slate",
        rejections,
    )

    assert len(selected) == 1
    assert selected[0]["bet_side"] == "home"

    assert rejections[
        (
            "2026_01_01",
            "moneyline",
            "away",
            "pick_preference",
        )
    ] == 1


def test_selector_rejects_blank_game_id(
    tmp_path: Path,
) -> None:
    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/04_select/hockey_select_bets.py"
    )

    module.INPUT_DIR = tmp_path
    module.LOG_FILE = tmp_path / "hockey_select_bets.log"

    input_path = tmp_path / "fixture_moneyline.csv"

    pd.DataFrame(
        [
            {
                "game_id": "",
                "dummy": "value",
            }
        ]
    ).to_csv(
        input_path,
        index=False,
    )

    with pytest.raises(SystemExit):
        module.read_market_file(
            input_path,
            "moneyline",
        )

    log_text = module.LOG_FILE.read_text(
        encoding="utf-8"
    )
    assert "blank game_id" in log_text


# =====================================================================
# FINAL SCORES / GRADING
# Regression fixture: unmatched grading rows.
# =====================================================================

@pytest.mark.parametrize(
    "row, expected",
    [
        (
            {
                "market_type": "moneyline",
                "bet_side": "home",
                "away_score": 2,
                "home_score": 4,
            },
            "Win",
        ),
        (
            {
                "market_type": "puck_line",
                "bet_side": "away",
                "line": 1.5,
                "away_score": 3,
                "home_score": 4,
                "away_puck_line_result": -1,
            },
            "Win",
        ),
        (
            {
                "market_type": "total",
                "bet_side": "under",
                "line": 6.5,
                "away_score": 2,
                "home_score": 3,
                "total_score": 5,
            },
            "Win",
        ),
        (
            {
                "market_type": "total",
                "bet_side": "over",
                "line": 6.0,
                "away_score": 2,
                "home_score": 4,
                "total_score": 6,
            },
            "Push",
        ),
    ],
)
def test_final_score_outcome_logic(
    row: dict,
    expected: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The grading script creates relative output directories at import.
    # Keep those side effects inside pytest's temporary directory.
    monkeypatch.chdir(tmp_path)

    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/05_final_scores/01_nhl_results_grade.py"
    )

    module.GRADE_ERROR_LOG = tmp_path / "grade_errors.log"
    module.GRADE_SUMMARY_LOG = tmp_path / "grade_summary.log"

    assert module.determine_outcome(row) == expected


def test_unmatched_grading_row_is_preserved_as_unresolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    module = load_repo_module(
        "docs/win/hockey/nhl/scripts/05_final_scores/01_nhl_results_grade.py"
    )

    module.GRADE_ERROR_LOG = tmp_path / "grade_errors.log"
    module.GRADE_SUMMARY_LOG = tmp_path / "grade_summary.log"

    bets = pd.DataFrame(
        [
            {
                "game_id": "2025020001",
                "game_date": "2026_01_01",
                "market_type": "moneyline",
                "bet_side": "home",
            }
        ]
    )

    scores = pd.DataFrame(
        columns=[
            "game_id",
        ]
    )
    statuses = pd.DataFrame(
        columns=[
            "game_id",
        ]
    )

    graded, pending, unresolved = module.grade_rows(
        bets,
        scores,
        statuses,
    )

    assert graded.empty
    assert pending.empty
    assert len(unresolved) == 1
    assert (
        unresolved.iloc[0]["game_id"]
        == "2025020001"
    )
    assert (
        unresolved.iloc[0]["unresolved_reason"]
        == "official_game_status_missing"
    )

    # Lossless regression check: the selected row must not disappear.
    assert (
        len(graded)
        + len(pending)
        + len(unresolved)
        == len(bets)
    )

