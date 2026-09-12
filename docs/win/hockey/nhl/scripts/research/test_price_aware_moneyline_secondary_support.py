#!/usr/bin/env python3
"""
Regression tests for the research-only price-aware moneyline support rule.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path

import pytest


def find_repo_root() -> Path:
    start = Path(__file__).resolve().parent
    for candidate in (start, *start.parents):
        if (candidate / "docs" / "win" / "hockey" / "nhl").is_dir():
            return candidate
    raise RuntimeError("Unable to locate NHL repository root")


REPO_ROOT = find_repo_root()
MODULE_PATH = (
    REPO_ROOT
    / "docs"
    / "win"
    / "hockey"
    / "nhl"
    / "scripts"
    / "research"
    / "price_aware_moneyline_secondary_support.py"
)


def load_module():
    name = f"price_aware_moneyline_support_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_home_plus_150_45pct_is_positive_economic_support() -> None:
    module = load_module()

    decimal_odds = module.american_to_decimal(150)
    result = module.price_aware_moneyline_support(
        bet_side="home",
        home_probability=0.45,
        decimal_odds=decimal_odds,
    )

    assert decimal_odds == pytest.approx(2.5)
    assert result.break_even_probability == pytest.approx(0.40)
    assert result.side_probability == pytest.approx(0.45)
    assert result.label == "supports"


def test_home_plus_150_45pct_differs_from_current_50pct_rule() -> None:
    module = load_module()

    current = module.current_50pct_moneyline_support_label(
        bet_side="home",
        home_probability=0.45,
    )
    research = module.price_aware_moneyline_support(
        bet_side="home",
        home_probability=0.45,
        decimal_odds=module.american_to_decimal(150),
    )

    assert current == "opposes"
    assert research.label == "supports"
    assert current != research.label


def test_home_minus_250_60pct_is_negative_ev_non_support() -> None:
    module = load_module()

    decimal_odds = module.american_to_decimal(-250)
    result = module.price_aware_moneyline_support(
        bet_side="home",
        home_probability=0.60,
        decimal_odds=decimal_odds,
    )

    assert decimal_odds == pytest.approx(1.4)
    assert result.break_even_probability == pytest.approx(1.0 / 1.4)
    assert result.break_even_probability == pytest.approx(0.7142857142857143)
    assert result.side_probability == pytest.approx(0.60)
    assert result.label == "opposes"


def test_home_minus_250_60pct_differs_from_current_50pct_rule() -> None:
    module = load_module()

    current = module.current_50pct_moneyline_support_label(
        bet_side="home",
        home_probability=0.60,
    )
    research = module.price_aware_moneyline_support(
        bet_side="home",
        home_probability=0.60,
        decimal_odds=module.american_to_decimal(-250),
    )

    assert current == "supports"
    assert research.label == "opposes"
    assert current != research.label


def test_away_support_uses_away_probability_against_away_price() -> None:
    module = load_module()

    result = module.price_aware_moneyline_support(
        bet_side="away",
        home_probability=0.60,
        decimal_odds=3.0,
    )

    assert result.side_probability == pytest.approx(0.40)
    assert result.break_even_probability == pytest.approx(1.0 / 3.0)
    assert result.label == "supports"


def test_optional_safety_margin_raises_required_probability() -> None:
    module = load_module()

    result = module.price_aware_moneyline_support(
        bet_side="home",
        home_probability=0.45,
        decimal_odds=2.5,
        safety_margin=0.06,
    )

    assert result.break_even_probability == pytest.approx(0.40)
    assert result.required_probability == pytest.approx(0.46)
    assert result.label == "opposes"
