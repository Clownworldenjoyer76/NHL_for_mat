#!/usr/bin/env python3
"""
Research-only price-aware moneyline secondary-support logic.

This module does not modify production Stage 04. It provides the proposed
moneyline support rule for research and deterministic regression tests.

Production rule being compared:
    home probability > 0.50 -> home support
    home probability < 0.50 -> away support

Research rule:
    side_probability > (1 / offered_decimal_odds) + safety_margin

The default safety margin is 0.0 because no additional margin has been
validated by this checklist item.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


EPS = 1e-12
DEFAULT_SAFETY_MARGIN = 0.0


@dataclass(frozen=True)
class MoneylineSupportResult:
    bet_side: str
    home_probability: float
    side_probability: float
    decimal_odds: float
    break_even_probability: float
    safety_margin: float
    required_probability: float
    label: str


def _finite_float(value, *, name: str) -> float:
    try:
        result = float(value)
    except Exception as exc:
        raise ValueError(f"{name} must be numeric") from exc

    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")

    return result


def current_50pct_moneyline_support_label(
    *,
    bet_side: str,
    home_probability,
) -> str:
    """
    Reproduce the current Stage 04 moneyline support rule exactly enough for
    research comparison.
    """
    side = str(bet_side).strip().lower()
    if side not in {"home", "away"}:
        raise ValueError("bet_side must be 'home' or 'away'")

    probability = _finite_float(
        home_probability,
        name="home_probability",
    )

    if not 0.0 <= probability <= 1.0:
        raise ValueError("home_probability must be between 0 and 1")

    if abs(probability - 0.5) < EPS:
        return "neutral"

    supports_home = probability > 0.5
    supports = supports_home if side == "home" else not supports_home
    return "supports" if supports else "opposes"


def price_aware_moneyline_support(
    *,
    bet_side: str,
    home_probability,
    decimal_odds,
    safety_margin: float = DEFAULT_SAFETY_MARGIN,
) -> MoneylineSupportResult:
    """
    Evaluate secondary moneyline support against sportsbook break-even price.

    For a home bet:
        side_probability = home_probability

    For an away bet:
        side_probability = 1 - home_probability

    Economic support:
        side_probability > 1 / decimal_odds + safety_margin

    Equality within EPS is neutral.
    """
    side = str(bet_side).strip().lower()
    if side not in {"home", "away"}:
        raise ValueError("bet_side must be 'home' or 'away'")

    home_probability = _finite_float(
        home_probability,
        name="home_probability",
    )
    decimal_odds = _finite_float(
        decimal_odds,
        name="decimal_odds",
    )
    safety_margin = _finite_float(
        safety_margin,
        name="safety_margin",
    )

    if not 0.0 <= home_probability <= 1.0:
        raise ValueError("home_probability must be between 0 and 1")

    if decimal_odds <= 1.0:
        raise ValueError("decimal_odds must be greater than 1")

    if safety_margin < 0.0:
        raise ValueError("safety_margin cannot be negative")

    side_probability = (
        home_probability
        if side == "home"
        else 1.0 - home_probability
    )

    break_even_probability = 1.0 / decimal_odds
    required_probability = break_even_probability + safety_margin

    difference = side_probability - required_probability

    if abs(difference) < EPS:
        label = "neutral"
    elif difference > 0:
        label = "supports"
    else:
        label = "opposes"

    return MoneylineSupportResult(
        bet_side=side,
        home_probability=home_probability,
        side_probability=side_probability,
        decimal_odds=decimal_odds,
        break_even_probability=break_even_probability,
        safety_margin=safety_margin,
        required_probability=required_probability,
        label=label,
    )


def american_to_decimal(american_odds) -> float:
    odds = _finite_float(
        american_odds,
        name="american_odds",
    )

    if odds == 0:
        raise ValueError("american_odds cannot be zero")

    if odds > 0:
        return 1.0 + odds / 100.0

    return 1.0 + 100.0 / abs(odds)


def example_results() -> list[dict]:
    cases = [
        {
            "case": "home_+150_secondary_45pct",
            "bet_side": "home",
            "american_odds": 150,
            "home_probability": 0.45,
        },
        {
            "case": "home_-250_secondary_60pct",
            "bet_side": "home",
            "american_odds": -250,
            "home_probability": 0.60,
        },
    ]

    rows = []

    for case in cases:
        decimal_odds = american_to_decimal(
            case["american_odds"]
        )

        current = current_50pct_moneyline_support_label(
            bet_side=case["bet_side"],
            home_probability=case["home_probability"],
        )

        research = price_aware_moneyline_support(
            bet_side=case["bet_side"],
            home_probability=case["home_probability"],
            decimal_odds=decimal_odds,
        )

        rows.append(
            {
                **case,
                "decimal_odds": decimal_odds,
                "break_even_probability": research.break_even_probability,
                "current_50pct_label": current,
                "price_aware_label": research.label,
                "rules_differ": current != research.label,
            }
        )

    return rows


def main() -> None:
    for row in example_results():
        print(
            f"{row['case']} | "
            f"american={row['american_odds']:+g} | "
            f"decimal={row['decimal_odds']:.6f} | "
            f"secondary_home_prob={row['home_probability']:.6f} | "
            f"break_even={row['break_even_probability']:.6f} | "
            f"current={row['current_50pct_label']} | "
            f"price_aware={row['price_aware_label']} | "
            f"differ={row['rules_differ']}"
        )


if __name__ == "__main__":
    main()
