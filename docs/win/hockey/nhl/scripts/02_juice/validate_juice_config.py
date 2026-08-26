#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/02_juice/validate_juice_config.py

import math
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_DIR = BASE_DIR / "config" / "juice"
INPUT_DIR = BASE_DIR / "01_merge" / "01_merguiced"
ERROR_DIR = BASE_DIR / "errors" / "02_juice"
LOG_FILE = ERROR_DIR / "validate_juice_config.txt"

ERROR_DIR.mkdir(parents=True, exist_ok=True)

MONEYLINE_FILE = CONFIG_DIR / "nhl_moneyline_juice.csv"
PUCK_LINE_FILE = CONFIG_DIR / "nhl_puck_line_juice.csv"
TOTAL_FILE = CONFIG_DIR / "nhl_total_juice.csv"

ADJUSTMENT_COLUMN = "model_calibration_adjustment"

MONEYLINE_REQUIRED = [
    "band",
    "band_min",
    "band_max",
    "fav_ud",
    "venue",
    ADJUSTMENT_COLUMN,
]

PUCK_LINE_REQUIRED = [
    "band",
    "band_min",
    "band_max",
    "venue",
    "fav_ud",
    ADJUSTMENT_COLUMN,
]

TOTAL_REQUIRED = [
    "band",
    "band_min",
    "band_max",
    "side",
    ADJUSTMENT_COLUMN,
]


###############################################################
###################### SUPPORTED DOMAIN #######################
###############################################################

MONEYLINE_FAVORITE_RANGE = range(-999, -99)
MONEYLINE_UNDERDOG_RANGE = range(100, 600)

PUCK_LINES = (
    -1.5,
    1.5,
)

TOTAL_LINES = (
    5.5,
    6.0,
    6.5,
    7.0,
    7.5,
)

VALID_VENUES = {
    "away",
    "home",
}

VALID_FAV_UD = {
    "favorite",
    "underdog",
}

VALID_TOTAL_SIDES = {
    "over",
    "under",
}


###############################################################
######################## LOGGING ##############################
###############################################################

def now() -> str:
    return datetime.now(UTC).isoformat()


def reset_log() -> None:
    LOG_FILE.write_text(
        f"=== validate_juice_config RUN {now()} ===\n",
        encoding="utf-8",
    )


def log(msg: str) -> None:
    with LOG_FILE.open(
        "a",
        encoding="utf-8",
    ) as f:
        f.write(
            f"{now()} | {msg}\n"
        )


def fail(
    errors: list[str],
    msg: str,
) -> None:
    errors.append(msg)
    log(
        f"ERROR | {msg}"
    )


###############################################################
######################## LOAD CONFIG ##########################
###############################################################

def load_config(
    path: Path,
    required: list[str],
    errors: list[str],
) -> pd.DataFrame | None:

    if not path.exists():
        fail(
            errors,
            f"MISSING CONFIG | {path}",
        )
        return None

    try:
        df = pd.read_csv(
            path,
            dtype=str,
        )

    except Exception as e:
        fail(
            errors,
            f"READ ERROR | {path} | {e}",
        )
        return None

    if df.empty:
        fail(
            errors,
            f"EMPTY CONFIG | {path}",
        )
        return None

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        fail(
            errors,
            f"MISSING COLUMNS | {path} | {missing}",
        )
        return None

    df = df.copy()

    for col in [
        "band_min",
        "band_max",
        ADJUSTMENT_COLUMN,
    ]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce",
        )

    for col in required:
        if col not in {
            "band_min",
            "band_max",
            ADJUSTMENT_COLUMN,
        }:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
            )

    bad_numeric = []

    for idx, row in df.iterrows():
        for col in [
            "band_min",
            "band_max",
            ADJUSTMENT_COLUMN,
        ]:
            value = row[col]

            if (
                pd.isna(value)
                or not math.isfinite(
                    float(value)
                )
            ):
                bad_numeric.append(
                    (
                        idx + 2,
                        col,
                    )
                )

    if bad_numeric:
        fail(
            errors,
            "INVALID NUMERIC VALUES | "
            f"{path.name} | "
            f"count={len(bad_numeric)} | "
            f"sample={bad_numeric[:10]}",
        )

    bad_ranges = df[
        df["band_min"].notna()
        & df["band_max"].notna()
        & (
            df["band_min"]
            > df["band_max"]
        )
    ]

    if not bad_ranges.empty:
        fail(
            errors,
            "BAND_MIN EXCEEDS BAND_MAX | "
            f"{path.name} | "
            f"rows={[(int(i) + 2) for i in bad_ranges.index]}",
        )

    bad_adjustments = df[
        df[ADJUSTMENT_COLUMN].notna()
        & (
            df[ADJUSTMENT_COLUMN]
            >= 1
        )
    ]

    if not bad_adjustments.empty:
        fail(
            errors,
            "CALIBRATION ADJUSTMENT MUST BE < 1 | "
            f"{path.name} | "
            f"rows={[(int(i) + 2) for i in bad_adjustments.index]}",
        )

    blank_band = (
        df["band"]
        .astype(str)
        .str.strip()
        .eq("")
    )

    if blank_band.any():
        fail(
            errors,
            "BLANK BAND LABEL | "
            f"{path.name} | "
            f"rows={[(int(i) + 2) for i in df.index[blank_band]]}",
        )

    return df


###############################################################
##################### CONFIG VALIDATION #######################
###############################################################

def validate_categories(
    df: pd.DataFrame,
    path: Path,
    column: str,
    allowed: set[str],
    errors: list[str],
) -> None:

    invalid = sorted(
        set(df[column]) - allowed
    )

    if invalid:
        fail(
            errors,
            f"INVALID {column.upper()} VALUES | "
            f"{path.name} | {invalid}",
        )


def validate_duplicates_and_overlaps(
    df: pd.DataFrame,
    path: Path,
    group_columns: list[str],
    errors: list[str],
) -> None:

    key_columns = (
        group_columns
        + [
            "band_min",
            "band_max",
        ]
    )

    duplicate_mask = df.duplicated(
        subset=key_columns,
        keep=False,
    )

    if duplicate_mask.any():
        fail(
            errors,
            "DUPLICATE CONFIG KEYS | "
            f"{path.name} | "
            f"rows={[(int(i) + 2) for i in df.index[duplicate_mask]]}",
        )

    usable = df.dropna(
        subset=[
            "band_min",
            "band_max",
        ]
    )

    group_arg = (
        group_columns[0]
        if len(group_columns) == 1
        else group_columns
    )

    overlaps = []

    for group_key, group in usable.groupby(
        group_arg,
        dropna=False,
    ):
        group = group.sort_values(
            [
                "band_min",
                "band_max",
            ],
            kind="stable",
        )

        previous_idx = None
        previous_max = None

        for idx, row in group.iterrows():

            band_min = float(
                row["band_min"]
            )
            band_max = float(
                row["band_max"]
            )

            if (
                previous_max is not None
                and band_min <= previous_max
            ):
                overlaps.append(
                    (
                        group_key,
                        previous_idx + 2,
                        idx + 2,
                    )
                )

            if (
                previous_max is None
                or band_max > previous_max
            ):
                previous_idx = idx
                previous_max = band_max

    if overlaps:
        fail(
            errors,
            "OVERLAPPING CONFIG KEYS | "
            f"{path.name} | "
            f"count={len(overlaps)} | "
            f"sample={overlaps[:10]}",
        )


def matches(
    df: pd.DataFrame,
    value: float,
    filters: dict[str, str],
) -> pd.DataFrame:

    mask = (
        df["band_min"].notna()
        & df["band_max"].notna()
        & (
            df["band_min"]
            <= value
        )
        & (
            value
            <= df["band_max"]
        )
    )

    for column, expected in filters.items():
        mask &= (
            df[column]
            == expected
        )

    return df.loc[
        mask
    ]


def validate_coverage_cases(
    df: pd.DataFrame,
    label: str,
    cases: list[
        tuple[
            float,
            dict[str, str],
        ]
    ],
    errors: list[str],
) -> None:

    bad = []

    for value, filters in cases:

        count = len(
            matches(
                df,
                value,
                filters,
            )
        )

        if count != 1:
            bad.append(
                (
                    value,
                    filters,
                    count,
                )
            )

    if bad:
        fail(
            errors,
            f"{label} COVERAGE ERRORS | "
            f"count={len(bad)} | "
            f"sample={bad[:10]}",
        )


###############################################################
###################### MONEYLINE CONFIG #######################
###############################################################

def validate_moneyline_static(
    df: pd.DataFrame,
    errors: list[str],
) -> None:

    validate_categories(
        df,
        MONEYLINE_FILE,
        "venue",
        VALID_VENUES,
        errors,
    )

    validate_categories(
        df,
        MONEYLINE_FILE,
        "fav_ud",
        VALID_FAV_UD,
        errors,
    )

    validate_duplicates_and_overlaps(
        df,
        MONEYLINE_FILE,
        [
            "fav_ud",
            "venue",
        ],
        errors,
    )

    sign_mismatches = []

    for idx, row in df.dropna(
        subset=[
            "band_min",
            "band_max",
        ]
    ).iterrows():

        band_min = float(
            row["band_min"]
        )
        band_max = float(
            row["band_max"]
        )
        fav_ud = row["fav_ud"]

        if (
            band_max < 0
            and fav_ud != "favorite"
        ):
            sign_mismatches.append(
                idx + 2
            )

        elif (
            band_min > 0
            and fav_ud != "underdog"
        ):
            sign_mismatches.append(
                idx + 2
            )

        elif (
            band_min <= 0 <= band_max
        ):
            sign_mismatches.append(
                idx + 2
            )

    if sign_mismatches:
        fail(
            errors,
            "MONEYLINE FAVORITE/UNDERDOG "
            "BAND MISMATCH | "
            f"rows={sign_mismatches}",
        )

    cases = []

    for odds in MONEYLINE_FAVORITE_RANGE:
        for venue in VALID_VENUES:
            cases.append(
                (
                    float(odds),
                    {
                        "fav_ud": "favorite",
                        "venue": venue,
                    },
                )
            )

    for odds in MONEYLINE_UNDERDOG_RANGE:
        for venue in VALID_VENUES:
            cases.append(
                (
                    float(odds),
                    {
                        "fav_ud": "underdog",
                        "venue": venue,
                    },
                )
            )

    validate_coverage_cases(
        df,
        "MONEYLINE",
        cases,
        errors,
    )


###############################################################
###################### PUCK-LINE CONFIG #######################
###############################################################

def validate_puck_line_static(
    df: pd.DataFrame,
    errors: list[str],
) -> None:

    validate_categories(
        df,
        PUCK_LINE_FILE,
        "venue",
        VALID_VENUES,
        errors,
    )

    validate_categories(
        df,
        PUCK_LINE_FILE,
        "fav_ud",
        VALID_FAV_UD,
        errors,
    )

    validate_duplicates_and_overlaps(
        df,
        PUCK_LINE_FILE,
        [
            "fav_ud",
            "venue",
        ],
        errors,
    )

    expected = {
        (
            -1.5,
            -1.5,
            venue,
            "favorite",
        )
        for venue in VALID_VENUES
    } | {
        (
            1.5,
            1.5,
            venue,
            "underdog",
        )
        for venue in VALID_VENUES
    }

    actual = {
        (
            float(
                row["band_min"]
            ),
            float(
                row["band_max"]
            ),
            str(
                row["venue"]
            ),
            str(
                row["fav_ud"]
            ),
        )
        for _, row in df.dropna(
            subset=[
                "band_min",
                "band_max",
            ]
        ).iterrows()
    }

    if actual != expected:
        fail(
            errors,
            "PUCK-LINE EXPECTED COMBINATIONS "
            "MISMATCH | "
            f"missing={sorted(expected - actual)} | "
            f"extra={sorted(actual - expected)}",
        )

    cases = []

    for line in PUCK_LINES:

        fav_ud = (
            "favorite"
            if line < 0
            else "underdog"
        )

        for venue in VALID_VENUES:
            cases.append(
                (
                    line,
                    {
                        "fav_ud": fav_ud,
                        "venue": venue,
                    },
                )
            )

    validate_coverage_cases(
        df,
        "PUCK-LINE",
        cases,
        errors,
    )


###############################################################
######################## TOTAL CONFIG #########################
###############################################################

def validate_total_static(
    df: pd.DataFrame,
    errors: list[str],
) -> None:

    validate_categories(
        df,
        TOTAL_FILE,
        "side",
        VALID_TOTAL_SIDES,
        errors,
    )

    validate_duplicates_and_overlaps(
        df,
        TOTAL_FILE,
        [
            "side",
        ],
        errors,
    )

    expected = {
        (
            line,
            line,
            side,
        )
        for line in TOTAL_LINES
        for side in VALID_TOTAL_SIDES
    }

    actual = {
        (
            float(
                row["band_min"]
            ),
            float(
                row["band_max"]
            ),
            str(
                row["side"]
            ),
        )
        for _, row in df.dropna(
            subset=[
                "band_min",
                "band_max",
            ]
        ).iterrows()
    }

    if actual != expected:
        fail(
            errors,
            "TOTAL EXPECTED COMBINATIONS "
            "MISMATCH | "
            f"missing={sorted(expected - actual)} | "
            f"extra={sorted(actual - expected)}",
        )

    cases = [
        (
            line,
            {
                "side": side,
            },
        )
        for line in TOTAL_LINES
        for side in VALID_TOTAL_SIDES
    ]

    validate_coverage_cases(
        df,
        "TOTAL",
        cases,
        errors,
    )


###############################################################
#################### RUNTIME VALIDATION #######################
###############################################################

def numeric_runtime_value(
    value,
    file_path: Path,
    row_number: int,
    column: str,
    errors: list[str],
) -> float | None:

    try:
        number = float(
            value
        )

    except Exception:
        fail(
            errors,
            "RUNTIME NON-NUMERIC VALUE | "
            f"{file_path.name} | "
            f"row={row_number} | "
            f"column={column} | "
            f"value={value!r}",
        )
        return None

    if not math.isfinite(
        number
    ):
        fail(
            errors,
            "RUNTIME NON-FINITE VALUE | "
            f"{file_path.name} | "
            f"row={row_number} | "
            f"column={column} | "
            f"value={value!r}",
        )
        return None

    return number


def check_runtime_calibration(
    config: pd.DataFrame,
    lookup_value: float,
    filters: dict[str, str],
    fair_decimal: float,
    file_path: Path,
    row_number: int,
    label: str,
    errors: list[str],
) -> bool:

    found = matches(
        config,
        lookup_value,
        filters,
    )

    if len(found) != 1:
        fail(
            errors,
            "RUNTIME COVERAGE ERROR | "
            f"{file_path.name} | "
            f"row={row_number} | "
            f"{label} | "
            f"lookup={lookup_value} | "
            f"filters={filters} | "
            f"matches={len(found)}",
        )
        return False

    if fair_decimal <= 1:
        fail(
            errors,
            "INVALID FAIR DECIMAL | "
            f"{file_path.name} | "
            f"row={row_number} | "
            f"{label}={fair_decimal}",
        )
        return False

    adjustment = float(
        found.iloc[0][
            ADJUSTMENT_COLUMN
        ]
    )

    adjusted_decimal = (
        fair_decimal
        * (
            1
            - adjustment
        )
    )

    if (
        not math.isfinite(
            adjusted_decimal
        )
        or adjusted_decimal <= 1
    ):
        fail(
            errors,
            "INVALID ADJUSTED DECIMAL | "
            f"{file_path.name} | "
            f"row={row_number} | "
            f"{label} | "
            f"fair_decimal={fair_decimal} | "
            f"adjustment={adjustment} | "
            f"adjusted_decimal={adjusted_decimal}",
        )
        return False

    return True


def read_runtime_file(
    file_path: Path,
    required: list[str],
    errors: list[str],
) -> pd.DataFrame | None:

    try:
        df = pd.read_csv(
            file_path,
            dtype=str,
        )

    except Exception as e:
        fail(
            errors,
            f"RUNTIME READ ERROR | "
            f"{file_path} | {e}",
        )
        return None

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        fail(
            errors,
            "RUNTIME MISSING COLUMNS | "
            f"{file_path} | "
            f"{missing}",
        )
        return None

    return df


###############################################################
################### RUNTIME MONEYLINE #########################
###############################################################

def validate_runtime_moneyline(
    config: pd.DataFrame,
    errors: list[str],
) -> int:

    files = sorted(
        INPUT_DIR.glob(
            "*_NHL_moneyline.csv"
        )
    )

    checked = 0

    required = [
        "away_dk_moneyline_american",
        "home_dk_moneyline_american",
        "away_fair_decimal_moneyline",
        "home_fair_decimal_moneyline",
    ]

    for file_path in files:

        df = read_runtime_file(
            file_path,
            required,
            errors,
        )

        if df is None:
            continue

        for idx, row in df.iterrows():

            row_number = idx + 2

            for venue in (
                "away",
                "home",
            ):

                odds_col = (
                    f"{venue}_dk_moneyline_american"
                )
                fair_col = (
                    f"{venue}_fair_decimal_moneyline"
                )

                odds = numeric_runtime_value(
                    row[odds_col],
                    file_path,
                    row_number,
                    odds_col,
                    errors,
                )

                fair = numeric_runtime_value(
                    row[fair_col],
                    file_path,
                    row_number,
                    fair_col,
                    errors,
                )

                if (
                    odds is None
                    or fair is None
                ):
                    continue

                if odds == 0:
                    fail(
                        errors,
                        "UNSUPPORTED MONEYLINE ODDS | "
                        f"{file_path.name} | "
                        f"row={row_number} | "
                        f"venue={venue} | "
                        "odds=0",
                    )
                    continue

                fav_ud = (
                    "favorite"
                    if odds < 0
                    else "underdog"
                )

                if check_runtime_calibration(
                    config,
                    odds,
                    {
                        "fav_ud": fav_ud,
                        "venue": venue,
                    },
                    fair,
                    file_path,
                    row_number,
                    fair_col,
                    errors,
                ):
                    checked += 1

    log(
        "RUNTIME MONEYLINE SIDES CHECKED | "
        f"{checked} | "
        f"files={len(files)}"
    )

    return checked


###############################################################
################### RUNTIME PUCK LINE #########################
###############################################################

def validate_runtime_puck_line(
    config: pd.DataFrame,
    errors: list[str],
) -> int:

    files = sorted(
        INPUT_DIR.glob(
            "*_NHL_puck_line.csv"
        )
    )

    checked = 0

    required = [
        "away_puck_line",
        "home_puck_line",
        "away_fair_decimal_puck_line",
        "home_fair_decimal_puck_line",
    ]

    for file_path in files:

        df = read_runtime_file(
            file_path,
            required,
            errors,
        )

        if df is None:
            continue

        for idx, row in df.iterrows():

            row_number = idx + 2

            for venue in (
                "away",
                "home",
            ):

                line_col = (
                    f"{venue}_puck_line"
                )
                fair_col = (
                    f"{venue}_fair_decimal_puck_line"
                )

                line = numeric_runtime_value(
                    row[line_col],
                    file_path,
                    row_number,
                    line_col,
                    errors,
                )

                fair = numeric_runtime_value(
                    row[fair_col],
                    file_path,
                    row_number,
                    fair_col,
                    errors,
                )

                if (
                    line is None
                    or fair is None
                ):
                    continue

                fav_ud = (
                    "favorite"
                    if line < 0
                    else "underdog"
                )

                if check_runtime_calibration(
                    config,
                    line,
                    {
                        "fav_ud": fav_ud,
                        "venue": venue,
                    },
                    fair,
                    file_path,
                    row_number,
                    fair_col,
                    errors,
                ):
                    checked += 1

    log(
        "RUNTIME PUCK-LINE SIDES CHECKED | "
        f"{checked} | "
        f"files={len(files)}"
    )

    return checked


###############################################################
###################### RUNTIME TOTAL ##########################
###############################################################

def validate_runtime_total(
    config: pd.DataFrame,
    errors: list[str],
) -> int:

    files = sorted(
        INPUT_DIR.glob(
            "*_NHL_total.csv"
        )
    )

    checked = 0

    required = [
        "total",
        "over_fair_decimal_total",
        "under_fair_decimal_total",
    ]

    for file_path in files:

        df = read_runtime_file(
            file_path,
            required,
            errors,
        )

        if df is None:
            continue

        for idx, row in df.iterrows():

            row_number = idx + 2

            total_line = numeric_runtime_value(
                row["total"],
                file_path,
                row_number,
                "total",
                errors,
            )

            if total_line is None:
                continue

            for side in (
                "over",
                "under",
            ):

                fair_col = (
                    f"{side}_fair_decimal_total"
                )

                fair = numeric_runtime_value(
                    row[fair_col],
                    file_path,
                    row_number,
                    fair_col,
                    errors,
                )

                if fair is None:
                    continue

                if check_runtime_calibration(
                    config,
                    total_line,
                    {
                        "side": side,
                    },
                    fair,
                    file_path,
                    row_number,
                    fair_col,
                    errors,
                ):
                    checked += 1

    log(
        "RUNTIME TOTAL SIDES CHECKED | "
        f"{checked} | "
        f"files={len(files)}"
    )

    return checked


###############################################################
######################## MAIN #################################
###############################################################

def main() -> None:

    reset_log()

    errors: list[str] = []

    log(
        f"CONFIG_DIR={CONFIG_DIR}"
    )
    log(
        f"INPUT_DIR={INPUT_DIR}"
    )

    log(
        "MONEYLINE SUPPORTED ODDS="
        "-999..-100 and +100..+599"
    )
    log(
        "PUCK-LINE SUPPORTED LINES="
        "-1.5,+1.5"
    )
    log(
        "TOTAL SUPPORTED LINES="
        "5.5,6.0,6.5,7.0,7.5"
    )

    moneyline = load_config(
        MONEYLINE_FILE,
        MONEYLINE_REQUIRED,
        errors,
    )

    puck_line = load_config(
        PUCK_LINE_FILE,
        PUCK_LINE_REQUIRED,
        errors,
    )

    total = load_config(
        TOTAL_FILE,
        TOTAL_REQUIRED,
        errors,
    )

    if moneyline is not None:
        validate_moneyline_static(
            moneyline,
            errors,
        )
        validate_runtime_moneyline(
            moneyline,
            errors,
        )

    if puck_line is not None:
        validate_puck_line_static(
            puck_line,
            errors,
        )
        validate_runtime_puck_line(
            puck_line,
            errors,
        )

    if total is not None:
        validate_total_static(
            total,
            errors,
        )
        validate_runtime_total(
            total,
            errors,
        )

    log(
        f"VALIDATION ERRORS | "
        f"{len(errors)}"
    )

    if errors:

        log(
            "STATUS: FAILED"
        )

        print(
            "NHL Stage 02 calibration "
            "validation FAILED: "
            f"{len(errors)} error(s). "
            f"See {LOG_FILE}"
        )

        sys.exit(1)

    log(
        "STATUS: SUCCESS"
    )

    print(
        "NHL Stage 02 calibration "
        "validation complete."
    )


if __name__ == "__main__":
    main()