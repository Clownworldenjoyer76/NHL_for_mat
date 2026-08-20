#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/00_intake/transform_hockey_odds.py

import csv
import json
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BOOKMAKER = "FanDuel"
ET = ZoneInfo("America/New_York")

TOTAL_MIN = 4.5
TOTAL_MAX = 7.5

ODDS_DIR = Path("docs/win/hockey/nhl/odds")
SPORTSBOOK_DIR = Path("docs/win/hockey/nhl/00_intake/sportsbook")
ERROR_DIR = Path("docs/win/hockey/nhl/errors/00_intake")
LOG_FILE = ERROR_DIR / "transform_hockey_odds.txt"

SPORTSBOOK_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

FIELDS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_dk_moneyline_american",
    "away_dk_moneyline_american",
    "home_puck_line",
    "away_puck_line",
    "total",
    "home_dk_puck_line_american",
    "away_dk_puck_line_american",
    "dk_total_over_american",
    "dk_total_under_american",
    "home_dk_moneyline_decimal",
    "away_dk_moneyline_decimal",
    "home_dk_puck_line_decimal",
    "away_dk_puck_line_decimal",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]


def reset_log() -> None:
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== transform_hockey_odds RUN {datetime.now(ET).isoformat()} ===\n")


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(ET).isoformat()} | {msg}\n")


def decimal_to_american(value) -> str:
    try:
        dec = float(value)
        if dec <= 1:
            return ""
        if dec >= 2:
            return f"+{round((dec - 1) * 100)}"
        return str(round(-100 / (dec - 1)))
    except Exception:
        return ""


def clean_decimal(value) -> str:
    if value in ("", None, "N/A"):
        return ""
    return str(value).strip()


def clean_line(value) -> str:
    if value in ("", None, "N/A"):
        return ""

    try:
        v = float(value)
        if v.is_integer():
            return str(int(v))
        return str(v)
    except Exception:
        return str(value).strip()


def to_et_date_time(date_str: str) -> tuple[str, str]:
    if not date_str:
        return "", ""

    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        dt_et = dt.astimezone(ET)
        return dt_et.strftime("%Y_%m_%d"), dt_et.strftime("%H:%M")
    except Exception:
        return "", ""


def get_status(event: dict, odds_payload: dict) -> str:
    return str(odds_payload.get("status") or event.get("status") or "").strip().lower()


def get_markets(odds_payload: dict) -> list:
    bookmakers = odds_payload.get("bookmakers", {})
    if not isinstance(bookmakers, dict):
        return []

    markets = bookmakers.get(BOOKMAKER, [])
    if not isinstance(markets, list):
        return []

    return markets


def find_market(markets: list, market_name: str) -> dict:
    wanted = market_name.strip().lower()

    for market in markets:
        if str(market.get("name", "")).strip().lower() == wanted:
            return market

    return {}


def parse_moneyline(game_id: str, markets: list, counters: dict) -> dict:
    market = find_market(markets, "ML")
    odds = market.get("odds", [])

    if not isinstance(odds, list) or not odds or not isinstance(odds[0], dict):
        counters["warnings"] += 1
        log(f"WARNING game_id={game_id} missing ML market")
        return {
            "home_decimal": "",
            "away_decimal": "",
            "home_american": "",
            "away_american": "",
        }

    row = odds[0]
    home_decimal = clean_decimal(row.get("home", ""))
    away_decimal = clean_decimal(row.get("away", ""))

    return {
        "home_decimal": home_decimal,
        "away_decimal": away_decimal,
        "home_american": decimal_to_american(home_decimal),
        "away_american": decimal_to_american(away_decimal),
    }


def pick_standard_puck_line_row(rows: list) -> dict:
    valid = [row for row in rows if isinstance(row, dict)]

    for row in valid:
        try:
            if abs(float(row.get("hdp"))) == 1.5:
                return row
        except Exception:
            continue

    return {}


def parse_spread(game_id: str, markets: list, counters: dict) -> dict:
    market = find_market(markets, "Spread")
    odds = market.get("odds", [])

    if not isinstance(odds, list):
        odds = []

    row = pick_standard_puck_line_row(odds)

    if not row:
        counters["warnings"] += 1
        log(f"WARNING game_id={game_id} no Spread row with abs(hdp)==1.5")
        return {
            "home_line": "",
            "away_line": "",
            "home_decimal": "",
            "away_decimal": "",
            "home_american": "",
            "away_american": "",
        }

    try:
        hdp = float(row.get("hdp"))

        if hdp > 0:
            home_line = abs(hdp)
            away_line = -abs(hdp)
            home_decimal = clean_decimal(row.get("home", ""))
            away_decimal = clean_decimal(row.get("away", ""))
        else:
            home_line = -abs(hdp)
            away_line = abs(hdp)
            home_decimal = clean_decimal(row.get("home", ""))
            away_decimal = clean_decimal(row.get("away", ""))

        counters["spread_1_5_selected"] += 1

    except Exception:
        counters["warnings"] += 1
        log(f"WARNING game_id={game_id} invalid Spread row")
        home_line = ""
        away_line = ""
        home_decimal = ""
        away_decimal = ""

    return {
        "home_line": clean_line(home_line),
        "away_line": clean_line(away_line),
        "home_decimal": home_decimal,
        "away_decimal": away_decimal,
        "home_american": decimal_to_american(home_decimal),
        "away_american": decimal_to_american(away_decimal),
    }


def pick_total_row_closest_odds(rows: list) -> dict:
    valid = [row for row in rows if isinstance(row, dict)]
    candidates = []

    for index, row in enumerate(valid):
        try:
            hdp = float(row.get("hdp"))

            if hdp < TOTAL_MIN or hdp > TOTAL_MAX:
                continue

            over = float(row.get("over"))
            under = float(row.get("under"))

            if over <= 1 or under <= 1:
                continue

            candidates.append((abs(over - under), index, row))

        except Exception:
            continue

    if not candidates:
        return {}

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def parse_totals(game_id: str, markets: list, counters: dict) -> dict:
    market = find_market(markets, "Totals")
    odds = market.get("odds", [])

    if not isinstance(odds, list):
        odds = []

    row = pick_total_row_closest_odds(odds)

    if not row:
        counters["warnings"] += 1
        log(
            f"WARNING game_id={game_id} no valid Totals row with "
            f"{TOTAL_MIN} <= hdp <= {TOTAL_MAX} and numeric over/under odds"
        )
        return {
            "total": "",
            "over_decimal": "",
            "under_decimal": "",
            "over_american": "",
            "under_american": "",
        }

    over_decimal = clean_decimal(row.get("over", ""))
    under_decimal = clean_decimal(row.get("under", ""))

    counters["total_closest_selected"] += 1

    return {
        "total": clean_line(row.get("hdp", "")),
        "over_decimal": over_decimal,
        "under_decimal": under_decimal,
        "over_american": decimal_to_american(over_decimal),
        "under_american": decimal_to_american(under_decimal),
    }


def build_row(event: dict, odds_payload: dict, counters: dict) -> dict:
    game_id = str(odds_payload.get("id") or event.get("id") or "").strip()

    markets = get_markets(odds_payload)
    if not markets:
        counters["warnings"] += 1
        log(f"WARNING game_id={game_id} missing {BOOKMAKER} bookmaker")

    moneyline = parse_moneyline(game_id, markets, counters)
    spread = parse_spread(game_id, markets, counters)
    totals = parse_totals(game_id, markets, counters)

    home_team = str(odds_payload.get("home") or event.get("home") or "").strip()
    away_team = str(odds_payload.get("away") or event.get("away") or "").strip()

    game_date, game_time = to_et_date_time(
        str(odds_payload.get("date") or event.get("date") or "")
    )

    return {
        "game_id": game_id,
        "sport": "hockey",
        "league": "nhl",
        "game_date": game_date,
        "game_time": game_time,
        "home_team": home_team,
        "away_team": away_team,
        "home_dk_moneyline_american": moneyline["home_american"],
        "away_dk_moneyline_american": moneyline["away_american"],
        "home_puck_line": spread["home_line"],
        "away_puck_line": spread["away_line"],
        "total": totals["total"],
        "home_dk_puck_line_american": spread["home_american"],
        "away_dk_puck_line_american": spread["away_american"],
        "dk_total_over_american": totals["over_american"],
        "dk_total_under_american": totals["under_american"],
        "home_dk_moneyline_decimal": moneyline["home_decimal"],
        "away_dk_moneyline_decimal": moneyline["away_decimal"],
        "home_dk_puck_line_decimal": spread["home_decimal"],
        "away_dk_puck_line_decimal": spread["away_decimal"],
        "dk_total_over_decimal": totals["over_decimal"],
        "dk_total_under_decimal": totals["under_decimal"],
    }


def read_existing_csv(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}

    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = {}

        for row in reader:
            game_id = str(row.get("game_id", "")).strip()
            if game_id:
                rows[game_id] = {field: row.get(field, "") for field in FIELDS}

    return rows


def row_changed(existing: dict, new: dict) -> bool:
    for field in FIELDS:
        if str(existing.get(field, "")) != str(new.get(field, "")):
            return True
    return False


def write_csv(path: Path, rows_by_game_id: dict[str, dict]) -> None:
    rows = list(rows_by_game_id.values())

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field, "") for field in FIELDS})


def main() -> None:
    reset_log()

    counters = {
        "json_files_processed": 0,
        "events_found": 0,
        "odds_payloads_found": 0,
        "rows_built": 0,
        "rows_added": 0,
        "rows_updated": 0,
        "rows_unchanged": 0,
        "rows_preserved_status_changed": 0,
        "rows_skipped_non_pending_no_existing": 0,
        "csv_files_written": 0,
        "spread_1_5_selected": 0,
        "total_closest_selected": 0,
        "warnings": 0,
        "errors": 0,
    }

    try:
        json_files = sorted(ODDS_DIR.glob("*.json"))
        log(f"Odds input directory: {ODDS_DIR}")
        log(f"JSON files found: {len(json_files)}")

        new_rows_by_date = defaultdict(dict)
        status_by_date_game = {}

        for json_file in json_files:
            file_rows_built = 0
            file_warnings_before = counters["warnings"]

            with open(json_file, "r", encoding="utf-8") as f:
                payload = json.load(f)

            counters["json_files_processed"] += 1

            events = payload.get("events", [])
            odds = payload.get("odds", [])

            if not isinstance(events, list):
                counters["warnings"] += 1
                log(f"WARNING {json_file} events was not a list")
                events = []

            if not isinstance(odds, list):
                counters["warnings"] += 1
                log(f"WARNING {json_file} odds was not a list")
                odds = []

            counters["events_found"] += len(events)
            counters["odds_payloads_found"] += len(odds)

            events_by_id = {
                str(event.get("id", "")).strip(): event
                for event in events
                if isinstance(event, dict) and event.get("id")
            }

            odds_ids = set()

            for odds_payload in odds:
                if not isinstance(odds_payload, dict):
                    counters["warnings"] += 1
                    log(f"WARNING {json_file} contains non-dict odds payload")
                    continue

                game_id = str(odds_payload.get("id", "")).strip()
                if not game_id:
                    counters["warnings"] += 1
                    log(f"WARNING {json_file} odds payload missing game_id")
                    continue

                odds_ids.add(game_id)

                event = events_by_id.get(game_id, {})
                if not event:
                    counters["warnings"] += 1
                    log(f"WARNING odds game_id={game_id} has no matching event payload")

                row = build_row(event, odds_payload, counters)
                game_date = row.get("game_date", "")
                current_status = get_status(event, odds_payload)

                if not game_date:
                    counters["warnings"] += 1
                    log(f"WARNING game_id={game_id} skipped because game_date was blank")
                    continue

                new_rows_by_date[game_date][game_id] = row
                status_by_date_game[(game_date, game_id)] = current_status

                counters["rows_built"] += 1
                file_rows_built += 1

            for event_id in events_by_id:
                if event_id not in odds_ids:
                    counters["warnings"] += 1
                    log(f"WARNING event game_id={event_id} has no matching odds payload")

            file_warnings = counters["warnings"] - file_warnings_before
            log(
                f"READ {json_file} | events={len(events)} | odds={len(odds)} "
                f"| rows_built={file_rows_built} | warnings={file_warnings}"
            )

        for game_date, new_rows in sorted(new_rows_by_date.items()):
            csv_path = SPORTSBOOK_DIR / f"NHL_{game_date}.csv"
            existing_rows = read_existing_csv(csv_path)

            merged_rows = dict(existing_rows)

            for game_id, new_row in new_rows.items():
                current_status = status_by_date_game.get((game_date, game_id), "")

                existing_row = existing_rows.get(game_id)

                if existing_row and current_status != "pending":
                    merged_rows[game_id] = existing_row
                    counters["rows_preserved_status_changed"] += 1
                    log(
                        f"PRESERVED game_id={game_id} date={game_date} "
                        f"because current_status={current_status}"
                    )
                    continue

                if not existing_row and current_status != "pending":
                    counters["rows_skipped_non_pending_no_existing"] += 1
                    counters["warnings"] += 1
                    log(
                        f"WARNING skipped new game_id={game_id} date={game_date} "
                        f"because current_status={current_status} and no existing row"
                    )
                    continue

                if not existing_row:
                    merged_rows[game_id] = new_row
                    counters["rows_added"] += 1
                    continue

                if row_changed(existing_row, new_row):
                    merged_rows[game_id] = new_row
                    counters["rows_updated"] += 1
                else:
                    merged_rows[game_id] = existing_row
                    counters["rows_unchanged"] += 1

            write_csv(csv_path, merged_rows)
            counters["csv_files_written"] += 1
            log(f"WROTE {csv_path} rows={len(merged_rows)}")

        log(f"Spread rows selected by abs(hdp)==1.5: {counters['spread_1_5_selected']}")
        log(
            f"Totals rows selected by closest over/under odds "
            f"within {TOTAL_MIN}-{TOTAL_MAX}: {counters['total_closest_selected']}"
        )

        log("--- SUMMARY ---")
        log(f"JSON files processed: {counters['json_files_processed']}")
        log(f"Events found: {counters['events_found']}")
        log(f"Odds payloads found: {counters['odds_payloads_found']}")
        log(f"Rows built: {counters['rows_built']}")
        log(f"Rows added: {counters['rows_added']}")
        log(f"Rows updated: {counters['rows_updated']}")
        log(f"Rows unchanged: {counters['rows_unchanged']}")
        log(f"Rows preserved due to non-pending status: {counters['rows_preserved_status_changed']}")
        log(f"Rows skipped non-pending with no existing row: {counters['rows_skipped_non_pending_no_existing']}")
        log(f"CSV files written: {counters['csv_files_written']}")
        log(f"Warnings: {counters['warnings']}")
        log(f"Errors: {counters['errors']}")
        log("STATUS: SUCCESS")

        print("NHL odds transform complete.")

    except Exception as e:
        counters["errors"] += 1
        log(f"FATAL ERROR: {e}")
        log(traceback.format_exc())
        log("--- SUMMARY ---")
        log(f"JSON files processed: {counters['json_files_processed']}")
        log(f"Events found: {counters['events_found']}")
        log(f"Odds payloads found: {counters['odds_payloads_found']}")
        log(f"Rows built: {counters['rows_built']}")
        log(f"Warnings: {counters['warnings']}")
        log(f"Errors: {counters['errors']}")
        log("STATUS: FAILED")
        raise


if __name__ == "__main__":
    main()
