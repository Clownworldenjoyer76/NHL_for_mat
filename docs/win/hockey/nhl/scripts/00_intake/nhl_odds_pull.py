#!/usr/bin/env python3
# docs/win/hockey/scripts/00_parsing/nhl_odds_pull.py

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

API_KEY_ENV = "API_ODDS"
BASE_URL = "https://api.odds-api.io/v3"

SPORT_SLUG = "ice-hockey"
LEAGUE_SLUGS = ["usa-nhl", "usa-nhl-playoffs"]
BOOKMAKER = "FanDuel"

ET = ZoneInfo("America/New_York")

JSON_OUT_DIR = Path("docs/win/hockey/nhl/odds")
JSON_OUT_DIR.mkdir(parents=True, exist_ok=True)


def get_api_key() -> str:
    api_key = os.environ.get(API_KEY_ENV, "").strip()
    if not api_key:
        raise RuntimeError(f"{API_KEY_ENV} environment variable is not set")
    return api_key


def request_json(path: str, params: dict) -> object:
    response = requests.get(f"{BASE_URL}{path}", params=params, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(
            f"HTTP {response.status_code} for {BASE_URL}{path} | body={response.text[:500]}"
        )

    return response.json()


def today_window_utc() -> tuple[str, str]:
    today_et = datetime.now(ET).date()
    tomorrow_et = today_et + timedelta(days=1)

    from_utc = datetime(
        today_et.year,
        today_et.month,
        today_et.day,
        0,
        0,
        0,
        tzinfo=ET,
    ).astimezone(ZoneInfo("UTC"))

    to_utc = datetime(
        tomorrow_et.year,
        tomorrow_et.month,
        tomorrow_et.day,
        0,
        0,
        0,
        tzinfo=ET,
    ).astimezone(ZoneInfo("UTC"))

    return (
        from_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        to_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def fetch_events(api_key: str) -> list[dict]:
    from_utc, to_utc = today_window_utc()
    all_events = []

    for league_slug in LEAGUE_SLUGS:
        payload = request_json(
            "/events",
            {
                "apiKey": api_key,
                "sport": SPORT_SLUG,
                "league": league_slug,
                "status": "pending",
                "from": from_utc,
                "to": to_utc,
                "limit": 5000,
            },
        )

        if isinstance(payload, list):
            all_events.extend(payload)

    seen = set()
    deduped = []

    for event in all_events:
        if not isinstance(event, dict):
            continue

        event_id = str(event.get("id", "")).strip()
        if not event_id:
            continue

        if event_id in seen:
            continue

        seen.add(event_id)
        deduped.append(event)

    return deduped


def fetch_odds_multi(api_key: str, event_ids: list[str]) -> list[dict]:
    all_odds = []

    for i in range(0, len(event_ids), 10):
        batch_ids = event_ids[i:i + 10]

        payload = request_json(
            "/odds/multi",
            {
                "apiKey": api_key,
                "eventIds": ",".join(batch_ids),
                "bookmakers": BOOKMAKER,
            },
        )

        if isinstance(payload, list):
            all_odds.extend(payload)

    return all_odds


def main() -> None:
    api_key = get_api_key()

    run_date = datetime.now(ET).strftime("%Y_%m_%d")
    json_out_path = JSON_OUT_DIR / f"{run_date}.json"

    from_utc, to_utc = today_window_utc()

    events = fetch_events(api_key)
    event_ids = [str(event["id"]) for event in events if "id" in event]

    odds = fetch_odds_multi(api_key, event_ids) if event_ids else []

    output = {
        "run_date": run_date,
        "generated_at_et": datetime.now(ET).isoformat(),
        "source": "odds-api.io",
        "sport_slug": SPORT_SLUG,
        "league_slugs": LEAGUE_SLUGS,
        "bookmaker": BOOKMAKER,
        "date_window": {
            "timezone": "America/New_York",
            "from_utc": from_utc,
            "to_utc": to_utc,
        },
        "events": events,
        "odds": odds,
    }

    with open(json_out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"WROTE {json_out_path}")


if __name__ == "__main__":
    main()
