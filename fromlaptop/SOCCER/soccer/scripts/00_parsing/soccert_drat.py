import time
import random
import pandas as pd
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright

URLS = {
    "mls":        "https://www.dratings.com/predictor/mls-soccer-predictions/",
    "epl":        "https://www.dratings.com/predictor/english-premier-league-predictions/",
    "ligue1":     "https://www.dratings.com/predictor/france-ligue-1-predictions/",
    "laliga":     "https://www.dratings.com/predictor/spain-la-liga-predictions/",
    "bundesliga": "https://www.dratings.com/predictor/german-bundesliga-predictions/",
    "seriea":     "https://www.dratings.com/predictor/italy-serie-a-predictions/",
}

LEAGUE_MAP = {
    "mls": "MLS",
    "epl": "EPL",
    "ligue1": "LIGUE1",
    "laliga": "LALIGA",
    "bundesliga": "BUNDESLIGA",
    "seriea": "SERIEA",
}

def scrape_page(page, url):
    page.goto(url)
    page.wait_for_selector("table")
    rows = page.query_selector_all("table tbody tr")
    return [[c.inner_text().strip() for c in r.query_selector_all("td")] for r in rows]

def split_date_time(dt):
    parts = dt.replace("\n", " ").split()
    date = datetime.strptime(parts[0], "%m/%d/%Y").strftime("%Y_%m_%d")
    time_ = " ".join(parts[1:])
    return date, time_

def safe_split(val):
    return val.split("\n") if "\n" in val else val.split()

def is_score_cell(val):
    parts = safe_split(val)
    return len(parts) == 2 and all(p.isdigit() for p in parts)

def parse_prediction(row, league):
    try:
        date, time_ = split_date_time(row[0])

        teams = safe_split(row[1])
        home, away = teams[0], teams[1]

        probs = safe_split(row[2])
        home_prob = probs[0]
        away_prob = probs[1]

        draw_prob = row[3]

        xg = safe_split(row[5]) if len(row) > 5 else ["", ""]
        home_xg = xg[0] if len(xg) > 0 else ""
        away_xg = xg[1] if len(xg) > 1 else ""

        total = row[6] if len(row) > 6 else ""

        return {
            "sport": "soccer",
            "league": league,
            "market": "",
            "match_date": date,
            "match_time": time_,
            "home_team": home,
            "away_team": away,
            "home_prob": home_prob,
            "draw_prob": draw_prob,
            "away_prob": away_prob,
            "home_xg": home_xg,
            "away_xg": away_xg,
            "expected_total_goals": total,
        }
    except Exception as e:
        print(f"[DEBUG][PREDICTION ERROR] {league} | {row} | {e}")
        return None

def parse_final(row, league):
    try:
        date, time_ = split_date_time(row[0])

        teams = safe_split(row[1])
        home, away = teams[0], teams[1]

        score_idx = None
        for i, cell in enumerate(row):
            if is_score_cell(cell):
                score_idx = i
                break

        if score_idx is None:
            print(f"[DEBUG] No score found: {row}")
            return None

        score = safe_split(row[score_idx])
        home_score = score[0]
        away_score = score[1]

        return {
            "sport": "soccer",
            "league": league,
            "market": "",
            "game_date": date,
            "match_time": time_,
            "home_team": home,
            "away_team": away,
            "away_score": away_score,
            "home_score": home_score,
        }
    except Exception as e:
        print(f"[DEBUG][FINAL ERROR] {league} | {row} | {e}")
        return None

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for key, url in URLS.items():
            league = LEAGUE_MAP[key]
            print(f"\n--- Scraping {league} ---")

            raw = scrape_page(page, url)

            predictions = []
            finals = []

            for i, r in enumerate(raw):
                if not r or "Sportsbooks" in r[0] or "DRatings" in r[0]:
                    continue

                has_score = any(is_score_cell(c) for c in r)

                if has_score:
                    parsed = parse_final(r, league)
                    if parsed:
                        finals.append(parsed)
                else:
                    parsed = parse_prediction(r, league)
                    if parsed:
                        predictions.append(parsed)

            print(f"[DEBUG] {league} → predictions: {len(predictions)}, finals: {len(finals)}")

            today = datetime.utcnow().strftime("%Y_%m_%d")

            pred_path = Path(f"docs/win/soccer/00_intake/predictions/{league}/{today}_{league}.csv")
            pred_path.parent.mkdir(parents=True, exist_ok=True)

            final_path = Path(f"docs/win/final_scores/results/soccer/final_scores/{league}/{today}_{league}.csv")
            final_path.parent.mkdir(parents=True, exist_ok=True)

            if predictions:
                pd.DataFrame(predictions).to_csv(pred_path, index=False)
                print(f"Saved predictions → {pred_path}")

            if finals:
                pd.DataFrame(finals).to_csv(final_path, index=False)
                print(f"Saved finals → {final_path}")

            time.sleep(random.uniform(2, 4))

        browser.close()

if __name__ == "__main__":
    main()
