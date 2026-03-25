"""
CrowdVolt Ticket Price Scraper
-------------------------------
Scrapes ask prices, bid prices, and listing counts from CrowdVolt event pages.
Stores everything in a local SQLite database and exports JSON for the dashboard.

Requirements:
    pip install playwright schedule loguru
    playwright install chromium
"""

import json
import re
import sqlite3
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import schedule
from loguru import logger
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ─── Config ───────────────────────────────────────────────────────────────────

BASE_URL = "https://www.crowdvolt.com"
DB_PATH = Path(__file__).parent / "prices.db"
EXPORT_PATH = Path(__file__).parent / "dashboard_data.json"
EVENTS_CONFIG = Path(__file__).parent / "events.json"

# How long to wait for page content (ms)
PAGE_TIMEOUT = 20_000
# Polite delay between page fetches (seconds)
REQUEST_DELAY = 3
# Run scrape daily at this time (24h format)
DAILY_RUN_TIME = "09:00"

# ─── Database ─────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            crowdvolt_url TEXT NOT NULL UNIQUE,
            artist      TEXT NOT NULL,
            venue       TEXT,
            city        TEXT,
            event_date  TEXT,
            face_value  REAL,
            us_listeners INTEGER,
            genre       TEXT,
            cap         TEXT,
            avail       TEXT,
            notes       TEXT,
            added_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id    INTEGER NOT NULL REFERENCES events(id),
            scraped_at  TEXT NOT NULL,
            lowest_ask  REAL,
            highest_bid REAL,
            num_asks    INTEGER,
            num_bids    INTEGER,
            last_sale   REAL,
            raw_json    TEXT,
            UNIQUE(event_id, scraped_at)
        );
    """)
    conn.commit()


def get_or_create_event(conn: sqlite3.Connection, event: dict) -> int:
    """Insert or update an event row, return its id."""
    conn.execute("""
        INSERT INTO events (crowdvolt_url, artist, venue, city, event_date,
                            face_value, us_listeners, genre, cap, avail, notes)
        VALUES (:url, :artist, :venue, :city, :event_date,
                :face_value, :us_listeners, :genre, :cap, :avail, :notes)
        ON CONFLICT(crowdvolt_url) DO UPDATE SET
            artist       = excluded.artist,
            venue        = excluded.venue,
            city         = excluded.city,
            event_date   = excluded.event_date,
            face_value   = excluded.face_value,
            us_listeners = excluded.us_listeners,
            genre        = excluded.genre,
            cap          = excluded.cap,
            avail        = excluded.avail,
            notes        = excluded.notes
    """, {
        "url": event["url"],
        "artist": event.get("artist", "Unknown"),
        "venue": event.get("venue", ""),
        "city": event.get("city", ""),
        "event_date": event.get("event_date", ""),
        "face_value": event.get("face_value"),
        "us_listeners": event.get("us_listeners"),
        "genre": event.get("genre", ""),
        "cap": event.get("cap", ""),
        "avail": event.get("avail", ""),
        "notes": event.get("notes", ""),
    })
    conn.commit()
    row = conn.execute(
        "SELECT id FROM events WHERE crowdvolt_url = ?", (event["url"],)
    ).fetchone()
    return row[0]


def save_snapshot(conn: sqlite3.Connection, event_id: int, data: dict) -> None:
    scraped_at = datetime.utcnow().strftime("%Y-%m-%d")
    conn.execute("""
        INSERT OR REPLACE INTO price_snapshots
            (event_id, scraped_at, lowest_ask, highest_bid,
             num_asks, num_bids, last_sale, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event_id,
        scraped_at,
        data.get("lowest_ask"),
        data.get("highest_bid"),
        data.get("num_asks"),
        data.get("num_bids"),
        data.get("last_sale"),
        json.dumps(data),
    ))
    conn.commit()


# ─── Scraping ─────────────────────────────────────────────────────────────────

def parse_price(text: str) -> Optional[float]:
    """Extract a float from a price string like '$142.50' or '142'."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text.strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def scrape_event_page(page, url: str) -> dict:
    """
    Navigate to a CrowdVolt event page and extract price data.

    CrowdVolt is a React SPA — we wait for key elements to appear rather
    than parsing raw HTML. The selectors below target the bid/ask order book
    and are the most likely stable points on the page; update them if the
    site redesigns (see README for how to find new selectors).
    """
    result = {
        "url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "lowest_ask": None,
        "highest_bid": None,
        "num_asks": None,
        "num_bids": None,
        "last_sale": None,
        "error": None,
    }

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

        # Wait for any price to appear — adjust selector if CV changes layout
        try:
            page.wait_for_selector(
                "[data-testid='lowest-ask'], .ask-price, .lowest-ask, "
                "[class*='askPrice'], [class*='lowestAsk'], "
                "text=/\\$[0-9]/",
                timeout=PAGE_TIMEOUT,
            )
        except PlaywrightTimeout:
            logger.warning(f"  Timed out waiting for price elements on {url}")

        # ── Strategy 1: look for data-testid attributes (most reliable) ──
        for selector in ["[data-testid='lowest-ask']", ".lowest-ask", "[class*='lowestAsk']"]:
            el = page.query_selector(selector)
            if el:
                result["lowest_ask"] = parse_price(el.inner_text())
                break

        for selector in ["[data-testid='highest-bid']", ".highest-bid", "[class*='highestBid']"]:
            el = page.query_selector(selector)
            if el:
                result["highest_bid"] = parse_price(el.inner_text())
                break

        for selector in ["[data-testid='last-sale']", ".last-sale", "[class*='lastSale']"]:
            el = page.query_selector(selector)
            if el:
                result["last_sale"] = parse_price(el.inner_text())
                break

        # ── Strategy 2: count ask / bid rows in the order book ──
        ask_rows = page.query_selector_all("[class*='askRow'], [data-testid='ask-row'], .ask-row")
        bid_rows = page.query_selector_all("[class*='bidRow'], [data-testid='bid-row'], .bid-row")
        if ask_rows:
            result["num_asks"] = len(ask_rows)
        if bid_rows:
            result["num_bids"] = len(bid_rows)

        # ── Strategy 3: fallback — scrape all dollar amounts on the page ──
        # If specific selectors didn't work, harvest all visible prices and
        # take the minimum as a proxy for lowest ask.
        if result["lowest_ask"] is None:
            all_prices = []
            price_els = page.query_selector_all("text=/^\\$[0-9]/")
            for el in price_els:
                p = parse_price(el.inner_text())
                if p and p > 0:
                    all_prices.append(p)
            if all_prices:
                result["lowest_ask"] = min(all_prices)
                logger.debug(f"  Fallback prices found: {sorted(all_prices)}")

        logger.info(
            f"  ask=${result['lowest_ask']}  bid=${result['highest_bid']}  "
            f"last=${result['last_sale']}  asks={result['num_asks']}"
        )

    except PlaywrightTimeout:
        result["error"] = "timeout"
        logger.error(f"  Page timeout: {url}")
    except Exception as exc:
        result["error"] = str(exc)
        logger.error(f"  Error scraping {url}: {exc}")

    return result


# ─── Export for dashboard ─────────────────────────────────────────────────────

def export_dashboard_json(conn: sqlite3.Connection) -> None:
    """
    Write dashboard_data.json — the format expected by the profit tracker.
    The dashboard JS reads this file on load and populates its state.
    """
    events = conn.execute("SELECT * FROM events ORDER BY event_date").fetchall()
    event_cols = [d[0] for d in conn.execute("SELECT * FROM events LIMIT 0").description]

    output = {"exported_at": datetime.utcnow().isoformat(), "events": [], "prices": []}

    for ev_row in events:
        ev = dict(zip(event_cols, ev_row))
        output["events"].append({
            "id": ev["id"],
            "artist": ev["artist"],
            "venue": ev["venue"],
            "city": ev["city"],
            "date": ev["event_date"],
            "face": ev["face_value"],
            "listeners": ev["us_listeners"],
            "genre": ev["genre"],
            "cap": ev["cap"],
            "avail": ev["avail"],
            "notes": ev["notes"],
            "url": ev["crowdvolt_url"],
        })

        snaps = conn.execute("""
            SELECT scraped_at, lowest_ask, highest_bid, num_asks, num_bids, last_sale
            FROM   price_snapshots
            WHERE  event_id = ?
            ORDER  BY scraped_at
        """, (ev["id"],)).fetchall()

        for s in snaps:
            if s[1] is None:
                continue  # skip days with no price data
            output["prices"].append({
                "eventId": ev["id"],
                "date": s[0],
                "price": s[1],          # lowest ask = the buy-now price
                "highestBid": s[2],
                "numAsks": s[3],
                "numBids": s[4],
                "lastSale": s[5],
                "section": "GA",
                "qty": s[3] or 1,
            })

    EXPORT_PATH.write_text(json.dumps(output, indent=2))
    logger.info(f"Dashboard JSON exported → {EXPORT_PATH}")


# ─── Main run ─────────────────────────────────────────────────────────────────

def load_events_config() -> list[dict]:
    """Load the list of events to track from events.json."""
    if not EVENTS_CONFIG.exists():
        logger.warning(f"{EVENTS_CONFIG} not found — creating sample file.")
        sample = [
            {
                "url": "https://www.crowdvolt.com/events/example-event-1",
                "artist": "Charlotte de Witte",
                "venue": "Brooklyn Mirage",
                "city": "New York, NY",
                "event_date": "2026-07-12",
                "face_value": 55,
                "us_listeners": 820000,
                "genre": "Techno",
                "cap": "large",
                "avail": "soldout",
                "notes": "First US tour headliner",
            }
        ]
        EVENTS_CONFIG.write_text(json.dumps(sample, indent=2))
        logger.info(f"Sample events.json created at {EVENTS_CONFIG} — edit it with your real event URLs.")
    return json.loads(EVENTS_CONFIG.read_text())


def run_scrape() -> None:
    logger.info(f"=== Scrape run started at {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")
    events = load_events_config()
    if not events:
        logger.warning("No events configured. Edit events.json to add event URLs.")
        return

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        for i, event in enumerate(events):
            url = event.get("url", "").strip()
            if not url:
                logger.warning(f"  Event #{i+1} has no URL — skipping.")
                continue

            logger.info(f"Scraping [{i+1}/{len(events)}]: {event.get('artist', url)}")
            data = scrape_event_page(page, url)

            event_id = get_or_create_event(conn, event)
            save_snapshot(conn, event_id, data)

            if i < len(events) - 1:
                time.sleep(REQUEST_DELAY)

        browser.close()

    export_dashboard_json(conn)
    conn.close()
    logger.info("=== Scrape run complete ===\n")


# ─── Scheduler entry point ────────────────────────────────────────────────────

def main() -> None:
    logger.add(
        Path(__file__).parent / "scraper.log",
        rotation="1 week",
        retention="4 weeks",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )

    # Run immediately on startup so you get data right away
    run_scrape()

    # Then schedule the daily job
    schedule.every().day.at(DAILY_RUN_TIME).do(run_scrape)
    logger.info(f"Scheduler started — next run at {DAILY_RUN_TIME} every day.")
    logger.info("Press Ctrl+C to stop.")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
