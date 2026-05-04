import os
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in your .env file.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

PROPERTIES_URL = "https://rentinmissoula.com/properties/"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; contact: parker.munsey)",
}

RENT_RE = re.compile(r"Rent:\s*\$([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
BEDS_RE = re.compile(r"(\d+)\s*Bedrooms?", re.IGNORECASE)
BATHS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*Bathrooms?", re.IGNORECASE)
SQFT_RE = re.compile(r"(\d+)\s*Square\s*Feet", re.IGNORECASE)


def clean(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def find_card_container(a_tag):
    """
    The 'View Details' button is inside a listing card.
    Walk up the DOM until we hit a reasonable container that holds the card text.
    """
    node = a_tag
    for _ in range(8):
        if not node:
            break
        if getattr(node, "name", None) in ("article", "section", "div"):
            txt = node.get_text(" ", strip=True).lower()
            if "rent:" in txt and ("bedroom" in txt or "square feet" in txt):
                return node
        node = node.parent
    return a_tag.parent


def parse_listings(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")

    listings: list[dict] = []
    seen_details = set()

    detail_links = soup.find_all("a", href=True)
    for a in detail_links:
        link_text = a.get_text(" ", strip=True).lower()
        href = a["href"]

        if "view details" not in link_text:
            continue
        if "unit-details" not in href:
            continue

        details_url = urljoin(PROPERTIES_URL, href)
        if details_url in seen_details:
            continue
        seen_details.add(details_url)

        card = find_card_container(a)
        card_text = card.get_text(" ", strip=True)

        listing_title = clean(card_text.split("Available")[0]) if "Available" in card_text else None

        available_text = None
        m = re.search(r"(Available.*?)(?:\s+Rent:|$)", card_text, re.IGNORECASE)
        if m:
            available_text = clean(m.group(1))

        rent = None
        beds = None
        baths = None
        sqft = None

        m = RENT_RE.search(card_text)
        if m:
            rent = float(m.group(1).replace(",", ""))

        m = BEDS_RE.search(card_text)
        if m:
            beds = int(m.group(1))

        m = BATHS_RE.search(card_text)
        if m:
            baths = float(m.group(1))

        m = SQFT_RE.search(card_text)
        if m:
            sqft = int(m.group(1))

        apply_url = None
        for link in card.find_all("a", href=True):
            t = link.get_text(" ", strip=True).lower()
            if "apply now" in t:
                apply_url = urljoin(PROPERTIES_URL, link["href"])
                break

        listings.append(
            {
                "listing_title": listing_title,
                "available_text": available_text,
                "beds": beds,
                "baths": baths,
                "sqft": sqft,
                "rent": rent,
                "details_url": details_url,
                "apply_url": apply_url,
                "scraped_at": datetime.now(timezone.utc),
            }
        )

    return listings


def main():
    html = fetch_html(PROPERTIES_URL)
    listings = parse_listings(html)

    print(f"Found {len(listings)} listings from {PROPERTIES_URL}")

    with engine.begin() as conn:
        for x in listings:
            # ---- UPSERT into mpm_listings ----
            conn.execute(
                text(
                    """
                    INSERT INTO mpm_listings
                      (listing_title, available_text, beds, baths, sqft, rent, details_url, apply_url, scraped_at)
                    VALUES
                      (:listing_title, :available_text, :beds, :baths, :sqft, :rent, :details_url, :apply_url, :scraped_at)
                    ON CONFLICT (details_url) DO UPDATE SET
                      listing_title = EXCLUDED.listing_title,
                      available_text = EXCLUDED.available_text,
                      beds = EXCLUDED.beds,
                      baths = EXCLUDED.baths,
                      sqft = EXCLUDED.sqft,
                      rent = EXCLUDED.rent,
                      apply_url = COALESCE(EXCLUDED.apply_url, mpm_listings.apply_url),
                      scraped_at = EXCLUDED.scraped_at;
                    """
                ),
                x,
            )

            observed_at = x.get("scraped_at") or datetime.now(timezone.utc)

            # ---- Append-only insert into raw_listings ----
            conn.execute(
                text(
                    """
                    INSERT INTO raw_listings (
                        source,
                        source_url,
                        source_record_id,
                        scraped_at,
                        raw_text,
                        raw_json
                    )
                    VALUES (
                        'mpm',
                        :source_url,
                        :source_record_id,
                        :scraped_at,
                        :raw_text,
                        :raw_json
                    );
                    """
                ),
                {
                    "source_url": x.get("details_url") or PROPERTIES_URL,
                    "source_record_id": x.get("details_url"),
                    "scraped_at": observed_at,
                    "raw_text": (
                        f"title={x.get('listing_title')}; "
                        f"available={x.get('available_text')}; "
                        f"beds={x.get('beds')}; baths={x.get('baths')}; "
                        f"sqft={x.get('sqft')}; rent={x.get('rent')}; "
                        f"details={x.get('details_url')}; apply={x.get('apply_url')}"
                    ),
                    "raw_json": json.dumps(x, ensure_ascii=False, default=str),
                },
            )

    print("MPM ingestion complete for mpm_listings and raw_listings.")
    print("Next step: run normalize_raw_to_stg.py to refresh stg_listings.")


if __name__ == "__main__":
    main()
