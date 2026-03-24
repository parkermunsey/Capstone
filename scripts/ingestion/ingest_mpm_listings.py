import os
import re
import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

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


def parse_mpm_availability(available_text: str | None, observed_at: datetime):
    """
    Returns: (availability_status, available_date, is_currently_available)
    - availability_status: available / waitlist / unavailable / unknown
    - available_date: date or None (parsed from m/d/yyyy)
    - is_currently_available: boolean based on status + date vs observed date
    """
    t = (available_text or "").strip()
    t_lower = t.lower()

    if "wait" in t_lower:
        status = "waitlist"
    elif "unavailable" in t_lower or "not available" in t_lower:
        status = "unavailable"
    elif "available" in t_lower:
        status = "available"
    else:
        status = "unknown"

    available_date = None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", t)
    if m:
        try:
            available_date = datetime.strptime(m.group(1), "%m/%d/%Y").date()
        except ValueError:
            available_date = None

    today = (observed_at or datetime.now(timezone.utc)).date()
    if status == "available":
        if available_date is None:
            is_current = True
        else:
            is_current = available_date <= today
    else:
        is_current = False

    return status, available_date, is_current


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
            availability_status, available_date, is_currently_available = parse_mpm_availability(
                x.get("available_text"), observed_at
            )

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
                    "source_url": PROPERTIES_URL,
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

            # ---- UPSERT into stg_listings (now includes availability fields) ----
            fingerprint_base = (
                f"{(x.get('listing_title') or '').strip().lower()}|"
                f"{x.get('beds')}|"
                f"{x.get('rent')}|"
                f"{x.get('details_url')}"
            )
            listing_fingerprint = hashlib.md5(fingerprint_base.encode("utf-8")).hexdigest()

            conn.execute(
                text(
                    """
                    INSERT INTO stg_listings (
                        source,
                        source_record_id,
                        listing_title,
                        bedrooms,
                        bathrooms,
                        sqft,
                        rent_min,
                        rent_period,
                        availability_status,
                        available_date,
                        availability_text_raw,
                        is_currently_available,
                        listing_url,
                        observed_at,
                        listing_fingerprint
                    )
                    VALUES (
                        'mpm',
                        :source_record_id,
                        :listing_title,
                        :bedrooms,
                        :bathrooms,
                        :sqft,
                        :rent_min,
                        'month',
                        :availability_status,
                        :available_date,
                        :availability_text_raw,
                        :is_currently_available,
                        :listing_url,
                        :observed_at,
                        :listing_fingerprint
                    )
                    ON CONFLICT (source, source_record_id) DO UPDATE SET
                        listing_title = EXCLUDED.listing_title,
                        bedrooms = EXCLUDED.bedrooms,
                        bathrooms = EXCLUDED.bathrooms,
                        sqft = EXCLUDED.sqft,
                        rent_min = EXCLUDED.rent_min,
                        rent_period = EXCLUDED.rent_period,
                        availability_status = EXCLUDED.availability_status,
                        available_date = EXCLUDED.available_date,
                        availability_text_raw = EXCLUDED.availability_text_raw,
                        is_currently_available = EXCLUDED.is_currently_available,
                        listing_url = EXCLUDED.listing_url,
                        observed_at = EXCLUDED.observed_at,
                        listing_fingerprint = EXCLUDED.listing_fingerprint;
                    """
                ),
                {
                    "source_record_id": x["details_url"],
                    "listing_title": x.get("listing_title"),
                    "bedrooms": int(x["beds"]) if x.get("beds") is not None else None,
                    "bathrooms": x.get("baths"),
                    "sqft": x.get("sqft"),
                    "rent_min": int(x["rent"]) if x.get("rent") is not None else None,
                    "availability_status": availability_status,
                    "available_date": available_date,
                    "availability_text_raw": x.get("available_text"),
                    "is_currently_available": is_currently_available,
                    "listing_url": x.get("details_url"),
                    "observed_at": observed_at,
                    "listing_fingerprint": listing_fingerprint,
                },
            )

    print("Upsert complete for mpm_listings, raw_listings, and stg_listings.")


if __name__ == "__main__":
    main()