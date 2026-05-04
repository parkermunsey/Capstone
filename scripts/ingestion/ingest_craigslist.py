import os
import json
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

try:
    from scripts.staging.utils import (
        clean_text,
        normalize_bedrooms,
        normalize_listing_url,
        normalize_sqft,
        parse_float_from_text,
    )
except ModuleNotFoundError:
    from utils import (
        clean_text,
        normalize_bedrooms,
        normalize_listing_url,
        normalize_sqft,
        parse_float_from_text,
    )

load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in your .env file.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

API_URL = (
    "https://sapi.craigslist.org/web/v8/postings/search/full"
    "?batch=656-0-360-0-0&searchPath=apa&lang=en&cc=us"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) Gecko/20100101 Firefox/148.0",
    "Accept": "*/*",
    "Origin": "https://missoula.craigslist.org",
    "Referer": "https://missoula.craigslist.org/",
}


def fetch_payload():
    resp = requests.get(API_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def build_fallback_source_record_id(record):
    raw = json.dumps(record, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def build_area_lookup(decode):
    raw_locations = decode.get("locations") or []
    lookup = {}

    for idx, item in enumerate(raw_locations):
        if idx == 0 or not isinstance(item, list) or len(item) < 2:
            continue
        lookup[idx] = clean_text(item[1])

    return lookup


def decode_posting_id(record, decode):
    if not isinstance(record, list) or len(record) < 1:
        return None

    base_posting_id = decode.get("minPostingId")
    try:
        return int(base_posting_id) + int(record[0])
    except (TypeError, ValueError):
        return None


def decode_posted_at(record, decode):
    if not isinstance(record, list) or len(record) < 2:
        return None

    base_posted_date = decode.get("minPostedDate")
    try:
        timestamp = int(base_posted_date) + int(record[1])
    except (TypeError, ValueError):
        return None

    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def extract_title(record):
    if isinstance(record, list) and len(record) > 10:
        return clean_text(record[10])
    return None


def extract_price(record):
    if isinstance(record, list) and len(record) > 3:
        return record[3]
    return None


def extract_slug(record):
    if not isinstance(record, list) or len(record) <= 8:
        return None

    slug_data = record[8]
    if isinstance(slug_data, list) and len(slug_data) > 1:
        return clean_text(slug_data[1])

    return None


def extract_location_text(record, decode):
    if not isinstance(record, list) or len(record) <= 4:
        return None

    location_value = clean_text(record[4])
    if not location_value:
        return None

    match = re.match(r"^\d+:(\d+)~", location_value)
    if not match:
        return None

    try:
        location_index = int(match.group(1))
    except ValueError:
        return None

    descriptions = decode.get("locationDescriptions") or []
    if location_index < len(descriptions):
        return clean_text(descriptions[location_index])

    return None


def extract_bedrooms(record):
    if not isinstance(record, list) or len(record) <= 11 or not isinstance(record[11], list):
        return None

    extra = record[11]
    if len(extra) > 1:
        return normalize_bedrooms(extra[1])

    return None


def extract_sqft(record):
    if not isinstance(record, list) or len(record) <= 11 or not isinstance(record[11], list):
        return None

    extra = record[11]
    if len(extra) > 2:
        sqft = normalize_sqft(extra[2])
        if sqft and sqft > 0:
            return sqft

    return None


def extract_bathrooms(title):
    return parse_float_from_text(title, r"(\d+(?:\.\d+)?)\s*(?:bath|bathroom|ba)\b")


def build_listing_url(record, payload, posting_id):
    slug = extract_slug(record)
    if not slug or not posting_id:
        return None

    data = payload.get("data", {})
    decode = data.get("decode", {})
    area_lookup = build_area_lookup(decode)

    default_host = clean_text(data.get("location", {}).get("url")) or "missoula.craigslist.org"
    host = default_host

    if isinstance(record, list) and len(record) > 2:
        area_code = area_lookup.get(record[2])
        if area_code:
            host = f"{area_code}.craigslist.org"

    category_abbr = clean_text(data.get("categoryAbbr")) or "apa"
    return normalize_listing_url(f"https://{host}/{category_abbr}/d/{slug}/{posting_id}.html")


def build_structured_raw_payload(record, payload):
    decode = payload.get("data", {}).get("decode", {})
    posting_id = decode_posting_id(record, decode)
    posted_at = decode_posted_at(record, decode)
    title = extract_title(record)
    price = extract_price(record)
    bedrooms = extract_bedrooms(record)
    sqft = extract_sqft(record)
    bathrooms = extract_bathrooms(title)
    location_text = extract_location_text(record, decode)
    listing_url = build_listing_url(record, payload, posting_id)

    return {
        "record": record,
        "posting_id": str(posting_id) if posting_id else None,
        "posted_at": posted_at.isoformat() if posted_at else None,
        "title": title,
        "price": price,
        "price_text": f"${int(price):,}" if isinstance(price, (int, float)) else None,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "sqft": sqft,
        "location_text": location_text,
        "listing_url": listing_url,
        "source_page": API_URL,
    }


def main():
    payload = fetch_payload()
    records = payload["data"]["items"]

    print(f"Found {len(records)} Craigslist listings")

    observed_at = datetime.now(timezone.utc)
    inserted = 0

    with engine.begin() as conn:
        for record in records:
            structured_raw = build_structured_raw_payload(record, payload)

            source_record_id = structured_raw.get("posting_id") or build_fallback_source_record_id(record)
            title = structured_raw.get("title")
            price = structured_raw.get("price")
            listing_url = structured_raw.get("listing_url")

            raw_text_parts = [f"source_record_id={source_record_id}"]
            if title:
                raw_text_parts.append(f"title={title}")
            if price is not None:
                raw_text_parts.append(f"price={price}")
            if structured_raw.get("bedrooms") is not None:
                raw_text_parts.append(f"bedrooms={structured_raw['bedrooms']}")
            if structured_raw.get("bathrooms") is not None:
                raw_text_parts.append(f"bathrooms={structured_raw['bathrooms']}")
            if structured_raw.get("sqft") is not None:
                raw_text_parts.append(f"sqft={structured_raw['sqft']}")
            if structured_raw.get("location_text"):
                raw_text_parts.append(f"location={structured_raw['location_text']}")
            if listing_url:
                raw_text_parts.append(f"url={listing_url}")
            raw_text_parts.append(json.dumps(record, ensure_ascii=False))

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
                        'craigslist',
                        :source_url,
                        :source_record_id,
                        :scraped_at,
                        :raw_text,
                        CAST(:raw_json AS jsonb)
                    )
                """
                ),
                {
                    "source_url": listing_url or API_URL,
                    "source_record_id": source_record_id,
                    "scraped_at": observed_at,
                    "raw_text": " | ".join(raw_text_parts),
                    "raw_json": json.dumps(structured_raw, ensure_ascii=False),
                },
            )

            inserted += 1

    print(f"Craigslist ingestion complete. Inserted {inserted} records.")


if __name__ == "__main__":
    main()
