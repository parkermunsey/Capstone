import os
import json
import hashlib
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

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


def build_source_record_id(record):
    """
    Craigslist items are compact arrays, not dicts.
    Use a stable hash of the raw record for the raw layer.
    """
    raw = json.dumps(record, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def extract_title(record):
    try:
        return record[10]
    except Exception:
        return None


def extract_price(record):
    try:
        return record[3]
    except Exception:
        return None


def main():
    payload = fetch_payload()
    records = payload["data"]["items"]

    print(f"Found {len(records)} Craigslist listings")

    observed_at = datetime.now(timezone.utc)
    inserted = 0

    with engine.begin() as conn:
        for record in records:
            source_record_id = build_source_record_id(record)

            title = extract_title(record)
            price = extract_price(record)

            raw_text_parts = []
            if title:
                raw_text_parts.append(f"title={title}")
            if price is not None:
                raw_text_parts.append(f"price={price}")
            raw_text_parts.append(json.dumps(record))

            raw_text = " | ".join(raw_text_parts)

            conn.execute(text("""
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
            """), {
                "source_url": API_URL,
                "source_record_id": source_record_id,
                "scraped_at": observed_at,
                "raw_text": raw_text,
                "raw_json": json.dumps(record),
            })

            inserted += 1

    print(f"Craigslist ingestion complete. Inserted {inserted} records.")


if __name__ == "__main__":
    main()