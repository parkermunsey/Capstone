import os
import json
import hashlib
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

API_URL = "https://www.adeapm.com/rts/collections/public/83edddd7/runtime/collection/appfolio-listings/query-data?pageSize=100&pageNumber=0&query=%28%29&language=ENGLISH"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; Parker Munsey)"
}


def parse_listing(data: dict) -> dict:
    return {
        "listing_title": data.get("title"),
        "beds": data.get("bedrooms"),
        "baths": data.get("bathrooms"),
        "sqft": data.get("square_feet"),
        "rent": data.get("rent"),
        "address": data.get("address"),
        "available_text": data.get("available_date"),
    }


def main():
    resp = requests.get(API_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    listings = payload.get("values", [])

    print(f"Found {len(listings)} ADEA listings")

    if not listings:
        print("No ADEA listings found. Exiting cleanly.")
        return

    with engine.begin() as conn:
        for item in listings:
            data = item.get("data", {})
            parsed = parse_listing(data)

            page_item_url = item.get("page_item_url")
            details_url = None
            if page_item_url:
                details_url = f"https://www.adeapm.com/listings/detail/{page_item_url}"

            observed_at = datetime.now(timezone.utc)

            conn.execute(
                text("""
                    INSERT INTO raw_listings (
                        source,
                        source_url,
                        source_record_id,
                        scraped_at,
                        raw_text,
                        raw_json
                    )
                    VALUES (
                        'adea',
                        :source_url,
                        :source_record_id,
                        :scraped_at,
                        :raw_text,
                        CAST(:raw_json AS jsonb)
                    )
                """),
                {
                    "source_url": API_URL,
                    "source_record_id": page_item_url,
                    "scraped_at": observed_at,
                    "raw_text": json.dumps(data, ensure_ascii=False),
                    "raw_json": json.dumps(data, ensure_ascii=False),
                }
            )

            fingerprint_base = (
                f"{parsed.get('listing_title') or ''}|"
                f"{parsed.get('beds') or ''}|"
                f"{parsed.get('rent') or ''}|"
                f"{details_url or ''}"
            )
            fingerprint = hashlib.md5(fingerprint_base.encode("utf-8")).hexdigest()

            conn.execute(
                text("""
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
                        availability_text_raw,
                        is_currently_available,
                        listing_url,
                        observed_at,
                        listing_fingerprint
                    )
                    VALUES (
                        'adea',
                        :source_record_id,
                        :listing_title,
                        :bedrooms,
                        :bathrooms,
                        :sqft,
                        :rent_min,
                        'month',
                        :availability_status,
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
                        availability_status = EXCLUDED.availability_status,
                        availability_text_raw = EXCLUDED.availability_text_raw,
                        is_currently_available = EXCLUDED.is_currently_available,
                        listing_url = EXCLUDED.listing_url,
                        observed_at = EXCLUDED.observed_at,
                        listing_fingerprint = EXCLUDED.listing_fingerprint
                """),
                {
                    "source_record_id": page_item_url,
                    "listing_title": parsed.get("listing_title"),
                    "bedrooms": parsed.get("beds"),
                    "bathrooms": parsed.get("baths"),
                    "sqft": parsed.get("sqft"),
                    "rent_min": parsed.get("rent"),
                    "availability_status": "available",
                    "availability_text_raw": parsed.get("available_text"),
                    "is_currently_available": True,
                    "listing_url": details_url,
                    "observed_at": observed_at,
                    "listing_fingerprint": fingerprint,
                }
            )

    print("ADEA ingestion complete.")


if __name__ == "__main__":
    main()