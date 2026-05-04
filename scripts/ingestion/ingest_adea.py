import os
import re
import json
import base64
from pathlib import Path
from datetime import datetime, timezone

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

API_URL = (
    "https://www.adeapm.com/rts/collections/public/83edddd7/runtime/"
    "collection/appfolio-listings/query-data?pageSize=100&pageNumber=0"
    "&query=%28%29&language=ENGLISH"
)

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; Parker Munsey)"
}

DEBUG_ADEA = False
DEBUG_TARGETS = {
    "beadc919-6b06-4232-88b5-18a7f0cbdf2e",  # 7160 Rowan
}


def clean_text(value):
    if value is None:
        return None
    text_value = str(value).replace("\xa0", " ").strip()
    if not text_value:
        return None
    text_value = re.sub(r"\s+", " ", text_value)
    return text_value


def should_debug(source_record_id: str | None) -> bool:
    return DEBUG_ADEA and source_record_id in DEBUG_TARGETS


def extract_base64_json_row_data(resp_text: str) -> dict:
    match = re.search(r"base64JsonRowData:\s*'([^']+)'", resp_text)
    if not match:
        return {}

    try:
        decoded = base64.b64decode(match.group(1))
        parsed = json.loads(decoded)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def format_currency(value) -> str | None:
    if value is None or value == "":
        return None

    try:
        return f"${int(float(value)):,}"
    except (TypeError, ValueError):
        pass

    text_value = clean_text(value)
    if not text_value:
        return None

    match = re.search(r"\$?\s*([\d,]+(?:\.\d+)?)", text_value)
    if not match:
        return None

    try:
        return f"${int(float(match.group(1).replace(',', ''))):,}"
    except (TypeError, ValueError):
        return None


def normalize_number_text(value) -> str | None:
    if value is None or value == "":
        return None

    try:
        num = float(value)
        if num.is_integer():
            return str(int(num))
        return str(num)
    except (TypeError, ValueError):
        return clean_text(value)


def extract_property_list_names(embedded: dict) -> list[str]:
    names = []
    for item in embedded.get("property_lists") or []:
        if isinstance(item, dict):
            name = clean_text(item.get("name"))
            if name:
                names.append(name)
    return names


def extract_detail_page_fields(details_url: str, source_record_id: str | None = None) -> dict:
    try:
        resp = requests.get(details_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        if should_debug(source_record_id):
            print(f"\nADEA DEBUG request failed for {details_url}: {exc}")
        return {}

    if should_debug(source_record_id):
        with open("rowan_debug.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        print("\nSaved Rowan HTML to rowan_debug.html")

    soup = BeautifulSoup(resp.text, "html.parser")
    embedded = extract_base64_json_row_data(resp.text)

    h1 = soup.find("h1")
    h3 = soup.find("h3")

    detail_title = clean_text(h1.get_text(" ", strip=True) if h1 else None)
    detail_subtitle = clean_text(h3.get_text(" ", strip=True) if h3 else None)

    property_list_names = extract_property_list_names(embedded)
    property_list_text = ", ".join(property_list_names) if property_list_names else None
    property_list_names_lower = [name.lower() for name in property_list_names]

    listing_commercial_detail = embedded.get("listing_commercial_detail")
    is_commercial = (
        any("commercial" in name for name in property_list_names_lower)
        or isinstance(listing_commercial_detail, dict)
    )

    rent_text = None
    for candidate in [
        embedded.get("market_rent"),
        embedded.get("rent"),
        embedded.get("rent_display"),
    ]:
        rent_text = format_currency(candidate)
        if rent_text:
            break

    if not rent_text:
        rent_range = embedded.get("rent_range")
        if isinstance(rent_range, list) and rent_range:
            if len(rent_range) == 1:
                rent_text = format_currency(rent_range[0])
            elif len(rent_range) >= 2 and rent_range[0] is not None:
                rent_text = format_currency(rent_range[0])

    bedrooms_text = normalize_number_text(embedded.get("bedrooms"))
    bathrooms_text = normalize_number_text(embedded.get("bathrooms"))
    square_feet_text = normalize_number_text(embedded.get("square_feet"))

    available_date_text = clean_text(embedded.get("available_date"))
    property_type = clean_text(embedded.get("property_type"))

    if should_debug(source_record_id):
        print("Extracted embedded fields:")
        print({
            "detail_title": detail_title,
            "detail_subtitle": detail_subtitle,
            "rent_text": rent_text,
            "bedrooms_text": bedrooms_text,
            "bathrooms_text": bathrooms_text,
            "square_feet_text": square_feet_text,
            "available_date": available_date_text,
            "property_type": property_type,
            "property_lists": property_list_names,
            "is_commercial": is_commercial,
        })

    return {
        "detail_title": detail_title,
        "detail_subtitle": detail_subtitle,
        "rent_text": rent_text,
        "bedrooms_text": bedrooms_text,
        "bathrooms_text": bathrooms_text,
        "square_feet_text": square_feet_text,
        "available_date_text": available_date_text,
        "property_type": property_type,
        "property_lists_text": property_list_text,
        "is_commercial": is_commercial,
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

    inserted_raw = 0

    with engine.begin() as conn:
        for item in listings:
            data = item.get("data", {}) or {}
            page_item_url = item.get("page_item_url")

            details_url = None
            if page_item_url:
                details_url = f"https://www.adeapm.com/listings/detail/{page_item_url}"

            detail_fields = {}
            if details_url:
                detail_fields = extract_detail_page_fields(
                    details_url=details_url,
                    source_record_id=page_item_url,
                )

            raw_payload = {
                **data,
                "details_url": details_url,
                "detail_title": detail_fields.get("detail_title"),
                "detail_subtitle": detail_fields.get("detail_subtitle"),
                "rent_text": detail_fields.get("rent_text"),
                "bedrooms_text": detail_fields.get("bedrooms_text"),
                "bathrooms_text": detail_fields.get("bathrooms_text"),
                "square_feet_text": detail_fields.get("square_feet_text"),
                "available_date_text": detail_fields.get("available_date_text"),
                "property_type": detail_fields.get("property_type"),
                "property_lists_text": detail_fields.get("property_lists_text"),
                "is_commercial": detail_fields.get("is_commercial"),
            }

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
                    "source_url": details_url or API_URL,
                    "source_record_id": page_item_url,
                    "scraped_at": observed_at,
                    "raw_text": json.dumps(raw_payload, ensure_ascii=False),
                    "raw_json": json.dumps(raw_payload, ensure_ascii=False),
                }
            )
            inserted_raw += 1

    print(f"ADEA raw ingestion complete. Inserted {inserted_raw} rows into raw_listings.")
    print("Next step: run normalize_raw_to_stg.py to refresh stg_listings.")


if __name__ == "__main__":
    main()