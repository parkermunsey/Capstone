from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

try:
    from scripts.staging.utils import (
        clean_text,
        coalesce_text,
        extract_address_candidate,
        is_currently_available,
        is_available_soon,
        looks_like_address,
        looks_like_placeholder_title,
        make_cross_source_fingerprint,
        make_listing_fingerprint,
        normalize_availability,
        normalize_bathrooms,
        normalize_bedrooms,
        normalize_listing_url,
        normalize_rent,
        normalize_sqft,
        parse_address_components,
        parse_available_date,
        parse_float_from_text,
    )
except ModuleNotFoundError:
    from utils import (
        clean_text,
        coalesce_text,
        extract_address_candidate,
        is_currently_available,
        is_available_soon,
        looks_like_address,
        looks_like_placeholder_title,
        make_cross_source_fingerprint,
        make_listing_fingerprint,
        normalize_availability,
        normalize_bathrooms,
        normalize_bedrooms,
        normalize_listing_url,
        normalize_rent,
        normalize_sqft,
        parse_address_components,
        parse_available_date,
        parse_float_from_text,
    )

load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in your .env file.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

ADEA_LISTING_BASE_URL = "https://www.adeapm.com/listings/detail/"


def extract_lat_lon_from_record(record):
    """
    Craigslist record format stores location in record[4], like:
    "1:1~46.8753~-113.9356"
    """
    try:
        if isinstance(record, list) and len(record) > 4:
            loc = record[4]
            if isinstance(loc, str) and "~" in loc:
                parts = loc.split("~")
                if len(parts) == 3:
                    lat = float(parts[1])
                    lon = float(parts[2])
                    return lat, lon
    except Exception:
        pass

    return None, None


def get_latest_mha_updated_date() -> str | None:
    sql = text(
        """
        SELECT MAX(raw_json->>'updated_date') AS latest_updated_date
        FROM raw_listings
        WHERE source = 'mha'
          AND raw_json->>'updated_date' IS NOT NULL
        """
    )

    with engine.connect() as conn:
        return conn.execute(sql).scalar()


LATEST_MHA_UPDATED_DATE = get_latest_mha_updated_date()


def fetch_recent_raw_rows(limit: int = 1000, source: str | None = None):
    if source:
        sql = text(
            """
            SELECT
                id,
                source,
                source_record_id,
                source_url,
                scraped_at,
                raw_text,
                raw_json
            FROM raw_listings
            WHERE source = :source
            ORDER BY scraped_at DESC
            LIMIT :limit
            """
        )
        params = {"source": source, "limit": limit}
    else:
        sql = text(
            """
            SELECT
                id,
                source,
                source_record_id,
                source_url,
                scraped_at,
                raw_text,
                raw_json
            FROM raw_listings
            ORDER BY scraped_at DESC
            LIMIT :limit
            """
        )
        params = {"limit": limit}

    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    return rows


def ensure_raw_json(value):
    if value is None:
        return {}

    if isinstance(value, (dict, list)):
        return value

    text_value = clean_text(value)
    if not text_value:
        return {}

    try:
        return json.loads(text_value)
    except json.JSONDecodeError:
        return {}


def build_adea_listing_url(source_record_id: object) -> str | None:
    source_record_id = clean_text(source_record_id)
    if not source_record_id:
        return None

    if source_record_id.startswith("http://") or source_record_id.startswith("https://"):
        return normalize_listing_url(source_record_id)

    return normalize_listing_url(ADEA_LISTING_BASE_URL + source_record_id.lstrip("/"))


def extract_mpm_bedrooms(raw_beds, availability_text):
    if availability_text:
        text_value = str(availability_text)
        tokens = re.findall(r"[A-Za-z0-9/]+", text_value)

        for i, token in enumerate(tokens):
            token_lower = token.lower()

            if token_lower in {"bedroom", "bedrooms"}:
                if i > 0 and tokens[i - 1].isdigit():
                    val = int(tokens[i - 1])
                    if 0 <= val <= 10:
                        return val

                if i + 1 < len(tokens) and tokens[i + 1].isdigit():
                    val = int(tokens[i + 1])
                    if 0 <= val <= 10:
                        return val

            if token_lower == "studio":
                return 0

    try:
        if raw_beds is not None:
            bedrooms = int(float(raw_beds))
            if 0 <= bedrooms <= 10:
                return bedrooms
    except (TypeError, ValueError):
        pass

    return None


def extract_mha_bedroom_list(vacancies_text: object) -> list[int | None]:
    text_value = clean_text(vacancies_text)
    if not text_value:
        return []

    lowered = text_value.lower()
    bedrooms = set()

    if "studio" in lowered:
        bedrooms.add(0)

    for match in re.finditer(r"\b([0-9])\s*bedrooms?\b", lowered):
        bedrooms.add(int(match.group(1)))

    for match in re.finditer(r"\b([0-9])\s*(?:br|bd|bed)\b", lowered):
        bedrooms.add(int(match.group(1)))

    return sorted(bedrooms)


def make_mha_source_record_id(
    property_name: object,
    address: object,
    updated_date: object,
    vacancies_text: object,
) -> str:
    base = (
        f"{(clean_text(property_name) or '').lower()}|"
        f"{(clean_text(address) or '').lower()}|"
        f"{clean_text(updated_date) or ''}|"
        f"{(clean_text(vacancies_text) or '').lower()}"
    )
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def extract_property_name(title: object, address: object) -> str | None:
    cleaned_title = clean_text(title)
    cleaned_address = clean_text(address)

    if not cleaned_title or looks_like_placeholder_title(cleaned_title):
        return None

    if looks_like_address(cleaned_title):
        if cleaned_address and cleaned_title == cleaned_address:
            return None
        if not cleaned_address:
            return None

    return cleaned_title


def build_stg_record(
    row,
    title,
    address_raw,
    bedrooms,
    bathrooms,
    sqft,
    rent_min,
    rent_max,
    rent_period,
    availability_text,
    listing_url,
    source_record_id: str | None = None,
    contact_name: str | None = None,
    contact_phone: str | None = None,
    contact_email: str | None = None,
    property_name: str | None = None,
    availability_status_override: str | None = None,
    available_date_override=None,
    latitude: float | None = None,
    longitude: float | None = None,
):
    title = clean_text(title)

    raw_address = clean_text(address_raw)
    if not raw_address:
        raw_address = extract_address_candidate(row.get("raw_text"))
    if not raw_address and title and looks_like_address(title):
        raw_address = title

    address_parts = parse_address_components(raw_address)
    address = clean_text(address_parts.get("display")) or raw_address

    canonical_url = normalize_listing_url(listing_url) or normalize_listing_url(row["source_url"])
    normalized_source_record_id = clean_text(source_record_id) or clean_text(row["source_record_id"])

    availability_text = clean_text(availability_text)
    available_date = available_date_override or parse_available_date(availability_text)

    if availability_status_override:
        availability_status = availability_status_override
    elif available_date:
        availability_status = "available"
    else:
        availability_status = normalize_availability(availability_text)

    observed_at = row["scraped_at"]

    property_name = clean_text(property_name) or extract_property_name(title, address)
    unit = address_parts.get("unit")

    return {
        "source": row["source"],
        "source_record_id": normalized_source_record_id,
        "listing_title": title or property_name or address,
        "address": address,
        "address_raw": raw_address,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "sqft": sqft,
        "rent_min": rent_min,
        "rent_max": rent_max,
        "rent_period": rent_period or "month",
        "availability_status": availability_status,
        "available_date": available_date,
        "availability_text_raw": availability_text,
        "contact_name": clean_text(contact_name),
        "contact_phone": clean_text(contact_phone),
        "contact_email": clean_text(contact_email),
        "is_currently_available": is_currently_available(
            availability_status,
            available_date,
            observed_at,
        ),
        "is_available_soon": is_available_soon(
            availability_status,
            available_date,
            observed_at,
        ),
        "listing_url": canonical_url,
        "observed_at": observed_at,
        "listing_fingerprint": make_listing_fingerprint(
            title,
            address,
            property_name,
            unit,
            bedrooms,
            bathrooms,
            rent_min,
            sqft,
            listing_url=canonical_url,
            source_record_id=normalized_source_record_id,
        ),
        "cross_source_fingerprint": make_cross_source_fingerprint(
            address or raw_address,
            title or property_name,
            bedrooms,
            rent_min,
            sqft,
            listing_url=canonical_url,
            unit=unit,
            property_name=property_name,
        ),
        "latitude": latitude,
        "longitude": longitude,
    }


def parse_mpm_row(row):
    raw = ensure_raw_json(row["raw_json"])
    if not isinstance(raw, dict):
        return []

    title = clean_text(raw.get("listing_title"))
    address_raw = clean_text(raw.get("address")) or title
    availability_text = clean_text(raw.get("available_text"))

    raw_beds = raw.get("beds")
    bedrooms = extract_mpm_bedrooms(raw_beds, availability_text)
    bathrooms = normalize_bathrooms(raw.get("baths"))
    sqft = normalize_sqft(raw.get("sqft"))

    rent_min, rent_max, rent_period = normalize_rent(raw.get("rent"))
    listing_url = clean_text(raw.get("details_url")) or row["source_url"]
    source_record_id = clean_text(raw.get("details_url")) or row["source_record_id"]

    return [
        build_stg_record(
            row=row,
            title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text=availability_text,
            listing_url=listing_url,
            source_record_id=source_record_id,
        )
    ]


def parse_adea_row(row):
    raw = ensure_raw_json(row["raw_json"])
    if not isinstance(raw, dict):
        return []

    if raw.get("is_commercial") is True:
        return []

    title = coalesce_text(raw.get("detail_title"), raw.get("title"), raw.get("address"))
    address_raw = coalesce_text(raw.get("address"), title if looks_like_address(title) else None)

    bedrooms = normalize_bedrooms(coalesce_text(raw.get("bedrooms"), raw.get("bedrooms_text")))
    bathrooms = normalize_bathrooms(coalesce_text(raw.get("bathrooms"), raw.get("bathrooms_text")))
    sqft = normalize_sqft(coalesce_text(raw.get("square_feet"), raw.get("square_feet_text")))

    market_rent_val = None
    market_rent = raw.get("market_rent")
    if isinstance(market_rent, dict):
        market_rent_val = market_rent.get("parsedValue")
    else:
        market_rent_val = market_rent

    rent_range_val = None
    rent_range = raw.get("rent_range")
    if isinstance(rent_range, list) and len(rent_range) > 0:
        first_val = rent_range[0]
        if isinstance(first_val, dict):
            rent_range_val = first_val.get("parsedValue")
        else:
            rent_range_val = first_val

    rent_value = coalesce_text(raw.get("rent_text"), raw.get("rent"), market_rent_val, rent_range_val)
    rent_min, rent_max, rent_period = normalize_rent(rent_value)

    availability_text = coalesce_text(
        raw.get("available_date"),
        raw.get("available_date_text"),
        raw.get("availability_text"),
    )

    listing_url = build_adea_listing_url(row["source_record_id"])

    return [
        build_stg_record(
            row=row,
            title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text=availability_text,
            listing_url=listing_url,
            source_record_id=clean_text(row["source_record_id"]),
        )
    ]


def parse_caras_row(row):
    raw = ensure_raw_json(row["raw_json"])
    if not isinstance(raw, dict):
        return []

    title = coalesce_text(raw.get("title"), raw.get("listing_title"), raw.get("address"))
    address_raw = coalesce_text(raw.get("address"), title if looks_like_address(title) else None)

    bedrooms = normalize_bedrooms(raw.get("bedrooms"))
    bathrooms = normalize_bathrooms(raw.get("bathrooms"))
    sqft = normalize_sqft(raw.get("sqft"))

    rent_min, rent_max, rent_period = normalize_rent(raw.get("price_text"))
    availability_text = clean_text(raw.get("availability_text"))
    listing_url = clean_text(raw.get("listing_url")) or row["source_url"]

    raw_text = clean_text(row.get("raw_text")) or ""
    raw_text_lower = raw_text.lower()
    availability_text_lower = (availability_text or "").lower()

    availability_status_override = None
    available_date_override = None
    availability_text_for_build = availability_text

    if any(
        phrase in raw_text_lower
        for phrase in [
            "for rent",
            "now renting",
            "units available",
            "unit available",
            "currently available",
            "available now",
        ]
    ):
        availability_status_override = "available"
        available_date_override = None
        availability_text_for_build = None

    elif any(
        phrase in raw_text_lower or phrase in availability_text_lower
        for phrase in [
            "leased",
            "not available",
            "no longer available",
            "rented",
            "application pending",
            "pending",
            "occupied",
        ]
    ):
        availability_status_override = "unavailable"
        available_date_override = None
        availability_text_for_build = None

    return [
        build_stg_record(
            row=row,
            title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text=availability_text_for_build,
            listing_url=listing_url,
            availability_status_override=availability_status_override,
            available_date_override=available_date_override,
        )
    ]


def is_probably_non_residential_plum(title, address_raw, availability_text, raw):
    text_parts = [
        clean_text(title),
        clean_text(address_raw),
        clean_text(availability_text),
        clean_text(raw.get("title")) if isinstance(raw, dict) else None,
        clean_text(raw.get("listing_title")) if isinstance(raw, dict) else None,
        clean_text(raw.get("unit_status")) if isinstance(raw, dict) else None,
        clean_text(raw.get("rent_text")) if isinstance(raw, dict) else None,
        clean_text(raw.get("deposit_text")) if isinstance(raw, dict) else None,
    ]

    combined = " | ".join([p for p in text_parts if p])
    lowered = combined.lower()

    blocked_phrases = [
        "storage",
        "office",
        "commercial",
        "retail",
        "warehouse",
        "suite",
    ]

    if any(phrase in lowered for phrase in blocked_phrases):
        return True

    has_rent = bool(raw.get("rent_text") or raw.get("rent"))
    has_beds = bool(raw.get("bedrooms_raw") or raw.get("bedrooms"))

    if not has_rent and not has_beds:
        return True

    return False


def parse_plum_row(row):
    raw = ensure_raw_json(row["raw_json"])
    if not isinstance(raw, dict):
        return []

    title = coalesce_text(raw.get("title"), raw.get("listing_title"))
    if looks_like_placeholder_title(title):
        title = None

    raw_text_address = extract_address_candidate(row.get("raw_text"))
    address_raw = coalesce_text(raw.get("address"), raw_text_address)

    if not title:
        title = address_raw

    bedrooms = normalize_bedrooms(coalesce_text(raw.get("bedrooms_raw"), raw.get("bedrooms")))
    bathrooms = normalize_bathrooms(coalesce_text(raw.get("bathrooms_raw"), raw.get("bathrooms")))
    sqft = normalize_sqft(
        coalesce_text(
            raw.get("sqft_raw"),
            raw.get("square_footage"),
            raw.get("square_feet"),
            raw.get("sqft"),
        )
    )

    rent_min, rent_max, rent_period = normalize_rent(coalesce_text(raw.get("rent_text"), raw.get("rent")))
    availability_text = coalesce_text(
        raw.get("unit_status"),
        raw.get("available_text"),
        raw.get("availability_text"),
    )

    if is_probably_non_residential_plum(title, address_raw, availability_text, raw):
        return []

    listing_url = clean_text(raw.get("listing_url")) or row["source_url"]

    return [
        build_stg_record(
            row=row,
            title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text=availability_text,
            listing_url=listing_url,
            source_record_id=clean_text(raw.get("source_record_id")) or clean_text(row["source_record_id"]),
        )
    ]


def parse_mha_row(row):
    raw = ensure_raw_json(row["raw_json"])
    if not isinstance(raw, dict):
        return []

    property_name = clean_text(raw.get("property_name"))
    address_raw = clean_text(raw.get("address"))
    vacancies_text = clean_text(raw.get("vacancies_text"))
    updated_date = clean_text(raw.get("updated_date"))
    listing_url = clean_text(raw.get("source_pdf_url")) or row["source_url"]

    if LATEST_MHA_UPDATED_DATE and updated_date != LATEST_MHA_UPDATED_DATE:
        return []

    if not vacancies_text:
        return []

    bedroom_list = extract_mha_bedroom_list(vacancies_text)
    if not bedroom_list:
        return []

    records = []
    for bedrooms in bedroom_list:
        bedroom_token = "na" if bedrooms is None else str(bedrooms)

        source_record_id = make_mha_source_record_id(
            property_name,
            address_raw,
            updated_date,
            f"{vacancies_text}|beds={bedroom_token}",
        )

        records.append(
            build_stg_record(
                row=row,
                title=property_name,
                address_raw=address_raw,
                bedrooms=bedrooms,
                bathrooms=None,
                sqft=None,
                rent_min=None,
                rent_max=None,
                rent_period="month",
                availability_text=vacancies_text,
                listing_url=listing_url,
                source_record_id=source_record_id,
                contact_name=raw.get("manager_name"),
                contact_phone=raw.get("manager_phone"),
                contact_email=raw.get("manager_email"),
                property_name=property_name,
                availability_status_override="available",
                available_date_override=row["scraped_at"].date(),
            )
        )

    return records


def is_probably_bad_craigslist_listing(title, address_raw, raw):
    text_parts = [
        clean_text(title),
        clean_text(address_raw),
        clean_text(raw.get("title")) if isinstance(raw, dict) else None,
        clean_text(raw.get("location_text")) if isinstance(raw, dict) else None,
    ]

    combined = " | ".join([p for p in text_parts if p]).lower()

    blocked_phrases = [
        "wanted",
        "lease pending",
        "pending",
        "no longer available",
        "rv hookup",
        "rv hookups",
        "rv site",
        "parking",
        "storage",
        "communal living",
        "room share",
        "roommate",
        "extended stay",
        "section 8",
        "low income housing wanted",
        "hotel",
    ]

    return any(phrase in combined for phrase in blocked_phrases)


def craigslist_is_recently_available(raw, observed_at, max_age_days=5):
    if not isinstance(raw, dict):
        return False

    posted_at = clean_text(raw.get("posted_at"))
    if not posted_at:
        return False

    try:
        posted_dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
    except ValueError:
        return False

    if posted_dt.tzinfo is None:
        posted_dt = posted_dt.replace(tzinfo=timezone.utc)

    if isinstance(observed_at, datetime):
        obs_dt = observed_at
        if obs_dt.tzinfo is None:
            obs_dt = obs_dt.replace(tzinfo=timezone.utc)
    else:
        obs_dt = datetime.now(timezone.utc)

    age_days = (obs_dt - posted_dt).days
    return 0 <= age_days <= max_age_days


def parse_craigslist_record_array(record):
    title = None
    rent_value = None
    bedrooms = None
    sqft = None

    if isinstance(record, list):
        if len(record) > 10:
            title = clean_text(record[10])

        if len(record) > 3:
            rent_value = record[3]

        extras = record[11] if len(record) > 11 and isinstance(record[11], list) else []
        if len(extras) > 1:
            bedrooms = normalize_bedrooms(extras[1])
        if len(extras) > 2:
            sqft_value = normalize_sqft(extras[2])
            if sqft_value and sqft_value > 0:
                sqft = sqft_value

    return title, rent_value, bedrooms, sqft


def parse_craigslist_row(row):
    raw = ensure_raw_json(row["raw_json"])
    record = raw

    title = None
    address_raw = None
    bedrooms = None
    bathrooms = None
    sqft = None
    listing_url = None
    source_record_id = clean_text(row["source_record_id"])
    rent_value = None

    if isinstance(raw, dict):
        record = raw.get("record", raw)
        title = coalesce_text(raw.get("title"), raw.get("listing_title"))
        address_raw = coalesce_text(raw.get("location_text"), raw.get("address"))
        bedrooms = normalize_bedrooms(raw.get("bedrooms"))
        bathrooms = normalize_bathrooms(raw.get("bathrooms"))
        sqft = normalize_sqft(raw.get("sqft"))
        listing_url = normalize_listing_url(raw.get("listing_url"))
        source_record_id = coalesce_text(raw.get("posting_id"), source_record_id)
        rent_value = raw.get("price")
        if rent_value is None:
            rent_value = raw.get("price_text")

    if title is None or rent_value is None:
        fallback_title, fallback_rent, fallback_bedrooms, fallback_sqft = parse_craigslist_record_array(record)
        title = title or fallback_title
        rent_value = rent_value if rent_value is not None else fallback_rent
        bedrooms = bedrooms if bedrooms is not None else fallback_bedrooms
        sqft = sqft if sqft is not None else fallback_sqft

    bathrooms = bathrooms if bathrooms is not None else parse_float_from_text(
        title,
        r"(\d+(?:\.\d+)?)\s*(?:bath|bathroom|ba)\b",
    )

    rent_min, rent_max, rent_period = normalize_rent(rent_value)

    if not listing_url and row["source_url"] and "craigslist.org/" in str(row["source_url"]):
        listing_url = row["source_url"]

    if not title or rent_min is None or not listing_url:
        return []

    if is_probably_bad_craigslist_listing(title, address_raw, raw):
        return []

    availability_status_override = None
    if craigslist_is_recently_available(raw, row["scraped_at"], max_age_days=5):
        availability_status_override = "available"

    if availability_status_override != "available":
        return []

    lat, lon = extract_lat_lon_from_record(record)

    return [
        build_stg_record(
            row=row,
            title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text="Recent Craigslist posting",
            listing_url=listing_url,
            source_record_id=source_record_id,
            availability_status_override=availability_status_override,
            latitude=lat,
            longitude=lon,
        )
    ]


def parse_row(row):
    source = row["source"]

    if source == "mpm":
        return parse_mpm_row(row)

    if source == "adea":
        return parse_adea_row(row)

    if source == "caras":
        return parse_caras_row(row)

    if source == "plum":
        return parse_plum_row(row)

    if source == "mha":
        return parse_mha_row(row)

    if source == "craigslist":
        return parse_craigslist_row(row)

    return []


def upsert_stg_listing(conn, record):
    sql = text(
        """
        INSERT INTO stg_listings (
            source,
            source_record_id,
            listing_title,
            address,
            bedrooms,
            bathrooms,
            sqft,
            rent_min,
            rent_max,
            rent_period,
            availability_status,
            available_date,
            availability_text_raw,
            contact_name,
            contact_phone,
            contact_email,
            listing_url,
            observed_at,
            listing_fingerprint,
            is_currently_available,
            is_available_soon,
            address_raw,
            cross_source_fingerprint,
            latitude,
            longitude
        )
        VALUES (
            :source,
            :source_record_id,
            :listing_title,
            :address,
            :bedrooms,
            :bathrooms,
            :sqft,
            :rent_min,
            :rent_max,
            :rent_period,
            :availability_status,
            :available_date,
            :availability_text_raw,
            :contact_name,
            :contact_phone,
            :contact_email,
            :listing_url,
            :observed_at,
            :listing_fingerprint,
            :is_currently_available,
            :is_available_soon,
            :address_raw,
            :cross_source_fingerprint,
            :latitude,
            :longitude
        )
        ON CONFLICT (source, source_record_id)
        DO UPDATE SET
            listing_title = EXCLUDED.listing_title,
            address = EXCLUDED.address,
            bedrooms = EXCLUDED.bedrooms,
            bathrooms = EXCLUDED.bathrooms,
            sqft = EXCLUDED.sqft,
            rent_min = EXCLUDED.rent_min,
            rent_max = EXCLUDED.rent_max,
            rent_period = EXCLUDED.rent_period,
            availability_status = EXCLUDED.availability_status,
            available_date = EXCLUDED.available_date,
            availability_text_raw = EXCLUDED.availability_text_raw,
            contact_name = EXCLUDED.contact_name,
            contact_phone = EXCLUDED.contact_phone,
            contact_email = EXCLUDED.contact_email,
            listing_url = EXCLUDED.listing_url,
            observed_at = EXCLUDED.observed_at,
            listing_fingerprint = EXCLUDED.listing_fingerprint,
            is_currently_available = EXCLUDED.is_currently_available,
            is_available_soon = EXCLUDED.is_available_soon,
            address_raw = EXCLUDED.address_raw,
            cross_source_fingerprint = EXCLUDED.cross_source_fingerprint,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        """
    )

    conn.execute(sql, record)


def main():
    source_filter = sys.argv[1].lower() if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 1000

    rows = fetch_recent_raw_rows(limit=limit, source=source_filter)

    prepared_records = {}
    raw_rows_supported = 0
    source_counts = {}
    duplicate_records_skipped = 0

    for row in rows:
        parsed_records = parse_row(row)
        if not parsed_records:
            continue

        raw_rows_supported += 1

        for record in parsed_records:
            key = (record["source"], record["source_record_id"])
            existing = prepared_records.get(key)

            if existing:
                if record["observed_at"] <= existing["observed_at"]:
                    duplicate_records_skipped += 1
                    continue

            prepared_records[key] = record
            source_counts[record["source"]] = source_counts.get(record["source"], 0) + 1

    parsed_rows = list(prepared_records.values())

    print(f"Found {len(rows)} raw rows")
    print(f"Supported raw rows: {raw_rows_supported}")
    print(f"Unique staged rows prepared: {len(parsed_rows)}")
    print(f"Duplicate staged rows skipped: {duplicate_records_skipped}")
    print(f"Source counts: {source_counts}")

    if source_filter:
        print(f"Source filter: {source_filter}")

    if not parsed_rows:
        print("No supported rows found.")
        return

    with engine.begin() as conn:
        if source_filter:
            conn.execute(
                text("DELETE FROM stg_listings WHERE source = :source"),
                {"source": source_filter},
            )
            print(f"Deleted existing staged rows for source: {source_filter}")

        for record in parsed_rows:
            upsert_stg_listing(conn, record)

    print(f"Upserted {len(parsed_rows)} rows into stg_listings.")


if __name__ == "__main__":
    main()
