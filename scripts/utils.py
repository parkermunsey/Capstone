from __future__ import annotations

import hashlib
import re
from typing import Optional

from datetime import datetime, date

def clean_text(value: object) -> Optional[str]:
    """
    Convert a value to cleaned text.
    Returns None for blank values.
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    # Collapse repeated whitespace
    text = re.sub(r"\s+", " ", text)
    return text


def parse_int(value: object) -> Optional[int]:
    """
    Parse an integer from a messy text value like '650 sqft' or '1,250'.
    """
    text = clean_text(value)
    if not text:
        return None

    text = text.replace(",", "")
    match = re.search(r"\d+", text)
    if not match:
        return None

    try:
        return int(match.group())
    except ValueError:
        return None


def parse_float(value: object) -> Optional[float]:
    """
    Parse a float from a messy text value like '1.5 baths'.
    """
    text = clean_text(value)
    if not text:
        return None

    text = text.replace(",", "")
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None

    try:
        return float(match.group())
    except ValueError:
        return None


def normalize_bedrooms(value: object) -> Optional[int]:
    """
    Normalize bedroom count.
    Studio becomes 0.

    Handles text like:
    - 'Studio'
    - '1 Bedrooms'
    - '2 Bed'
    - 'Available approximately 6/4/2026 Bedrooms 1 Bathrooms 350 Square Feet'
    """
    text = clean_text(value)
    if not text:
        return None

    lowered = text.lower()

    if "studio" in lowered:
        return 0

    # Best case: number appears before bedroom/bed
    match = re.search(r"(\d+)\s*(bedroom|bedrooms|bed)\b", lowered)
    if match:
        return int(match.group(1))

    # Also handle cases where the word comes before the number
    match = re.search(r"\b(bedroom|bedrooms|bed)\s*(\d+)", lowered)
    if match:
        return int(match.group(2))

    # Fallback only if the whole value looks like just a number
    if re.fullmatch(r"\d+", lowered):
        return int(lowered)

    return None


def normalize_bathrooms(value: object) -> Optional[float]:
    """
    Normalize bathroom count.
    """
    return parse_float(value)


def normalize_sqft(value: object) -> Optional[int]:
    """
    Normalize square footage.
    """
    return parse_int(value)


def normalize_rent(value: object) -> tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Normalize rent into (rent_min, rent_max, rent_period).

    Handles:
    - numeric values like 1080 or 1080.0
    - strings like '$1200/month'
    - ranges like '$1200 - $1400'
    - text like '1200 to 1400'
    """
    if value is None:
        return None, None, "month"

    # Handle true numeric values first so 1080.0 does not become [1080, 0]
    if isinstance(value, (int, float)):
        rent_value = int(float(value))
        return rent_value, rent_value, "month"

    text = clean_text(value)
    if not text:
        return None, None, "month"

    cleaned = text.lower().replace(",", "")

    # Pull decimal-aware numbers, then convert to ints
    raw_numbers = re.findall(r"\d+(?:\.\d+)?", cleaned)
    if not raw_numbers:
        return None, None, "month"

    values = [int(float(n)) for n in raw_numbers]

    if len(values) == 1:
        return values[0], values[0], "month"

    return min(values), max(values), "month"


def normalize_availability(value: object) -> str:
    """
    Map raw availability text into one of:
    available, waitlist, unavailable, unknown
    """
    text = clean_text(value)
    if not text:
        return "unknown"

    lowered = text.lower()

    if "waitlist" in lowered:
        return "waitlist"

    if any(word in lowered for word in ["available", "available now", "now available", "vacant", "immediate"]):
        return "available"

    if any(word in lowered for word in ["unavailable", "not available", "occupied", "leased", "rented", "none"]):
        return "unavailable"

    return "unknown"


def is_currently_available(availability_status: str) -> bool:
    """
    Convert normalized availability status to dashboard-ready boolean.
    """
    return availability_status == "available"


def make_listing_fingerprint(*parts: object) -> str:
    """
    Stable fingerprint for source-level record comparison.
    """
    joined = " | ".join((clean_text(part) or "") for part in parts)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def normalize_for_fingerprint(value: object) -> str:
    """
    Clean text more aggressively for cross-source matching.
    """
    text = clean_text(value) or ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def make_cross_source_fingerprint(
    address_raw: object,
    listing_title: object,
    bedrooms: object,
    rent_min: object,
    sqft: object,
) -> str:
    """
    URL-free fingerprint meant for future duplicate detection across sources.
    Prefer address if available, otherwise fall back to title.
    """
    base_location = normalize_for_fingerprint(address_raw) or normalize_for_fingerprint(listing_title)

    joined = " | ".join(
        [
            base_location,
            str(bedrooms or ""),
            str(rent_min or ""),
            str(sqft or ""),
        ]
    )

    return hashlib.md5(joined.encode("utf-8")).hexdigest()

def parse_available_date(value: object) -> Optional[date]:
    """
    Extract a date from text like:
    - 'Available approximately 03/17/26!!'
    - 'Available Approximately 4/10/2026!'
    - 'Available 6/20/2024!!'
    - 'Nov 7, 2025'
    - 'November 7, 2025'
    Returns a Python date or None.
    """
    text = clean_text(value)
    if not text:
        return None

    match = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", text)
    if match:
        raw_date = match.group(1)
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(raw_date, fmt).date()
            except ValueError:
                continue

    match = re.search(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b", text)
    if match:
        raw_date = match.group(1)
        for fmt in ("%b %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(raw_date, fmt).date()
            except ValueError:
                continue

    return None