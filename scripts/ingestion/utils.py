from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from html import unescape
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

PLACEHOLDER_TITLES = {
    "details",
    "details - rentplum.com",
    "details - rentplum",
    "rentplum.com",
}

ADDRESS_RE = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.\-/ ]+,\s*[A-Za-z .'-]+,\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?\b"
)
CITY_STATE_ZIP_RE = re.compile(
    r"^(?P<body>.*?)(?:,\s*(?P<city>[A-Za-z .'-]+))?(?:,\s*(?P<state>[A-Z]{2}))"
    r"(?:\s+(?P<postal>\d{5}(?:-\d{4})?))?$"
)
UNIT_RE = re.compile(
    r"(?:^|,|\s)(?:unit|apt|apartment|suite|ste|#|lot)\s*([A-Za-z0-9\-]+)\b",
    flags=re.IGNORECASE,
)


def clean_text(value: object) -> Optional[str]:
    """
    Convert a value to cleaned text.
    Returns None for blank or placeholder values.
    """
    if value is None:
        return None

    text = unescape(str(value)).replace("\xa0", " ").strip()
    if not text:
        return None

    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"none", "null", "n/a", "na", "nan", "-", "--"}:
        return None

    return text


def coalesce_text(*values: object) -> Optional[str]:
    """
    Return the first non-blank cleaned text value.
    """
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return None


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


def parse_float_from_text(value: object, pattern: str) -> Optional[float]:
    """
    Parse a float from text using a custom pattern.
    """
    text = clean_text(value)
    if not text:
        return None

    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None

    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None


def normalize_bedrooms(value: object) -> Optional[int]:
    """
    Normalize bedroom count.
    Studio becomes 0.
    """
    text = clean_text(value)
    if not text:
        return None

    lowered = text.lower()

    if "studio" in lowered:
        return 0

    match = re.search(r"(\d+)\s*(bedroom|bedrooms|bed|br)\b", lowered)
    if match:
        return int(match.group(1))

    match = re.search(r"\b(bedroom|bedrooms|bed|br)\s*(\d+)", lowered)
    if match:
        return int(match.group(2))

    if re.fullmatch(r"\d+", lowered):
        return int(lowered)

    return None


def normalize_bathrooms(value: object) -> Optional[float]:
    """
    Normalize bathroom count.
    """
    parsed = parse_float(value)
    if parsed is not None:
        return parsed

    return parse_float_from_text(value, r"(\d+(?:\.\d+)?)\s*(?:bath|bathroom|ba)\b")


def normalize_sqft(value: object) -> Optional[int]:
    """
    Normalize square footage.
    """
    return parse_int(value)


def normalize_rent(value: object) -> tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Normalize rent into (rent_min, rent_max, rent_period).
    """
    if value is None:
        return None, None, "month"

    if isinstance(value, (int, float)):
        rent_value = int(float(value))
        return rent_value, rent_value, "month"

    text = clean_text(value)
    if not text:
        return None, None, "month"

    cleaned = text.lower().replace(",", "")

    if any(token in cleaned for token in ["/week", "per week", "weekly"]):
        rent_period = "week"
    elif any(token in cleaned for token in ["/day", "per day", "daily"]):
        rent_period = "day"
    else:
        rent_period = "month"

    raw_numbers = re.findall(r"\d+(?:\.\d+)?", cleaned)
    if not raw_numbers:
        return None, None, rent_period

    values = [int(float(n)) for n in raw_numbers]

    if len(values) == 1:
        return values[0], values[0], rent_period

    return min(values), max(values), rent_period


def normalize_availability(value: object) -> str:
    """
    Map raw availability text into one of:
    available, waitlist, unavailable, unknown
    """
    text = clean_text(value)
    if not text:
        return "unknown"

    lowered = text.lower()

    if "waitlist" in lowered or "wait list" in lowered:
        return "waitlist"

    if any(
        phrase in lowered
        for phrase in [
            "unavailable",
            "not available",
            "no availability",
            "occupied",
            "leased",
            "rented",
            "none available",
            "currently full",
            "no vacancies",
        ]
    ):
        return "unavailable"

    if any(
        phrase in lowered
        for phrase in [
            "available",
            "available now",
            "now available",
            "vacant",
            "immediate",
            "coming soon",
            "open now",
        ]
    ):
        return "available"

    return "unknown"


def is_currently_available(
    availability_status: str,
    available_date: Optional[date] = None,
    observed_at: object = None,
) -> bool:
    """
    Convert normalized availability fields into a dashboard-ready boolean.

    Rules:
    - Must have normalized status = "available"
    - If no date is present, treat as available
    - If a date is present, it must be on or before the observed date
    - If a date is very old, treat it as stale rather than currently available
    """
    if availability_status != "available":
        return False

    if not available_date:
        return True

    observed_date = None

    if isinstance(observed_at, datetime):
        observed_date = observed_at.date()
    elif isinstance(observed_at, date):
        observed_date = observed_at
    else:
        text = clean_text(observed_at)
        if text:
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    observed_date = datetime.strptime(text, fmt).date()
                    break
                except ValueError:
                    continue

    if observed_date is None:
        observed_date = datetime.utcnow().date()

    if available_date > observed_date:
        return False

    stale_days = (observed_date - available_date).days
    if stale_days > 60:
        return False

    return True

def is_available_soon(
    availability_status: str,
    available_date: Optional[date] = None,
    observed_at: object = None,
    days_ahead: int = 60,
) -> bool:
    """
    True when a listing is explicitly available in the near future.
    Excludes listings that are already currently available.
    """
    if availability_status != "available":
        return False

    if not available_date:
        return False

    observed_date = None

    if isinstance(observed_at, datetime):
        observed_date = observed_at.date()
    elif isinstance(observed_at, date):
        observed_date = observed_at
    else:
        text = clean_text(observed_at)
        if text:
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    observed_date = datetime.strptime(text, fmt).date()
                    break
                except ValueError:
                    continue

    if observed_date is None:
        observed_date = datetime.utcnow().date()

    days_until = (available_date - observed_date).days
    return 0 < days_until <= days_ahead

def looks_like_placeholder_title(value: object) -> bool:
    """
    Detect obvious site boilerplate instead of real listing titles.
    """
    text = clean_text(value)
    if not text:
        return True

    lowered = text.lower()
    if lowered in PLACEHOLDER_TITLES:
        return True

    return lowered.startswith("details - ") or lowered.endswith(" - rentplum.com")


def looks_like_address(value: object) -> bool:
    """
    Heuristic check for address-like text.
    """
    text = clean_text(value)
    if not text or looks_like_placeholder_title(text):
        return False

    if ADDRESS_RE.search(text):
        return True

    if re.match(r"^\d{1,6}\s+\S+", text):
        return True

    return bool(re.search(r",\s*[A-Za-z .'-]+,\s*[A-Z]{2}(?:\s+\d{5}(?:-\d{4})?)?$", text))


def extract_address_candidate(value: object) -> Optional[str]:
    """
    Pull the best address-like string from free text.
    """
    if value is None:
        return None

    raw_text = str(value)

    match = ADDRESS_RE.search(raw_text)
    if match:
        return clean_text(match.group(0))

    for line in raw_text.splitlines():
        candidate = clean_text(line)
        if candidate and looks_like_address(candidate):
            return candidate

    text = clean_text(raw_text)
    if text and looks_like_address(text):
        return text

    return None


def normalize_listing_url(value: object) -> Optional[str]:
    """
    Canonicalize URLs for fingerprinting and staging.
    """
    text = clean_text(value)
    if not text:
        return None

    if text.startswith("//"):
        text = "https:" + text

    try:
        parts = urlsplit(text)
    except ValueError:
        return text

    scheme = (parts.scheme or "").lower()
    netloc = (parts.netloc or "").lower()
    path = re.sub(r"/{2,}", "/", parts.path or "")

    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    query_items = parse_qsl(parts.query, keep_blank_values=True)
    query = urlencode(query_items, doseq=True)

    if not scheme and not netloc:
        return text

    return urlunsplit((scheme, netloc, path, query, ""))


def extract_unit(value: object) -> Optional[str]:
    """
    Extract a unit number or letter from address-like text.
    Handles:
    - Unit #202
    - Apt 3
    - Suite E
    - #29
    """
    text = clean_text(value)
    if not text:
        return None

    patterns = [
        r"(?:^|,|\s)(?:unit|apt|apartment|suite|ste|lot)\b\s*#?\s*([A-Za-z0-9\-]+)\b",
        r"(?:^|,|\s)#\s*([A-Za-z0-9\-]+)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            unit = clean_text(match.group(1))
            return unit.upper() if unit else None

    return None


def strip_unit_from_address(value: object) -> Optional[str]:
    """
    Remove common unit labels from an address string.
    """
    text = clean_text(value)
    if not text:
        return None

    patterns = [
        r"(?:,\s*|\s+)(?:unit|apt|apartment|suite|ste|lot)\b\s*#?\s*[A-Za-z0-9\-]+(?=,|$)",
        r"(?:,\s*|\s+)#\s*[A-Za-z0-9\-]+(?=,|$)",
    ]

    stripped = text
    for pattern in patterns:
        stripped = re.sub(pattern, "", stripped, flags=re.IGNORECASE)

    stripped = re.sub(r"\s+,", ",", stripped)
    stripped = re.sub(r",\s*,", ", ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip(" ,")

    return stripped or None


def parse_address_components(value: object) -> dict[str, Optional[str]]:
    """
    Parse best-effort address components for fingerprinting and dashboard use.
    """
    original = clean_text(value)
    result = {
        "raw": original,
        "display": None,
        "street": None,
        "unit": None,
        "city": None,
        "state": None,
        "postal_code": None,
    }

    if not original or looks_like_placeholder_title(original):
        return result

    if not looks_like_address(original) and not re.match(
        r"^[A-Za-z .'-]+,\s*[A-Z]{2}(?:\s+\d{5}(?:-\d{4})?)?$",
        original,
    ):
        return result

    match = CITY_STATE_ZIP_RE.match(original)
    body = original

    if match:
        body = clean_text(match.group("body")) or original
        result["city"] = clean_text(match.group("city"))
        result["state"] = clean_text(match.group("state"))
        result["postal_code"] = clean_text(match.group("postal"))

    result["unit"] = extract_unit(body)
    result["street"] = strip_unit_from_address(body)

    # Location-only strings like "Missoula, MT" should land in city/state.
    if result["state"] and not result["city"] and body and not re.match(r"^\d", body):
        result["city"] = body
        result["street"] = None
        result["unit"] = None

    display_parts = []
    if result["street"]:
        display_parts.append(result["street"])
    if result["unit"]:
        display_parts.append(f"Unit {result['unit']}")
    location_parts = [result["city"], result["state"]]
    location = ", ".join(part for part in location_parts if part)
    if result["postal_code"]:
        location = f"{location} {result['postal_code']}".strip()
    if location:
        display_parts.append(location)

    result["display"] = ", ".join(display_parts) if display_parts else original
    return result


def normalize_for_fingerprint(value: object) -> str:
    """
    Clean text more aggressively for cross-source matching.
    """
    text = clean_text(value) or ""
    text = text.lower().replace("&", " and ")

    replacements = {
        r"\bstreet\b": "st",
        r"\bst\.\b": "st",
        r"\bavenue\b": "ave",
        r"\broad\b": "rd",
        r"\bdrive\b": "dr",
        r"\bboulevard\b": "blvd",
        r"\blane\b": "ln",
        r"\bcourt\b": "ct",
        r"\bplace\b": "pl",
        r"\bapartment\b": "apt",
        r"\bsuite\b": "ste",
    }

    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def make_address_key(address_raw: object) -> str:
    """
    Build a stable normalized address key for fingerprinting.
    """
    parts = parse_address_components(address_raw)

    location_parts = [
        normalize_for_fingerprint(parts.get("street")),
        normalize_for_fingerprint(parts.get("city")),
        normalize_for_fingerprint(parts.get("state")),
        normalize_for_fingerprint(parts.get("postal_code")),
    ]

    joined = " | ".join(part for part in location_parts if part)
    return joined or normalize_for_fingerprint(address_raw)


def make_listing_fingerprint(
    *parts: object,
    listing_url: object = None,
    source_record_id: object = None,
) -> str:
    """
    Stable fingerprint for source-level record comparison.
    """
    normalized_parts = []

    normalized_url = normalize_listing_url(listing_url)
    if normalized_url:
        normalized_parts.append(normalized_url)

    source_id = clean_text(source_record_id)
    if source_id:
        normalized_parts.append(source_id)

    normalized_parts.extend((clean_text(part) or str(part or "")) for part in parts)
    joined = " | ".join(normalized_parts)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def make_cross_source_fingerprint(
    address_raw: object,
    listing_title: object,
    bedrooms: object,
    rent_min: object,
    sqft: object,
    listing_url: object = None,
    unit: object = None,
    property_name: object = None,
) -> str:
    """
    URL-light fingerprint for duplicate detection across sources.
    """
    address_key = make_address_key(address_raw)
    unit_key = normalize_for_fingerprint(unit or extract_unit(address_raw))
    property_key = normalize_for_fingerprint(property_name or listing_title)
    fallback_url = normalize_for_fingerprint(normalize_listing_url(listing_url))

    base_location = address_key or property_key or fallback_url

    joined = " | ".join(
        [
            base_location,
            unit_key,
            str(bedrooms or ""),
            str(rent_min or ""),
            str(sqft or ""),
        ]
    )

    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def parse_available_date(value: object) -> Optional[date]:
    text = clean_text(value)
    if not text:
        return None

    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
    if match:
        raw_date = match.group(1)
        try:
            return datetime.strptime(raw_date, "%Y-%m-%d").date()
        except ValueError:
            pass

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