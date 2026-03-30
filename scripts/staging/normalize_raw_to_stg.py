"""
normalize_raw_to_stg.py

Unified normalizer for Missoula rental listings pipeline.

- Reads raw_listings (append-only ingestion) and upserts into stg_listings.
- Canonical schema includes normalized rent/beds/availability and fingerprints.
- Idempotent upsert: PRIMARY KEY / UNIQUE(source, source_record_id) on stg_listings.
- Craigslist is marked research_only by default (see compliance notes).

Usage:
  DATABASE_URL="postgresql+psycopg2://..." python normalize_raw_to_stg.py --source all --lookback-days 14

Env:
  DATABASE_URL                   required
  NORMALIZER_PARSER_VERSION      optional (default: GITHUB_SHA or 'v1')
  ALLOW_CRAIGSLIST_PRODUCTION    optional (default 0; set 1 to set research_only=False for craigslist)
"""
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()
import argparse
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    create_engine,
    inspect,
    text as sql_text,
)
from sqlalchemy.dialects.postgresql import ARRAY, insert as pg_insert


# Optional deps (keep minimal)
try:
    from dateutil import parser as dateutil_parser  # type: ignore
except Exception:  # pragma: no cover
    dateutil_parser = None


LOG = logging.getLogger("normalize_raw_to_stg")

# Availability enum-ish strings
AVAIL_AVAILABLE = "available"
AVAIL_WAITLIST = "waitlist"
AVAIL_UNAVAILABLE = "unavailable"
AVAIL_UNKNOWN = "unknown"

RENT_PERIOD_MONTH = "month"

SUPPORTED_SOURCES = {"adea", "caras", "plum", "mpm", "rentinmissoula", "mha", "craigslist"}


@dataclass(frozen=True)
class RawRow:
    id: int
    source: str
    raw_source_record_id: Optional[str]
    source_url: Optional[str]
    observed_at: datetime
    raw_json: Any


@dataclass(frozen=True)
class Staged:
    # identity
    source: str
    source_record_id: str
    raw_listing_id: Optional[int]
    raw_source_record_id: Optional[str]

    # timestamps/urls
    observed_at: datetime
    posted_at: Optional[datetime]
    listing_url: Optional[str]

    # listing attributes
    listing_title: Optional[str]
    address_raw: Optional[str]
    address_norm: Optional[str]
    unit_raw: Optional[str]
    unit_norm: Optional[str]

    bedrooms: Optional[int]
    bathrooms: Optional[float]
    sqft: Optional[int]

    rent_min: Optional[int]
    rent_max: Optional[int]
    rent_period: str

    availability_status: str
    available_date: Optional[date]
    availability_text_raw: Optional[str]
    is_currently_available: bool

    # fingerprints
    listing_fingerprint: str
    cross_source_fingerprint: str

    # governance/debug
    research_only: bool
    parse_warnings: List[str]
    parse_confidence: int
    parser_version: str


# ---------- generic helpers ----------
_WS = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9\s]+", flags=re.IGNORECASE)


def clean_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return _WS.sub(" ", s)


def ensure_json(x: Any) -> Any:
    """Convert jsonb-ish payloads into Python objects when drivers return strings."""
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            return json.loads(x.decode("utf-8"))
        except Exception:
            return x
    if isinstance(x, str):
        xs = x.strip()
        if xs and xs[0] in "{[":
            try:
                return json.loads(xs)
            except Exception:
                return x
    return x


def coerce_int(x: Any) -> Optional[int]:
    s = clean_text(x)
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+", s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def coerce_float(x: Any) -> Optional[float]:
    s = clean_text(x)
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


# ---------- bedrooms ----------
_BED_RE = re.compile(r"\b(\d+)\s*(?:bedroom|bedrooms|beds|bed|br|bd)\b", re.IGNORECASE)
_STUDIO_RE = re.compile(r"\bstudio\b", re.IGNORECASE)


def parse_bedrooms(*candidates: Any) -> Optional[int]:
    for v in candidates:
        if v is None:
            continue
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            b = int(float(v))
            if 0 <= b <= 10:
                return b
        s = clean_text(v)
        if not s:
            continue
        if _STUDIO_RE.search(s):
            return 0
        m = _BED_RE.search(s)
        if m:
            b = int(m.group(1))
            if 0 <= b <= 10:
                return b
        if re.fullmatch(r"\d+", s):
            b = int(s)
            if 0 <= b <= 10:
                return b
    return None


# ---------- rent ----------
_MONEY_RE = re.compile(r"\$?\s*([0-9]{3,6})(?:\.\d{1,2})?", re.IGNORECASE)


def parse_rent(x: Any) -> Tuple[Optional[int], Optional[int], str, List[str]]:
    warnings: List[str] = []
    if x is None:
        return None, None, RENT_PERIOD_MONTH, warnings
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        n = int(float(x))
        if n > 0:
            return n, n, RENT_PERIOD_MONTH, warnings
        warnings.append("rent_numeric_nonpositive")
        return None, None, RENT_PERIOD_MONTH, warnings

    s = clean_text(x)
    if not s:
        return None, None, RENT_PERIOD_MONTH, warnings

    sl = s.lower()
    if any(k in sl for k in ["call", "tbd", "contact", "ask", "varies", "variable"]):
        warnings.append("rent_text_nonneumeric")
        return None, None, RENT_PERIOD_MONTH, warnings

    nums = [int(m.group(1)) for m in _MONEY_RE.finditer(sl)]
    nums = [n for n in nums if 200 <= n <= 20000]
    if not nums:
        warnings.append("rent_parse_failed")
        return None, None, RENT_PERIOD_MONTH, warnings
    if len(nums) == 1:
        return nums[0], nums[0], RENT_PERIOD_MONTH, warnings
    return min(nums), max(nums), RENT_PERIOD_MONTH, warnings


# ---------- availability ----------
_MMDDYYYY = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
_MONTHNAME = re.compile(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b")


def parse_available_date(x: Any) -> Optional[date]:
    """
    Conservative date parsing: only parse explicit date-like substrings.
    Avoid broad fuzzy parsing on arbitrary text.
    """
    s = clean_text(x)
    if not s:
        return None
    m = _MMDDYYYY.search(s)
    if m:
        raw = m.group(1)
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
    m = _MONTHNAME.search(s)
    if m:
        raw = m.group(1)
        for fmt in ("%b %d, %Y", "%B %d, %Y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        if dateutil_parser is not None:
            try:
                return dateutil_parser.parse(raw, fuzzy=False).date()
            except Exception:
                return None
    return None


def parse_availability_status(x: Any) -> str:
    s = clean_text(x)
    if not s:
        return AVAIL_UNKNOWN
    sl = s.lower()
    if "waitlist" in sl or re.search(r"\bwait\b", sl):
        return AVAIL_WAITLIST
    if any(k in sl for k in ["unavailable", "not available", "occupied", "leased", "rented", "no vacancies", "none"]):
        return AVAIL_UNAVAILABLE
    if any(k in sl for k in ["available", "available now", "now available", "vacant", "immediate"]):
        return AVAIL_AVAILABLE
    return AVAIL_UNKNOWN


def derive_is_currently_available(status: str, avail_date: Optional[date], observed_at: datetime) -> bool:
    if status != AVAIL_AVAILABLE:
        return False
    if avail_date is None:
        return True
    return avail_date <= observed_at.date()


# ---------- address normalization + unit ----------
_UNIT_RE = re.compile(r"(?:\bapt\b|\bapartment\b|\bunit\b|\bsuite\b|#)\s*([A-Za-z0-9\-]+)", re.IGNORECASE)

_STREET_MAP = {
    "street": "st", "st.": "st",
    "avenue": "ave", "ave.": "ave",
    "road": "rd", "rd.": "rd",
    "drive": "dr", "dr.": "dr",
    "boulevard": "blvd", "blvd.": "blvd",
    "lane": "ln", "ln.": "ln",
    "court": "ct", "ct.": "ct",
    "place": "pl", "pl.": "pl",
    "circle": "cir", "cir.": "cir",
}


def extract_unit(text_val: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    s = clean_text(text_val)
    if not s:
        return None, None
    m = _UNIT_RE.search(s)
    if not m:
        return None, None
    raw = m.group(1).strip()
    norm = re.sub(r"[^A-Za-z0-9\-]", "", raw).upper()
    return raw or None, norm or None


def normalize_address(address: Optional[str]) -> Optional[str]:
    s = clean_text(address)
    if not s:
        return None
    s = s.lower()
    s = re.sub(r"\bmissoula\b", "", s)
    s = re.sub(r"\bmontana\b", "", s)
    s = re.sub(r"\bmt\b", "", s)
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    tokens = [_STREET_MAP.get(t, t) for t in s.split(" ") if t]
    s = _WS.sub(" ", " ".join(tokens)).strip()
    return s or None


# ---------- fingerprints / confidence ----------
def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def cross_source_fp(address_norm: Optional[str], unit_norm: Optional[str], bedrooms: Optional[int], title: Optional[str]) -> str:
    base = address_norm or normalize_address(title) or (clean_text(title) or "")
    beds = "" if bedrooms is None else str(bedrooms)
    unit = unit_norm or ""
    return sha256_hex(f"{base}|{unit}|{beds}")


def observation_fp(
    address_norm: Optional[str],
    unit_norm: Optional[str],
    bedrooms: Optional[int],
    bathrooms: Optional[float],
    sqft: Optional[int],
    rent_min: Optional[int],
    rent_max: Optional[int],
    availability_status: str,
    available_date: Optional[date],
) -> str:
    parts = [
        address_norm or "",
        unit_norm or "",
        "" if bedrooms is None else str(bedrooms),
        "" if bathrooms is None else f"{bathrooms:.1f}",
        "" if sqft is None else str(sqft),
        "" if rent_min is None else str(rent_min),
        "" if rent_max is None else str(rent_max),
        availability_status or "",
        "" if available_date is None else available_date.isoformat(),
    ]
    return hashlib.md5("|".join(parts).encode("utf-8")).hexdigest()


def compute_confidence(title: Optional[str], address_raw: Optional[str], bedrooms: Optional[int], rent_min: Optional[int], warnings: Sequence[str]) -> int:
    score = 100
    if not title:
        score -= 10
    if not address_raw:
        score -= 25
    if bedrooms is None:
        score -= 15
    if rent_min is None:
        score -= 20
    score -= min(30, 5 * len(warnings))
    return max(0, min(100, score))


def build_staged(
    *,
    row: RawRow,
    source_record_id: str,
    listing_url: Optional[str],
    listing_title: Optional[str],
    address_raw: Optional[str],
    bedrooms: Optional[int],
    bathrooms: Optional[float],
    sqft: Optional[int],
    rent_min: Optional[int],
    rent_max: Optional[int],
    rent_period: str,
    availability_text_raw: Optional[str],
    availability_status: str,
    available_date: Optional[date],
    posted_at: Optional[datetime],
    research_only: bool,
    parser_version: str,
    warnings: List[str],
) -> Staged:
    unit_raw, unit_norm = extract_unit(address_raw or listing_title)
    address_norm = normalize_address(address_raw or listing_title)

    csfp = cross_source_fp(address_norm, unit_norm, bedrooms, listing_title)
    lfp = observation_fp(address_norm, unit_norm, bedrooms, bathrooms, sqft, rent_min, rent_max, availability_status, available_date)
    is_current = derive_is_currently_available(availability_status, available_date, row.observed_at)
    conf = compute_confidence(listing_title, address_raw, bedrooms, rent_min, warnings)

    return Staged(
        source=row.source,
        source_record_id=source_record_id,
        raw_listing_id=row.id,
        raw_source_record_id=row.raw_source_record_id,
        observed_at=row.observed_at,
        posted_at=posted_at,
        listing_url=listing_url,
        listing_title=listing_title,
        address_raw=address_raw,
        address_norm=address_norm,
        unit_raw=unit_raw,
        unit_norm=unit_norm,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        sqft=sqft,
        rent_min=rent_min,
        rent_max=rent_max,
        rent_period=rent_period,
        availability_status=availability_status,
        available_date=available_date,
        availability_text_raw=availability_text_raw,
        is_currently_available=is_current,
        listing_fingerprint=lfp,
        cross_source_fingerprint=csfp,
        research_only=research_only,
        parse_warnings=warnings,
        parse_confidence=conf,
        parser_version=parser_version,
    )


# ---------- per-source parsers ----------
def parse_adea(row: RawRow, parser_version: str) -> List[Staged]:
    raw = ensure_json(row.raw_json) or {}
    if not isinstance(raw, dict):
        return []
    warnings: List[str] = []

    title = clean_text(raw.get("title") or raw.get("listing_title"))

    addr_val = raw.get("address")
    address_raw: Optional[str] = None
    if isinstance(addr_val, dict):
        address_raw = clean_text(addr_val.get("address") or addr_val.get("street") or addr_val.get("line1"))
        if not address_raw:
            pieces = [addr_val.get(k) for k in ["line1", "line2", "city", "state", "postal_code"]]
            pieces = [clean_text(p) for p in pieces if clean_text(p)]
            if pieces:
                address_raw = ", ".join(pieces)
                warnings.append("adea_address_joined_from_components")
    else:
        address_raw = clean_text(addr_val)

    bedrooms = parse_bedrooms(raw.get("bedrooms"))
    bathrooms = coerce_float(raw.get("bathrooms"))
    sqft = coerce_int(raw.get("square_feet") or raw.get("sqft"))

    rent_min, rent_max, rent_period, rent_w = parse_rent(raw.get("rent"))
    warnings.extend([f"rent:{w}" for w in rent_w])

    availability_text = clean_text(raw.get("available_date") or raw.get("availability_text") or raw.get("available_text"))
    available_date = parse_available_date(availability_text)
    status = parse_availability_status(availability_text)
    if status == AVAIL_UNKNOWN and available_date is not None:
        status = AVAIL_AVAILABLE
        warnings.append("availability:status_inferred_from_date")

    # Use ingester raw source_record_id (page_item_url) to reconstruct detail URL
    page_item = clean_text(row.raw_source_record_id)
    if page_item:
        source_record_id = page_item
        listing_url = f"https://www.adeapm.com/listings/detail/{page_item}"
    else:
        source_record_id = sha256_hex((row.source_url or (title or "")))[:32]
        listing_url = row.source_url
        warnings.append("adea_missing_page_item_url")

    return [
        build_staged(
            row=row,
            source_record_id=source_record_id,
            listing_url=listing_url,
            listing_title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text_raw=availability_text,
            availability_status=status,
            available_date=available_date,
            posted_at=None,
            research_only=False,
            parser_version=parser_version,
            warnings=warnings,
        )
    ]


def parse_caras(row: RawRow, parser_version: str) -> List[Staged]:
    raw = ensure_json(row.raw_json) or {}
    if not isinstance(raw, dict):
        return []
    warnings: List[str] = []

    title = clean_text(raw.get("title") or raw.get("address"))
    address_raw = clean_text(raw.get("address")) or title

    bedrooms = parse_bedrooms(raw.get("bedrooms"))
    bathrooms = coerce_float(raw.get("bathrooms"))
    sqft = coerce_int(raw.get("sqft"))

    rent_min, rent_max, rent_period, rent_w = parse_rent(raw.get("price_text"))
    warnings.extend([f"rent:{w}" for w in rent_w])

    availability_text = clean_text(raw.get("availability_text"))
    available_date = parse_available_date(availability_text)
    status = parse_availability_status(availability_text)

    listing_url = clean_text(raw.get("listing_url")) or row.source_url
    source_record_id = row.raw_source_record_id or sha256_hex(listing_url or (title or ""))[:32]

    return [
        build_staged(
            row=row,
            source_record_id=source_record_id,
            listing_url=listing_url,
            listing_title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text_raw=availability_text,
            availability_status=status,
            available_date=available_date,
            posted_at=None,
            research_only=False,
            parser_version=parser_version,
            warnings=warnings,
        )
    ]


def parse_plum(row: RawRow, parser_version: str) -> List[Staged]:
    raw = ensure_json(row.raw_json) or {}
    if not isinstance(raw, dict):
        return []
    warnings: List[str] = []

    title = clean_text(raw.get("title"))
    address_raw = title  # often address-like

    bedrooms = parse_bedrooms(raw.get("bedrooms_raw"), title)
    bathrooms = coerce_float(raw.get("bathrooms_raw"))
    sqft = coerce_int(raw.get("sqft_raw"))

    rent_min, rent_max, rent_period, rent_w = parse_rent(raw.get("rent_text"))
    warnings.extend([f"rent:{w}" for w in rent_w])

    availability_text = clean_text(raw.get("unit_status"))
    available_date = parse_available_date(availability_text)
    status = parse_availability_status(availability_text)

    listing_url = clean_text(raw.get("listing_url")) or row.source_url
    source_record_id = row.raw_source_record_id or sha256_hex(listing_url or (title or ""))[:32]

    return [
        build_staged(
            row=row,
            source_record_id=source_record_id,
            listing_url=listing_url,
            listing_title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text_raw=availability_text,
            availability_status=status,
            available_date=available_date,
            posted_at=None,
            research_only=False,
            parser_version=parser_version,
            warnings=warnings,
        )
    ]


def parse_mpm(row: RawRow, parser_version: str) -> List[Staged]:
    raw = ensure_json(row.raw_json) or {}
    if not isinstance(raw, dict):
        return []
    warnings: List[str] = []

    title = clean_text(raw.get("listing_title"))
    address_raw = title  # current scraper makes title address-like

    availability_text = clean_text(raw.get("available_text"))
    available_date = parse_available_date(availability_text)
    status = parse_availability_status(availability_text)

    bedrooms = parse_bedrooms(raw.get("beds"), availability_text, title)
    bathrooms = coerce_float(raw.get("baths"))
    sqft = coerce_int(raw.get("sqft"))

    rent_min, rent_max, rent_period, rent_w = parse_rent(raw.get("rent"))
    warnings.extend([f"rent:{w}" for w in rent_w])

    listing_url = clean_text(raw.get("details_url")) or row.source_url or clean_text(row.raw_source_record_id)
    source_record_id = sha256_hex(listing_url or (title or ""))[:32]

    return [
        build_staged(
            row=row,
            source_record_id=source_record_id,
            listing_url=listing_url,
            listing_title=title,
            address_raw=address_raw,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            sqft=sqft,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text_raw=availability_text,
            availability_status=status,
            available_date=available_date,
            posted_at=None,
            research_only=False,
            parser_version=parser_version,
            warnings=warnings,
        )
    ]


def extract_mha_beds(vacancies_text: Optional[str]) -> List[int]:
    if not vacancies_text:
        return []
    t = vacancies_text.lower()
    beds = set()
    if "studio" in t:
        beds.add(0)
    for m in re.finditer(r"\b([0-9])\s*bedrooms?\b", t, flags=re.IGNORECASE):
        beds.add(int(m.group(1)))
    for m in re.finditer(r"\b([0-9])\s*(?:br|bd|bed)\b", t, flags=re.IGNORECASE):
        beds.add(int(m.group(1)))
    return sorted(beds)


def parse_mha(row: RawRow, parser_version: str) -> List[Staged]:
    raw = ensure_json(row.raw_json) or {}
    if not isinstance(raw, dict):
        return []
    warnings: List[str] = []

    title = clean_text(raw.get("property_name"))
    address_raw = clean_text(raw.get("address"))
    vacancies_text = clean_text(raw.get("vacancies_text"))

    beds_list = extract_mha_beds(vacancies_text)
    if not beds_list:
        beds_list = [None]
        if vacancies_text:
            warnings.append("mha_vacancies_unparsed")

    status = AVAIL_AVAILABLE if vacancies_text else AVAIL_UNAVAILABLE

    listing_url = clean_text(raw.get("source_pdf_url")) or row.source_url

    # Stable per property+bed; do not include updated_date/vacancies_text so rows update cleanly weekly
    base_key = normalize_address(address_raw or title) or clean_text(title) or ""
    out: List[Staged] = []
    for b in beds_list:
        source_record_id = sha256_hex(f"{base_key}|{'' if b is None else b}")[:32]
        out.append(
            build_staged(
                row=row,
                source_record_id=source_record_id,
                listing_url=listing_url,
                listing_title=title,
                address_raw=address_raw,
                bedrooms=b,
                bathrooms=None,
                sqft=None,
                rent_min=None,
                rent_max=None,
                rent_period=RENT_PERIOD_MONTH,
                availability_text_raw=vacancies_text,
                availability_status=status,
                available_date=None,
                posted_at=None,
                research_only=False,
                parser_version=parser_version,
                warnings=warnings.copy(),
            )
        )
    return out


def parse_craigslist(row: RawRow, parser_version: str, allow_production: bool) -> List[Staged]:
    """
    Craigslist SAPI rows are stored either as:
      - legacy: raw_json == record list
      - recommended: raw_json == {"decode": {...}, "item": [...], "site": "missoula"}

    Posting timestamp decoding relies on:
      posted_at = datetime.fromtimestamp(decode["minPostedDate"] + item[1])

    WARNING: Craigslist ToU prohibits unlicensed automated access/scraping.
    Default: research_only=True unless ALLOW_CRAIGSLIST_PRODUCTION=1.
    """
    raw = ensure_json(row.raw_json)
    warnings: List[str] = []
    item: Any = None
    decode: Any = None
    site = "missoula"

    if isinstance(raw, dict):
        item = raw.get("item") or raw.get("record") or raw.get("data")
        decode = raw.get("decode")
        site = clean_text(raw.get("site")) or site
    else:
        item = raw

    if not isinstance(item, list):
        warnings.append("craigslist_item_not_list")
        return []

    post_id = item[0] if len(item) > 0 and isinstance(item[0], int) else None
    posted_offset = item[1] if len(item) > 1 and isinstance(item[1], int) else None
    price_val = item[3] if len(item) > 3 else None
    title = clean_text(item[10]) if len(item) > 10 else None

    if post_id is None:
        warnings.append("craigslist_missing_post_id_fallback_hash")
        source_record_id = hashlib.sha1(json.dumps(item, sort_keys=True).encode("utf-8")).hexdigest()[:32]
    else:
        source_record_id = str(post_id)

    if price_val not in (None, -1):
        rent_min, rent_max, rent_period, rent_w = parse_rent(price_val)
    else:
        rent_min, rent_max, rent_period, rent_w = parse_rent(title)
    warnings.extend([f"rent:{w}" for w in rent_w])

    bedrooms = parse_bedrooms(title)

    posted_at = None
    if isinstance(decode, dict) and isinstance(decode.get("minPostedDate"), (int, float)) and posted_offset is not None:
        try:
            posted_at = datetime.fromtimestamp(int(decode["minPostedDate"]) + int(posted_offset), tz=timezone.utc)
        except Exception:
            warnings.append("craigslist_posted_at_decode_failed")
    else:
        warnings.append("craigslist_missing_decode_minPostedDate")

    listing_url = f"https://{site}.craigslist.org/apa/d/{post_id}.html" if post_id is not None else row.source_url

    # availability unknown without detail-page scraping
    status = AVAIL_UNKNOWN

    return [
        build_staged(
            row=row,
            source_record_id=source_record_id,
            listing_url=listing_url,
            listing_title=title,
            address_raw=None,
            bedrooms=bedrooms,
            bathrooms=None,
            sqft=None,
            rent_min=rent_min,
            rent_max=rent_max,
            rent_period=rent_period,
            availability_text_raw=None,
            availability_status=status,
            available_date=None,
            posted_at=posted_at,
            research_only=(False if allow_production else True),
            parser_version=parser_version,
            warnings=warnings,
        )
    ]


PARSERS = {
    "adea": parse_adea,
    "caras": parse_caras,
    "plum": parse_plum,
    "mpm": parse_mpm,
    "rentinmissoula": parse_mpm,  # alias
    "mha": parse_mha,
}


# ---------- db utilities ----------
def stg_table(metadata: MetaData) -> Table:
    """Must match the DDL provided in this report."""
    return Table(
        "stg_listings",
        metadata,
        Column("source", Text, nullable=False),
        Column("source_record_id", Text, nullable=False),
        Column("raw_listing_id", Integer, nullable=True),
        Column("raw_source_record_id", Text, nullable=True),
        Column("observed_at", DateTime(timezone=True), nullable=False),
        Column("posted_at", DateTime(timezone=True), nullable=True),
        Column("listing_url", Text, nullable=True),
        Column("listing_title", Text, nullable=True),
        Column("address_raw", Text, nullable=True),
        Column("address_norm", Text, nullable=True),
        Column("unit_raw", Text, nullable=True),
        Column("unit_norm", Text, nullable=True),
        Column("bedrooms", Integer, nullable=True),
        Column("bathrooms", Numeric(3, 1), nullable=True),
        Column("sqft", Integer, nullable=True),
        Column("rent_min", Integer, nullable=True),
        Column("rent_max", Integer, nullable=True),
        Column("rent_period", Text, nullable=False),
        Column("availability_status", Text, nullable=False),
        Column("available_date", Date, nullable=True),
        Column("availability_text_raw", Text, nullable=True),
        Column("is_currently_available", Boolean, nullable=False),
        Column("listing_fingerprint", Text, nullable=False),
        Column("cross_source_fingerprint", Text, nullable=False),
        Column("research_only", Boolean, nullable=False),
        Column("parse_warnings", ARRAY(Text), nullable=False),
        Column("parse_confidence", Integer, nullable=False),
        Column("parser_version", Text, nullable=False),
        Column("normalized_at", DateTime(timezone=True), nullable=False),
    )


def detect_raw_timestamp_column(engine) -> str:
    """raw_listings may use scraped_at or observed_at; detect."""
    insp = inspect(engine)
    cols = {c["name"] for c in insp.get_columns("raw_listings", schema="public")}
    if "scraped_at" in cols:
        return "scraped_at"
    if "observed_at" in cols:
        return "observed_at"
    return "scraped_at"


def fetch_raw(engine, sources: Sequence[str], lookback_days: int, limit: Optional[int]) -> Iterable[RawRow]:
    ts_col = detect_raw_timestamp_column(engine)
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    sql = f"""
    SELECT
      id,
      source,
      source_record_id,
      source_url,
      {ts_col} AS observed_at,
      raw_json
    FROM raw_listings
    WHERE source = ANY(:sources)
      AND {ts_col} >= :cutoff
    ORDER BY {ts_col} ASC, id ASC
    """
    if limit:
        sql += "\nLIMIT :limit"

    params = {"sources": list(sources), "cutoff": cutoff, "limit": limit}

    with engine.connect() as conn:
        for r in conn.execute(sql_text(sql), params).mappings():
            yield RawRow(
                id=int(r["id"]),
                source=str(r["source"]).strip().lower(),
                raw_source_record_id=clean_text(r.get("source_record_id")),
                source_url=clean_text(r.get("source_url")),
                observed_at=r["observed_at"] if isinstance(r["observed_at"], datetime) else datetime.now(timezone.utc),
                raw_json=r.get("raw_json"),
            )
def dedupe_staged_rows(rows):
    seen = set()
    deduped = []

    for r in rows:
        key = (r.source, r.source_record_id)

        if key not in seen:
            seen.add(key)
            deduped.append(r)

    return deduped

def upsert_stg(engine, table: Table, staged_rows: Sequence[Staged], dry_run: bool) -> int:
    if not staged_rows:
        return 0

    now = datetime.now(timezone.utc)
    values: List[Dict[str, Any]] = []
    for s in staged_rows:
        values.append(
            {
                "source": s.source,
                "source_record_id": s.source_record_id,
                "raw_listing_id": s.raw_listing_id,
                "raw_source_record_id": s.raw_source_record_id,
                "observed_at": s.observed_at,
                "posted_at": s.posted_at,
                "listing_url": s.listing_url,
                "listing_title": s.listing_title,
                "address_raw": s.address_raw,
                "address_norm": s.address_norm,
                "unit_raw": s.unit_raw,
                "unit_norm": s.unit_norm,
                "bedrooms": s.bedrooms,
                "bathrooms": s.bathrooms,
                "sqft": s.sqft,
                "rent_min": s.rent_min,
                "rent_max": s.rent_max,
                "rent_period": s.rent_period,
                "availability_status": s.availability_status,
                "available_date": s.available_date,
                "availability_text_raw": s.availability_text_raw,
                "is_currently_available": s.is_currently_available,
                "listing_fingerprint": s.listing_fingerprint,
                "cross_source_fingerprint": s.cross_source_fingerprint,
                "research_only": s.research_only,
                "parse_warnings": s.parse_warnings,
                "parse_confidence": s.parse_confidence,
                "parser_version": s.parser_version,
                "normalized_at": now,
            }
        )

    ins = pg_insert(table).values(values)
    update_cols = {c.name: ins.excluded[c.name] for c in table.columns if c.name not in {"source", "source_record_id"}}
    stmt = ins.on_conflict_do_update(index_elements=["source", "source_record_id"], set_=update_cols)

    if dry_run:
        LOG.info("Dry run: would upsert %d rows", len(values))
        return len(values)

    with engine.begin() as conn:
        conn.execute(stmt)
    return len(values)


def normalize(raw_rows: Iterable[RawRow], parser_version: str, allow_craigslist_production: bool) -> Tuple[List[Staged], Dict[str, int], int]:
    out: List[Staged] = []
    counts: Dict[str, int] = {}
    errors = 0

    for row in raw_rows:
        if row.source not in SUPPORTED_SOURCES:
            continue
        try:
            if row.source == "craigslist":
                staged = parse_craigslist(row, parser_version=parser_version, allow_production=allow_craigslist_production)
            else:
                fn = PARSERS.get(row.source)
                if not fn:
                    continue
                staged = fn(row, parser_version=parser_version)
        except Exception as e:
            errors += 1
            LOG.exception("Parse error raw_id=%s source=%s: %s", row.id, row.source, e)
            continue

        for s in staged:
            out.append(s)
            counts[row.source] = counts.get(row.source, 0) + 1

    return out, counts, errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="all", help="Source name, comma-separated, or 'all'")
    ap.add_argument("--lookback-days", type=int, default=14)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL env var is required")

    parser_version = os.getenv("NORMALIZER_PARSER_VERSION") or os.getenv("GITHUB_SHA") or "v1"
    allow_craigslist_production = os.getenv("ALLOW_CRAIGSLIST_PRODUCTION", "0") == "1"

    engine = create_engine(db_url, pool_pre_ping=True)

    if args.source.lower() == "all":
        sources = sorted(SUPPORTED_SOURCES)
    else:
        sources = [s.strip().lower() for s in args.source.split(",") if s.strip()]
        unknown = [s for s in sources if s not in SUPPORTED_SOURCES]
        if unknown:
            raise ValueError(f"Unknown sources: {unknown}")

    insp = inspect(engine)
    if "stg_listings" not in insp.get_table_names(schema="public"):
        raise RuntimeError("stg_listings not found. Apply DDL first.")

    raw_rows = fetch_raw(engine, sources, args.lookback_days, args.limit)
    staged_rows, counts, errors = normalize(raw_rows, parser_version, allow_craigslist_production)

    LOG.info("Staged per source: %s", counts)
    LOG.info("Parse errors: %d", errors)
    LOG.info("Total staged rows: %d", len(staged_rows))

    if not staged_rows:
        LOG.warning("No rows staged; exiting")
        return

    md = MetaData()
    table = stg_table(md)
    staged_rows = dedupe_staged_rows(staged_rows)

    print(f"After dedupe: {len(staged_rows)} rows")

    written = upsert_stg(engine, table, staged_rows, args.dry_run)
    
    LOG.info("Upserted %d stg rows", written)


if __name__ == "__main__":
    main()
