import os
import re
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

SOURCE = "caras"
START_URL = "https://caras-properties.com/missoula/residential/"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; Parker Munsey)",
    "Accept": "text/html,application/xhtml+xml",
}

MAX_PAGES = 25
MAX_LISTINGS = 500


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_url_for_id(url: str) -> str:
    if not url:
        return ""
    p = urlparse(url.strip())
    scheme = (p.scheme or "").lower()
    netloc = (p.netloc or "").lower()
    path = p.path or ""
    query = p.query or ""
    return urlunparse((scheme, netloc, path, "", query, ""))


def source_record_id_from_url(url: str) -> str:
    norm = normalize_url_for_id(url)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:32]


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    return r.text


def get_raw_json_storage_type(conn) -> str:
    q = """
    SELECT data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'raw_listings'
      AND column_name = 'raw_json'
    """
    row = conn.execute(text(q)).mappings().first()
    if not row:
        return "unknown"
    if row.get("udt_name") == "jsonb":
        return "jsonb"
    return row.get("data_type") or "unknown"


def build_raw_insert_sql(raw_json_type: str) -> str:
    """
    SQLAlchemy text() uses :param placeholders.
    For jsonb, cast with CAST(:raw_json AS jsonb) (NOT :raw_json::jsonb).
    """
    raw_json_expr = "CAST(:raw_json AS jsonb)" if raw_json_type == "jsonb" else ":raw_json"

    return f"""
    INSERT INTO raw_listings (
        source,
        source_url,
        source_record_id,
        scraped_at,
        raw_text,
        raw_json
    )
    VALUES (
        :source,
        :source_url,
        :source_record_id,
        :scraped_at,
        :raw_text,
        {raw_json_expr}
    );
    """


RAW_SELECT_LATEST_BATCH_SQL = """
SELECT
    source_record_id,
    source_url,
    scraped_at,
    raw_text,
    raw_json
FROM raw_listings
WHERE source = :source
  AND scraped_at = (
      SELECT max(scraped_at) FROM raw_listings WHERE source = :source
  );
"""

STG_UPSERT_SQL = """
INSERT INTO stg_listings (
    source,
    source_record_id,
    listing_title,
    bedrooms,
    bathrooms,
    sqft,
    rent_min,
    rent_max,
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
    :source,
    :source_record_id,
    :listing_title,
    :bedrooms,
    :bathrooms,
    :sqft,
    :rent_min,
    :rent_max,
    :rent_period,
    :availability_status,
    :available_date,
    :availability_text_raw,
    :is_currently_available,
    :listing_url,
    :observed_at,
    :listing_fingerprint
)
ON CONFLICT (source, source_record_id)
DO UPDATE SET
    listing_title = EXCLUDED.listing_title,
    bedrooms = EXCLUDED.bedrooms,
    bathrooms = EXCLUDED.bathrooms,
    sqft = EXCLUDED.sqft,
    rent_min = EXCLUDED.rent_min,
    rent_max = EXCLUDED.rent_max,
    rent_period = EXCLUDED.rent_period,
    availability_status = EXCLUDED.availability_status,
    available_date = EXCLUDED.available_date,
    availability_text_raw = EXCLUDED.availability_text_raw,
    is_currently_available = EXCLUDED.is_currently_available,
    listing_url = EXCLUDED.listing_url,
    observed_at = EXCLUDED.observed_at,
    listing_fingerprint = EXCLUDED.listing_fingerprint;
"""

STG_COUNT_BY_SOURCE_SQL = """
SELECT COUNT(*) AS n
FROM stg_listings
WHERE source = :source;
"""


def parse_money_month(text_in: str):
    if not text_in:
        return (None, None, None)
    t = text_in.replace(",", "").strip().lower()
    nums = re.findall(r"\$?\s*(\d{3,5})", t)
    if not nums:
        return (None, None, None)
    val = int(nums[0])
    period = "month" if ("month" in t or "/mo" in t or "mo" in t) else None
    return (val, val, period)


def parse_date_fuzzy(text_in: str):
    if not text_in:
        return None
    t = text_in.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(t, fmt).date()
        except Exception:
            pass
    return None


def compute_availability(available_date, availability_text_raw, observed_at: datetime):
    t = (availability_text_raw or "").strip().lower()

    if "waitlist" in t:
        return ("waitlist", False)

    if "unavailable" in t or "not available" in t:
        return ("unavailable", False)

    if available_date:
        today = observed_at.date()
        return ("available", available_date <= today)

    return ("unknown", None)


def fingerprint_without_url(title, bedrooms, rent_min, sqft):
    base = "|".join([
        (title or "").strip().lower(),
        str(bedrooms) if bedrooms is not None else "",
        str(rent_min) if rent_min is not None else "",
        str(sqft) if sqft is not None else "",
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def extract_property_links_from_residential_page(html: str, base_url: str):
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        if "/properties/" in full.lower():
            links.add(full)

    return sorted(links)


def find_next_page_url(html: str, current_url: str):
    soup = BeautifulSoup(html, "html.parser")

    a = soup.find("a", rel=lambda v: v and "next" in v)
    if a and a.get("href"):
        return urljoin(current_url, a["href"])

    for cand in soup.find_all("a", href=True):
        txt = cand.get_text(" ", strip=True).lower()
        cls = " ".join(cand.get("class", [])).lower()
        if txt == "next" or "next" in cls:
            return urljoin(current_url, cand["href"])

    return None


def crawl_all_property_links():
    all_links = set()
    visited_pages = set()

    url = START_URL
    for _ in range(MAX_PAGES):
        if url in visited_pages:
            break
        visited_pages.add(url)

        html = fetch_html(url)
        page_links = extract_property_links_from_residential_page(html, base_url=url)

        for l in page_links:
            all_links.add(l)

        if len(all_links) >= MAX_LISTINGS:
            break

        nxt = find_next_page_url(html, current_url=url)
        if not nxt:
            break
        url = nxt

    return sorted(all_links)


def parse_detail_page_fields(detail_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    # Caras has multiple <h1> tags: one is rent, one is the address.
    title = None
    h1_texts = [h.get_text(" ", strip=True) for h in soup.find_all("h1")]
    h1_texts = [t for t in h1_texts if t]

    def looks_like_rent(t: str) -> bool:
        tl = t.lower()
        return ("month" in tl) and ("$" in t or re.search(r"\d{3,5}", t))

    def looks_like_address(t: str) -> bool:
        return bool(re.match(r"^\d+\s+", t)) and not looks_like_rent(t)

    for t in h1_texts:
        if looks_like_address(t):
            title = t
            break

    if not title:
        for t in h1_texts:
            if not looks_like_rent(t):
                title = t
                break

    if not title:
        ttag = soup.find("title")
        if ttag:
            title = ttag.get_text(" ", strip=True)

    # Price, beds, baths, sqft, available date
    m_price = re.search(r"\$\s*[\d,]{3,6}\s*/\s*month", page_text, flags=re.I)
    price_text = m_price.group(0) if m_price else None

    m_bed = re.search(r"(\d+)\s*bedroom", page_text, flags=re.I)
    bedrooms = int(m_bed.group(1)) if m_bed else None

    m_bath = re.search(r"(\d+(?:\.\d+)?)\s*bathroom", page_text, flags=re.I)
    bathrooms = float(m_bath.group(1)) if m_bath else None

    m_sqft = re.search(r"(\d{2,5})\s*sf", page_text, flags=re.I)
    sqft = int(m_sqft.group(1)) if m_sqft else None

    m_date = re.search(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b", page_text)
    availability_text = m_date.group(1) if m_date else None

    return {
        "listing_url": detail_url,
        "listing_title": title,  # now should be address-like for Caras
        "price_text": price_text,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "sqft": sqft,
        "availability_text": availability_text,
        "raw_text": page_text,
    }


def insert_raw_detail_rows(detail_rows, scraped_at: datetime):
    inserted = 0
    with engine.begin() as conn:
        raw_json_type = get_raw_json_storage_type(conn)
        raw_insert_sql = build_raw_insert_sql(raw_json_type)

        for r in detail_rows:
            listing_url = r["listing_url"]
            source_record_id = source_record_id_from_url(listing_url)

            raw_payload = {
                "source_page": START_URL,
                "listing_url": listing_url,
                "title": r.get("listing_title"),  # address-like title
                "address": r.get("listing_title"),  # stored explicitly for future schema upgrade
                "price_text": r.get("price_text"),
                "bedrooms": r.get("bedrooms"),
                "bathrooms": r.get("bathrooms"),
                "sqft": r.get("sqft"),
                "availability_text": r.get("availability_text"),
            }

            payload = {
                "source": SOURCE,
                "source_url": listing_url,
                "source_record_id": source_record_id,
                "scraped_at": scraped_at,
                "raw_text": r.get("raw_text") or "",
                "raw_json": json.dumps(raw_payload, ensure_ascii=False, default=str),
            }

            conn.execute(text(raw_insert_sql), payload)
            inserted += 1

    return inserted


def normalize_latest_batch_to_stg():
    upserted = 0
    with engine.begin() as conn:
        raws = conn.execute(text(RAW_SELECT_LATEST_BATCH_SQL), {"source": SOURCE}).mappings().all()

        for r in raws:
            raw_json = r["raw_json"]
            if raw_json is None:
                raw_json = {}
            elif not isinstance(raw_json, dict):
                raw_json = json.loads(raw_json)

            listing_url = r["source_url"]

            # Prefer explicit address, fallback to title
            title = raw_json.get("address") or raw_json.get("title")

            bedrooms = raw_json.get("bedrooms")
            bathrooms = raw_json.get("bathrooms")
            sqft = raw_json.get("sqft")

            rent_min, rent_max, rent_period = parse_money_month(raw_json.get("price_text") or "")

            availability_text_raw = raw_json.get("availability_text")
            available_date = parse_date_fuzzy(availability_text_raw) if availability_text_raw else None

            observed_at = r["scraped_at"] or utc_now()
            availability_status, is_currently_available = compute_availability(
                available_date=available_date,
                availability_text_raw=availability_text_raw,
                observed_at=observed_at,
            )

            listing_fingerprint = fingerprint_without_url(
                title=title,
                bedrooms=bedrooms,
                rent_min=rent_min,
                sqft=sqft,
            )

            stg_payload = {
                "source": SOURCE,
                "source_record_id": r["source_record_id"],
                "listing_title": title,  # now should display address
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "sqft": sqft,
                "rent_min": rent_min,
                "rent_max": rent_max,
                "rent_period": rent_period,
                "availability_status": availability_status,
                "available_date": available_date,
                "availability_text_raw": availability_text_raw,
                "is_currently_available": is_currently_available,
                "listing_url": listing_url,
                "observed_at": observed_at,
                "listing_fingerprint": listing_fingerprint,
            }

            conn.execute(text(STG_UPSERT_SQL), stg_payload)
            upserted += 1

        stg_count = conn.execute(text(STG_COUNT_BY_SOURCE_SQL), {"source": SOURCE}).scalar_one()

    return upserted, stg_count


def main():
    scraped_at = utc_now()

    property_links = crawl_all_property_links()
    print(f"{SOURCE}: found {len(property_links)} property detail links across pagination")

    if len(property_links) == 0:
        raise RuntimeError(
            "Found 0 property detail links. Next steps:\n"
            "1) Open View Page Source and search for '/properties/'\n"
            "2) Paste 10 lines around one listing link so we can adjust link detection"
        )

    detail_rows = []
    for i, link in enumerate(property_links, start=1):
        html = fetch_html(link)
        detail_rows.append(parse_detail_page_fields(link, html))

        if i % 10 == 0 or i == len(property_links):
            print(f"{SOURCE}: parsed {i}/{len(property_links)} detail pages")

    raw_inserted = insert_raw_detail_rows(detail_rows, scraped_at=scraped_at)
    print(f"{SOURCE}: inserted {raw_inserted} rows into raw_listings")

    upserted, stg_count = normalize_latest_batch_to_stg()
    print(f"{SOURCE}: upserted {upserted} rows into stg_listings (latest batch only)")
    print(f"{SOURCE}: stg_listings total rows for source = {stg_count}")

    print(f"{SOURCE}: DONE")


if __name__ == "__main__":
    main()