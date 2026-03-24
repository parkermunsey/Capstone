"""
ingest_plum.py

Plum Property Management (rentplum.com) → raw_listings (append-only) → stg_listings (upsert)

Pattern:
- Crawl properties index pages (with pagination)
- Extract detail URLs (robust extraction: hrefs, data-* attrs, regex scan)
- Fetch each detail page and parse normalized fields
- Insert into raw_listings (append-only)
- Normalize only latest scrape batch into stg_listings (upsert)

Notes:
- Enum is enforced: available / waitlist / unavailable / unknown
- listing_fingerprint does NOT include URL
- SQLAlchemy engine + text(), no ORM
"""

import os
import re
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

SOURCE = "plum"
START_URL = "https://rentplum.com/properties/"
BASE_DOMAIN = "https://rentplum.com"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; Parker Munsey)",
    "Accept": "text/html,application/xhtml+xml",
}

MAX_PAGES = 30
MAX_LISTINGS = 600


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


def source_record_id_from_detail_url(detail_url: str) -> str:
    """
    Prefer stable pid+id if present, else hash normalized URL.
    """
    try:
        p = urlparse(detail_url)
        qs = parse_qs(p.query)
        pid = (qs.get("pid") or [None])[0]
        _id = (qs.get("id") or [None])[0]
        if pid and _id:
            base = f"pid={pid}|id={_id}"
            return hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]
    except Exception:
        pass

    norm = normalize_url_for_id(detail_url)
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
    For jsonb, cast with CAST(:raw_json AS jsonb).
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
  AND scraped_at = (SELECT max(scraped_at) FROM raw_listings WHERE source = :source);
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


def extract_detail_links_from_index(html: str, base_url: str):
    """
    Robust extraction for Plum:
    1) Any <a href> containing "details" + pid= + id=
    2) Any element with data-pid/data-id patterns
    3) Regex scan for pid/id pairs near 'details' in the raw HTML
    """
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    # 1) Standard anchors
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        low = full.lower()
        if "details" in low and "pid=" in low and "id=" in low:
            links.add(full)

    # 2) data-* attributes
    for tag in soup.find_all(True):
        attrs = tag.attrs or {}
        pid = attrs.get("data-pid") or attrs.get("data_pid") or attrs.get("pid")
        _id = attrs.get("data-id") or attrs.get("data_id")

        if pid and _id:
            pid_s = str(pid).strip()
            id_s = str(_id).strip()
            if pid_s.isdigit() and id_s.isdigit():
                links.add(f"{BASE_DOMAIN}/details/?pid={pid_s}&id={id_s}")

    # 3) Regex scan for embedded URLs or fragments
    for m in re.finditer(r"(details[^\"']*?pid=(\d+)[^\"']*?id=(\d+))", html, flags=re.I):
        chunk = m.group(1)
        pid = m.group(2)
        _id = m.group(3)

        if chunk.lower().startswith("http"):
            links.add(chunk)
        else:
            # normalize to /details/... if it starts with details/...
            if "details" in chunk.lower() and not chunk.startswith("/"):
                chunk = "/" + chunk
            links.add(urljoin(BASE_DOMAIN, chunk))
            # also add canonical form
            links.add(f"{BASE_DOMAIN}/details/?pid={pid}&id={_id}")

    return sorted(links)


def find_next_page_url(html: str, current_url: str):
    """
    WordPress pagination patterns:
    - rel="next"
    - anchor text "Next"
    - /page/2/ style links
    """
    soup = BeautifulSoup(html, "html.parser")

    # rel=next
    a = soup.find("a", rel=lambda v: v and "next" in v)
    if a and a.get("href"):
        return urljoin(current_url, a["href"])

    # link text/class contains "next"
    for cand in soup.find_all("a", href=True):
        txt = cand.get_text(" ", strip=True).lower()
        cls = " ".join(cand.get("class", [])).lower()
        if txt == "next" or "next" in cls:
            return urljoin(current_url, cand["href"])

    # fallback: /page/2/ if present anywhere
    for cand in soup.find_all("a", href=True):
        href = (cand.get("href") or "").strip()
        if "/page/" in href and href.rstrip("/").split("/")[-2] == "page":
            return urljoin(current_url, href)

    return None


def crawl_all_detail_links():
    all_links = set()
    visited_pages = set()

    url = START_URL
    for _ in range(MAX_PAGES):
        if url in visited_pages:
            break
        visited_pages.add(url)

        print(f"{SOURCE}: crawling index page {url}")
        html = fetch_html(url)

        page_links = extract_detail_links_from_index(html, base_url=url)
        print(f"{SOURCE}: found {len(page_links)} detail links on this page")

        for l in page_links:
            all_links.add(l)

        if len(all_links) >= MAX_LISTINGS:
            break

        nxt = find_next_page_url(html, current_url=url)
        if not nxt:
            break
        url = nxt

    return sorted(all_links)


def money_to_int(text_in: str):
    if not text_in:
        return None
    t = text_in.replace(",", "")
    m = re.search(r"\$\s*(\d{2,6})(?:\.\d{2})?", t)
    return int(m.group(1)) if m else None


def parse_bedrooms(text_in: str):
    if not text_in:
        return None
    t = text_in.strip().lower()
    if "studio" in t:
        return 0
    m = re.search(r"(\d+)", t)
    return int(m.group(1)) if m else None


def parse_float(text_in: str):
    if not text_in:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", text_in)
    return float(m.group(1)) if m else None


def parse_int(text_in: str):
    if not text_in:
        return None
    m = re.search(r"(\d+)", text_in)
    return int(m.group(1)) if m else None


def parse_detail_page(detail_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    # Title
    title = None
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
    if not title:
        t = soup.find("title")
        title = t.get_text(" ", strip=True) if t else None

    # Unit Status / Rent / Deposit / Bedrooms / Bathrooms / Sq Ft
    unit_status = None
    m = re.search(r"Unit Status:\s*(.+)", page_text, flags=re.I)
    if m:
        unit_status = m.group(1).strip()

    rent_text = None
    m = re.search(r"Rent:\s*(\$\s*[\d,]+(?:\.\d{2})?)", page_text, flags=re.I)
    if m:
        rent_text = m.group(1).strip()

    deposit_text = None
    m = re.search(r"Deposit:\s*(\$\s*[\d,]+(?:\.\d{2})?)", page_text, flags=re.I)
    if m:
        deposit_text = m.group(1).strip()

    bedrooms_raw = None
    m = re.search(r"Bedrooms:\s*([A-Za-z0-9]+)", page_text, flags=re.I)
    if m:
        bedrooms_raw = m.group(1).strip()

    bathrooms_raw = None
    m = re.search(r"Bathrooms:\s*([0-9]+(?:\.[0-9]+)?)", page_text, flags=re.I)
    if m:
        bathrooms_raw = m.group(1).strip()

    sqft_raw = None
    m = re.search(r"Sq\.?\s*Ft\.?:\s*([0-9]{2,6})", page_text, flags=re.I)
    if m:
        sqft_raw = m.group(1).strip()

    return {
        "listing_url": detail_url,
        "listing_title": title,
        "unit_status": unit_status,
        "rent_text": rent_text,
        "deposit_text": deposit_text,
        "bedrooms_raw": bedrooms_raw,
        "bathrooms_raw": bathrooms_raw,
        "sqft_raw": sqft_raw,
        "raw_text": page_text,
    }


def normalize_availability(unit_status: str):
    """
    Enum: available / waitlist / unavailable / unknown
    Plum often uses: "Call to Schedule a Showing!" which is not definitive availability.
    """
    t = (unit_status or "").strip().lower()
    if not t:
        return ("unknown", None)

    if "waitlist" in t:
        return ("waitlist", False)

    if "unavailable" in t or "not available" in t or "rented" in t:
        return ("unavailable", False)

    if "available" in t:
        return ("available", True)

    return ("unknown", None)


def fingerprint_without_url(title, bedrooms, rent_min, sqft):
    base = "|".join([
        (title or "").strip().lower(),
        str(bedrooms) if bedrooms is not None else "",
        str(rent_min) if rent_min is not None else "",
        str(sqft) if sqft is not None else "",
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def insert_raw(detail_rows, scraped_at: datetime):
    inserted = 0
    with engine.begin() as conn:
        raw_json_type = get_raw_json_storage_type(conn)
        raw_insert_sql = build_raw_insert_sql(raw_json_type)

        for r in detail_rows:
            detail_url = r["listing_url"]
            source_record_id = source_record_id_from_detail_url(detail_url)

            raw_payload = {
                "source_page": START_URL,
                "listing_url": detail_url,
                "title": r.get("listing_title"),
                "unit_status": r.get("unit_status"),
                "rent_text": r.get("rent_text"),
                "deposit_text": r.get("deposit_text"),
                "bedrooms_raw": r.get("bedrooms_raw"),
                "bathrooms_raw": r.get("bathrooms_raw"),
                "sqft_raw": r.get("sqft_raw"),
            }

            payload = {
                "source": SOURCE,
                "source_url": detail_url,
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
            title = raw_json.get("title")

            bedrooms = parse_bedrooms(raw_json.get("bedrooms_raw") or "")
            bathrooms = parse_float(raw_json.get("bathrooms_raw") or "")
            sqft = parse_int(raw_json.get("sqft_raw") or "")

            rent_min = money_to_int(raw_json.get("rent_text") or "")
            rent_max = rent_min
            rent_period = "month" if rent_min is not None else None

            availability_text_raw = raw_json.get("unit_status")
            availability_status, is_currently_available = normalize_availability(availability_text_raw)

            observed_at = r["scraped_at"] or utc_now()

            listing_fingerprint = fingerprint_without_url(
                title=title,
                bedrooms=bedrooms,
                rent_min=rent_min,
                sqft=sqft,
            )

            stg_payload = {
                "source": SOURCE,
                "source_record_id": r["source_record_id"],
                "listing_title": title,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "sqft": sqft,
                "rent_min": rent_min,
                "rent_max": rent_max,
                "rent_period": rent_period,
                "availability_status": availability_status,
                "available_date": None,
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

    detail_links = crawl_all_detail_links()
    print(f"{SOURCE}: found {len(detail_links)} detail links across pagination")

    if len(detail_links) == 0:
        raise RuntimeError(
            "Found 0 detail links.\n"
            "Next step: View Page Source on https://rentplum.com/properties/ and search for 'details' or 'pid='.\n"
            "If still stuck, paste 10–20 lines around one 'LEARN MORE' button from the page source."
        )

    detail_rows = []
    for i, link in enumerate(detail_links, start=1):
        html = fetch_html(link)
        detail_rows.append(parse_detail_page(link, html))
        if i % 10 == 0 or i == len(detail_links):
            print(f"{SOURCE}: parsed {i}/{len(detail_links)} detail pages")

    raw_inserted = insert_raw(detail_rows, scraped_at=scraped_at)
    print(f"{SOURCE}: inserted {raw_inserted} rows into raw_listings")

    upserted, stg_count = normalize_latest_batch_to_stg()
    print(f"{SOURCE}: upserted {upserted} rows into stg_listings (latest batch only)")
    print(f"{SOURCE}: stg_listings total rows for source = {stg_count}")
    print(f"{SOURCE}: DONE")


if __name__ == "__main__":
    main()