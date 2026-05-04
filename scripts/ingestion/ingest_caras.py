import os
import re
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse
from pathlib import Path

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

    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or "").lower()
    netloc = (parsed.netloc or "").lower()
    path = parsed.path or ""
    query = parsed.query or ""

    return urlunparse((scheme, netloc, path, "", query, ""))


def source_record_id_from_url(url: str) -> str:
    normalized = normalize_url_for_id(url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=45)
    response.raise_for_status()
    return response.text


def get_raw_json_storage_type(conn) -> str:
    sql = """
    SELECT data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'raw_listings'
      AND column_name = 'raw_json'
    """
    row = conn.execute(text(sql)).mappings().first()

    if not row:
        return "unknown"
    if row.get("udt_name") == "jsonb":
        return "jsonb"

    return row.get("data_type") or "unknown"


def build_raw_insert_sql(raw_json_type: str) -> str:
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


def extract_property_links_from_residential_page(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        full_url = urljoin(base_url, href)
        if "/properties/" in full_url.lower():
            links.add(full_url)

    return sorted(links)


def find_next_page_url(html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    next_link = soup.find("a", rel=lambda value: value and "next" in str(value).lower())
    if next_link and next_link.get("href"):
        return urljoin(current_url, next_link["href"])

    for candidate in soup.find_all("a", href=True):
        text_value = candidate.get_text(" ", strip=True).lower()
        class_value = " ".join(candidate.get("class", [])).lower()
        aria_label = (candidate.get("aria-label") or "").lower()

        if (
            "next" in text_value
            or "next" in class_value
            or "next" in aria_label
        ):
            return urljoin(current_url, candidate["href"])

    return None


def crawl_all_property_links() -> list[str]:
    all_links = set()
    visited_pages = set()

    current_url = START_URL

    for _ in range(MAX_PAGES):
        if current_url in visited_pages:
            break

        visited_pages.add(current_url)

        html = fetch_html(current_url)
        page_links = extract_property_links_from_residential_page(html, base_url=current_url)

        for link in page_links:
            all_links.add(link)

        if len(all_links) >= MAX_LISTINGS:
            break

        next_url = find_next_page_url(html, current_url=current_url)
        if not next_url:
            break

        current_url = next_url

    return sorted(all_links)


def parse_detail_page_fields(detail_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    h1_texts = [h.get_text(" ", strip=True) for h in soup.find_all("h1")]
    h1_texts = [t for t in h1_texts if t]

    def looks_like_rent(t: str) -> bool:
        if not t:
            return False
        tl = t.lower()
        return ("month" in tl) and ("$" in t or re.search(r"\d{3,5}", t))

    def looks_like_address(t: str) -> bool:
        if not t:
            return False
        t = t.strip()
        return bool(re.match(r"^\d+\s+", t)) and not looks_like_rent(t)

    def clean_slug_to_title(url: str) -> str | None:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if not parts:
            return None

        slug = parts[-1]
        if slug in {"properties", ""}:
            return None

        text_value = slug.replace("-", " ").strip()
        if not text_value:
            return None

        # Clean up common formatting leftovers
        text_value = re.sub(r"\s+", " ", text_value)
        text_value = re.sub(r"\bmissoula\b", "Missoula", text_value, flags=re.I)
        text_value = re.sub(r"\blower duplex\b", "Lower Duplex", text_value, flags=re.I)
        text_value = re.sub(r"\bupper duplex\b", "Upper Duplex", text_value, flags=re.I)

        return text_value.title()

    title = None

    # 1. Best case: use an address-like H1
    for t in h1_texts:
        if looks_like_address(t):
            title = t.strip()
            break

    # 2. Try HTML <title> if it looks address-like
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title_candidate = title_tag.get_text(" ", strip=True)
            if title_candidate and not looks_like_rent(title_candidate):
                # Remove common site suffix if present
                title_candidate = re.sub(r"\s*\|\s*.*$", "", title_candidate).strip()
                if looks_like_address(title_candidate):
                    title = title_candidate

    # 3. Try slug-derived title from URL
    if not title:
        slug_title = clean_slug_to_title(detail_url)
        if slug_title and not looks_like_rent(slug_title):
            title = slug_title

    # 4. Last resort: first non-rent H1
    if not title:
        for t in h1_texts:
            if not looks_like_rent(t):
                title = t.strip()
                break

    # 5. Absolute fallback: if title is still rent-like, use URL slug
    if not title or looks_like_rent(title):
        slug_title = clean_slug_to_title(detail_url)
        if slug_title:
            title = slug_title

    price_match = re.search(r"\$\s*[\d,]{3,6}\s*/\s*month", page_text, flags=re.I)
    price_text = price_match.group(0) if price_match else None

    bedroom_match = re.search(
    r"(\d+)\s*(?:bedroom|bedrooms|bed|beds|bd)\b",
    page_text,
    flags=re.I
)
    bedrooms = int(bedroom_match.group(1)) if bedroom_match else None

    bathroom_match = re.search(r"(\d+(?:\.\d+)?)\s*bathroom", page_text, flags=re.I)
    bathrooms = float(bathroom_match.group(1)) if bathroom_match else None

    sqft_match = re.search(r"(\d{2,5})\s*sf", page_text, flags=re.I)
    sqft = int(sqft_match.group(1)) if sqft_match else None

    date_match = re.search(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b", page_text)
    availability_text = date_match.group(1) if date_match else None

    return {
        "listing_url": detail_url,
        "listing_title": title,
        "price_text": price_text,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "sqft": sqft,
        "availability_text": availability_text,
        "raw_text": page_text,
    }


def insert_raw_detail_rows(detail_rows: list[dict], scraped_at: datetime) -> int:
    inserted = 0

    with engine.begin() as conn:
        raw_json_type = get_raw_json_storage_type(conn)
        raw_insert_sql = build_raw_insert_sql(raw_json_type)

        for row in detail_rows:
            listing_url = row["listing_url"]
            source_record_id = source_record_id_from_url(listing_url)

            raw_payload = {
                "source_page": START_URL,
                "listing_url": listing_url,
                "title": row.get("listing_title"),
                "address": row.get("listing_title"),
                "price_text": row.get("price_text"),
                "bedrooms": row.get("bedrooms"),
                "bathrooms": row.get("bathrooms"),
                "sqft": row.get("sqft"),
                "availability_text": row.get("availability_text"),
            }

            payload = {
                "source": SOURCE,
                "source_url": listing_url,
                "source_record_id": source_record_id,
                "scraped_at": scraped_at,
                "raw_text": row.get("raw_text") or "",
                "raw_json": json.dumps(raw_payload, ensure_ascii=False, default=str),
            }

            conn.execute(text(raw_insert_sql), payload)
            inserted += 1

    return inserted


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
    print(f"{SOURCE}: raw ingestion complete")


if __name__ == "__main__":
    main()