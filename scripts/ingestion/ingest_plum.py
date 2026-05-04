import os
import re
import json
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs

import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in your .env file.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SOURCE = "plum"
START_URL = "https://rentplum.com/properties/"
BASE_DOMAIN = "https://rentplum.com"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; Parker Munsey)",
    "Accept": "text/html,application/xhtml+xml",
}

MAX_PAGES = 30
MAX_LISTINGS = 600
REQUEST_TIMEOUT = 45
FETCH_RETRIES = 4
FETCH_BACKOFF_SECONDS = 2.0
DETAIL_PAGE_DELAY_SECONDS = 0.5


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


def fetch_html(
    url: str,
    retries: int = FETCH_RETRIES,
    backoff_seconds: float = FETCH_BACKOFF_SECONDS,
) -> str:
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except RequestException as exc:
            last_error = exc
            print(f"{SOURCE}: request failed for {url} on attempt {attempt}/{retries}: {exc}")

            if attempt < retries:
                sleep_for = backoff_seconds * attempt
                time.sleep(sleep_for)

    raise RuntimeError(f"{SOURCE}: failed to fetch {url} after {retries} attempts") from last_error


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


def clean_text(value) -> str | None:
    if value is None:
        return None
    text_value = re.sub(r"\s+", " ", str(value)).strip()
    return text_value or None


def extract_labeled_value(text_lines: list[str], *labels: str) -> str | None:
    normalized_labels = {label.lower().rstrip(":") for label in labels}

    for idx, line in enumerate(text_lines[:-1]):
        normalized_line = line.lower().rstrip(":")
        if normalized_line not in normalized_labels:
            continue

        candidate = text_lines[idx + 1].strip()
        if candidate:
            return candidate

    return None


def extract_value_from_block_text(block_text: str, label: str) -> str | None:
    pattern = rf"{re.escape(label)}\s*:\s*(.+?)(?:\n|$)"
    match = re.search(pattern, block_text, flags=re.IGNORECASE)
    if match:
        return clean_text(match.group(1))
    return None


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

    for cand in soup.find_all("a", href=True):
        href = (cand.get("href") or "").strip()
        parts = href.rstrip("/").split("/")
        if len(parts) >= 2 and parts[-2] == "page":
            return urljoin(current_url, href)

    return None


def looks_like_listing_container(node) -> bool:
    if node is None or getattr(node, "name", None) not in {"div", "article", "section", "li"}:
        return False

    text_value = node.get_text("\n", strip=True)
    lowered = text_value.lower()

    return (
        "property type" in lowered
        and ("learn more" in lowered or "rent:" in lowered or "deposit:" in lowered)
    )


def find_listing_container(anchor):
    node = anchor
    for _ in range(10):
        if node is None:
            break
        if looks_like_listing_container(node):
            return node
        node = node.parent
    return None


def parse_index_card(container, detail_url: str) -> dict | None:
    block_text = container.get_text("\n", strip=True)
    text_lines = [line.strip() for line in block_text.splitlines() if line.strip()]

    property_type = extract_value_from_block_text(block_text, "Property Type")
    if not property_type:
        property_type = extract_labeled_value(text_lines, "Property Type")

    title = None
    for candidate in container.find_all(["h1", "h2", "h3", "h4", "strong"]):
        candidate_text = clean_text(candidate.get_text(" ", strip=True))
        if not candidate_text:
            continue
        lowered = candidate_text.lower()
        if lowered in {
            "learn more",
            "property type",
            "unit status",
            "rent",
            "deposit",
            "bedrooms",
            "bathrooms",
            "square footage",
        }:
            continue
        title = candidate_text
        break

    if not title:
        for line in text_lines:
            lowered = line.lower()
            if lowered in {
                "learn more",
                "property type",
                "unit status",
                "rent",
                "deposit",
                "bedrooms",
                "bathrooms",
                "square footage",
            }:
                continue
            if ":" in line:
                continue
            title = clean_text(line)
            if title:
                break

    unit_status = extract_value_from_block_text(block_text, "Unit Status")
    if not unit_status:
        unit_status = extract_labeled_value(text_lines, "Unit Status")

    rent_text = extract_value_from_block_text(block_text, "Rent")
    if not rent_text:
        rent_text = extract_labeled_value(text_lines, "Rent")

    deposit_text = extract_value_from_block_text(block_text, "Deposit")
    if not deposit_text:
        deposit_text = extract_labeled_value(text_lines, "Deposit")

    bedrooms_raw = extract_value_from_block_text(block_text, "Bedrooms")
    if not bedrooms_raw:
        bedrooms_raw = extract_labeled_value(text_lines, "Bedrooms")

    bathrooms_raw = extract_value_from_block_text(block_text, "Bathrooms")
    if not bathrooms_raw:
        bathrooms_raw = extract_labeled_value(text_lines, "Bathrooms")

    sqft_raw = extract_value_from_block_text(block_text, "Square Footage")
    if not sqft_raw:
        sqft_raw = extract_labeled_value(text_lines, "Square Footage", "Sq Ft", "Sq. Ft.")

    return {
        "listing_url": detail_url,
        "listing_title": title,
        "property_type": clean_text(property_type),
        "unit_status": clean_text(unit_status),
        "available_text": clean_text(unit_status),
        "rent_text": clean_text(rent_text),
        "deposit_text": clean_text(deposit_text),
        "bedrooms_raw": clean_text(bedrooms_raw),
        "bathrooms_raw": clean_text(bathrooms_raw),
        "sqft_raw": clean_text(sqft_raw),
        "raw_index_text": block_text,
    }


def extract_residential_cards_from_index(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    residential_cards = []
    seen_urls = set()

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href"))
        if not href:
            continue

        full_url = urljoin(base_url, href)
        lowered_url = full_url.lower()

        if "details" not in lowered_url or "pid=" not in lowered_url or "id=" not in lowered_url:
            continue

        container = find_listing_container(anchor)
        if container is None:
            continue

        card = parse_index_card(container, detail_url=full_url)
        if card is None:
            continue

        property_type = (card.get("property_type") or "").strip().lower()
        if property_type != "residential":
            continue

        if full_url in seen_urls:
            continue

        seen_urls.add(full_url)
        residential_cards.append(card)

    return residential_cards


def crawl_residential_cards():
    all_cards_by_url = {}
    visited_pages = set()

    url = START_URL
    for _ in range(MAX_PAGES):
        if url in visited_pages:
            break
        visited_pages.add(url)

        print(f"{SOURCE}: crawling index page {url}")
        html = fetch_html(url)

        page_cards = extract_residential_cards_from_index(html, base_url=url)
        print(f"{SOURCE}: found {len(page_cards)} residential cards on this page")

        for card in page_cards:
            all_cards_by_url[card["listing_url"]] = card

        if len(all_cards_by_url) >= MAX_LISTINGS:
            break

        nxt = find_next_page_url(html, current_url=url)
        if not nxt:
            break
        url = nxt

    return list(all_cards_by_url.values())


def parse_detail_page(detail_url: str, html: str, index_card: dict | None = None):
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)
    text_lines = [line.strip() for line in page_text.splitlines() if line.strip()]

    title = None
    h1 = soup.find("h1")
    if h1:
        title = clean_text(h1.get_text(" ", strip=True))
    if not title:
        t = soup.find("title")
        title = clean_text(t.get_text(" ", strip=True)) if t else None
    if not title and index_card:
        title = index_card.get("listing_title")

    property_type = None
    m = re.search(r"Property Type:\s*(.+)", page_text, flags=re.I)
    if m:
        property_type = clean_text(m.group(1))
    if not property_type:
        property_type = extract_labeled_value(text_lines, "Property Type")
    if not property_type and index_card:
        property_type = index_card.get("property_type")

    unit_status = None
    m = re.search(r"Unit Status:\s*(.+)", page_text, flags=re.I)
    if m:
        unit_status = clean_text(m.group(1))
    if not unit_status:
        unit_status = extract_labeled_value(text_lines, "Unit Status")
    if not unit_status and index_card:
        unit_status = index_card.get("unit_status")

    available_value = extract_labeled_value(text_lines, "Available")
    if available_value:
        if unit_status and available_value.lower() not in unit_status.lower():
            unit_status = f"{unit_status} {available_value}"
        elif not unit_status:
            unit_status = f"Available {available_value}"

    rent_text = None
    m = re.search(r"Rent:\s*(\$\s*[\d,]+(?:\.\d{2})?)", page_text, flags=re.I)
    if m:
        rent_text = clean_text(m.group(1))
    if not rent_text:
        rent_text = extract_labeled_value(text_lines, "Rent")
    if not rent_text and index_card:
        rent_text = index_card.get("rent_text")

    deposit_text = None
    m = re.search(r"Deposit:\s*(\$\s*[\d,]+(?:\.\d{2})?)", page_text, flags=re.I)
    if m:
        deposit_text = clean_text(m.group(1))
    if not deposit_text:
        deposit_text = extract_labeled_value(text_lines, "Deposit")
    if not deposit_text and index_card:
        deposit_text = index_card.get("deposit_text")

    bedrooms_raw = None
    m = re.search(r"Bedrooms:\s*([A-Za-z0-9]+)", page_text, flags=re.I)
    if m:
        bedrooms_raw = clean_text(m.group(1))
    if not bedrooms_raw:
        bedrooms_raw = extract_labeled_value(text_lines, "Bedrooms")
    if not bedrooms_raw and index_card:
        bedrooms_raw = index_card.get("bedrooms_raw")

    bathrooms_raw = None
    m = re.search(r"Bathrooms:\s*([0-9]+(?:\.[0-9]+)?)", page_text, flags=re.I)
    if m:
        bathrooms_raw = clean_text(m.group(1))
    if not bathrooms_raw:
        bathrooms_raw = extract_labeled_value(text_lines, "Bathrooms")
    if not bathrooms_raw and index_card:
        bathrooms_raw = index_card.get("bathrooms_raw")

    sqft_raw = None
    m = re.search(r"Sq\.?\s*Ft\.?:\s*([0-9]{2,6})", page_text, flags=re.I)
    if m:
        sqft_raw = clean_text(m.group(1))
    if not sqft_raw:
        sqft_raw = extract_labeled_value(text_lines, "Square Footage", "Sq Ft", "Sq. Ft.")
    if not sqft_raw and index_card:
        sqft_raw = index_card.get("sqft_raw")

    return {
        "listing_url": detail_url,
        "listing_title": title,
        "property_type": clean_text(property_type),
        "unit_status": clean_text(unit_status),
        "available_text": clean_text(unit_status),
        "rent_text": clean_text(rent_text),
        "deposit_text": clean_text(deposit_text),
        "bedrooms_raw": clean_text(bedrooms_raw),
        "bathrooms_raw": clean_text(bathrooms_raw),
        "sqft_raw": clean_text(sqft_raw),
        "raw_text": page_text,
        "raw_index_text": index_card.get("raw_index_text") if index_card else None,
    }


def insert_raw(detail_rows, scraped_at: datetime):
    inserted = 0
    with engine.begin() as conn:
        raw_json_type = get_raw_json_storage_type(conn)
        raw_insert_sql = build_raw_insert_sql(raw_json_type)

        for row in detail_rows:
            detail_url = row["listing_url"]
            source_record_id = source_record_id_from_detail_url(detail_url)

            raw_payload = {
                "source_page": START_URL,
                "listing_url": detail_url,
                "title": row.get("listing_title"),
                "property_type": row.get("property_type"),
                "unit_status": row.get("unit_status"),
                "available_text": row.get("available_text"),
                "rent_text": row.get("rent_text"),
                "deposit_text": row.get("deposit_text"),
                "bedrooms_raw": row.get("bedrooms_raw"),
                "bathrooms_raw": row.get("bathrooms_raw"),
                "sqft_raw": row.get("sqft_raw"),
                "raw_index_text": row.get("raw_index_text"),
            }

            payload = {
                "source": SOURCE,
                "source_url": detail_url,
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

    residential_cards = crawl_residential_cards()
    print(f"{SOURCE}: found {len(residential_cards)} residential detail links across pagination")

    if len(residential_cards) == 0:
        raise RuntimeError(
            "Found 0 residential detail links.\n"
            "Next step: inspect one visible Residential listing card and confirm the card still contains "
            "'Property Type: Residential' plus a LEARN MORE details link."
        )

    detail_rows = []
    failed_links = []
    skipped_non_residential = 0

    for i, card in enumerate(residential_cards, start=1):
        link = card["listing_url"]

        try:
            html = fetch_html(link)
            parsed = parse_detail_page(link, html, index_card=card)

            property_type = (parsed.get("property_type") or "").strip().lower()
            if property_type and property_type != "residential":
                skipped_non_residential += 1
                print(f"{SOURCE}: skipping non-residential detail page {i}/{len(residential_cards)} -> {link}")
            else:
                detail_rows.append(parsed)

        except Exception as exc:
            failed_links.append({"url": link, "error": str(exc)})
            print(f"{SOURCE}: skipping failed detail page {i}/{len(residential_cards)} -> {link}")
            print(f"{SOURCE}: error: {exc}")

        time.sleep(DETAIL_PAGE_DELAY_SECONDS)

        if i % 10 == 0 or i == len(residential_cards):
            print(f"{SOURCE}: processed {i}/{len(residential_cards)} residential detail pages")

    if not detail_rows:
        raise RuntimeError(f"{SOURCE}: no residential detail pages were successfully parsed")

    raw_inserted = insert_raw(detail_rows, scraped_at=scraped_at)
    print(f"{SOURCE}: inserted {raw_inserted} rows into raw_listings")

    if skipped_non_residential:
        print(f"{SOURCE}: skipped {skipped_non_residential} detail pages because they were not residential")

    if failed_links:
        print(f"{SOURCE}: failed on {len(failed_links)} detail pages")
        for item in failed_links[:10]:
            print(f"{SOURCE}: failed url -> {item['url']}")
        if len(failed_links) > 10:
            print(f"{SOURCE}: additional failed links not shown: {len(failed_links) - 10}")

    print(f"{SOURCE}: raw ingestion complete")


if __name__ == "__main__":
    main()