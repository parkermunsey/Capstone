import os
import re
import io
import json
import hashlib
from datetime import datetime, timezone, date

import requests
import pdfplumber
from dateutil import parser as dtparser
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from bs4 import BeautifulSoup
from urllib.parse import urljoin


load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

AFFORDABLE_HOUSING_URL = "https://www.missoulahousing.org/affordable-housing"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; contact: parker.munsey)",
}

PHONE_RE = re.compile(r"\(\d{3}\)\s*\d{3}\-\d{4}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def clean(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None


def parse_updated_date(full_text: str) -> date | None:
    m = re.search(
        r"Information updated:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})",
        full_text,
        re.IGNORECASE,
    )
    if not m:
        return None
    try:
        return dtparser.parse(m.group(1)).date()
    except Exception:
        return None


def discover_current_vacancy_pdf_url() -> str:
    resp = requests.get(AFFORDABLE_HOUSING_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    for a in soup.find_all("a", href=True):
        if a.get_text(strip=True).lower() == "vacancies":
            return urljoin(AFFORDABLE_HOUSING_URL, a["href"].strip())

    raise RuntimeError("Could not find the 'Vacancies' link on the Affordable Housing page.")


def download_pdf_bytes(pdf_url: str) -> bytes:
    r = requests.get(pdf_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.content


def split_property_name_address(prop_block: str | None):
    if not prop_block:
        return None, None

    prop_block = clean(prop_block)
    if not prop_block:
        return None, None

    property_name = prop_block
    address = None

    m = re.search(r"\b\d{1,5}\s", prop_block)
    if m:
        idx = m.start()
        property_name = clean(prop_block[:idx])
        address = clean(prop_block[idx:])

    return property_name, address


def parse_manager_block(mgr_block: str | None):
    if not mgr_block:
        return None, None, None

    mgr_block = clean(mgr_block)
    if not mgr_block:
        return None, None, None

    phone_match = PHONE_RE.search(mgr_block)
    email_match = EMAIL_RE.search(mgr_block)

    manager_phone = phone_match.group(0) if phone_match else None
    manager_email = email_match.group(0) if email_match else None

    manager_name = clean(mgr_block.split("(")[0])

    return manager_name, manager_phone, manager_email


def extract_bedroom_list(vacancies_text: str | None) -> list[int]:
    """
    MHA vacancies text can contain multiple bedroom types like:
    "2 Bedrooms 3 Bedrooms"
    Return a sorted list of unique ints.
    """
    if not vacancies_text:
        return []

    t = vacancies_text.lower()
    beds = set()

    if "studio" in t:
        beds.add(0)

    for m in re.finditer(r"\b([0-9])\s*bedrooms?\b", t, re.IGNORECASE):
        try:
            beds.add(int(m.group(1)))
        except Exception:
            pass

    for m in re.finditer(r"\b([0-9])\s*(?:br|bd|bed)\b", t, re.IGNORECASE):
        try:
            beds.add(int(m.group(1)))
        except Exception:
            pass

    return sorted(beds)


def normalize_mha_availability(vacancies_text: str | None):
    """
    Returns: (availability_status, available_date, is_currently_available)

    Given your current PDF output, vacancies_text is either NULL or a bedroom-types string.
    So:
    - non-empty -> available
    - empty/NULL -> unavailable
    """
    t = (vacancies_text or "").strip()
    if t:
        return "available", None, True
    return "unavailable", None, False


def make_source_record_id(property_name: str | None, address: str | None, updated: date | None, vacancies_text: str | None):
    base = (
        f"{(property_name or '').strip().lower()}|"
        f"{(address or '').strip().lower()}|"
        f"{(updated.isoformat() if updated else '')}|"
        f"{(vacancies_text or '').strip().lower()}"
    )
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def make_listing_fingerprint(property_name: str | None, bedrooms: int | None, rent_min: int | None):
    base = f"{(property_name or '').strip().lower()}|{bedrooms}|{rent_min}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def main():
    pdf_url = discover_current_vacancy_pdf_url()
    print(f"Using vacancy PDF: {pdf_url}")

    pdf_bytes = download_pdf_bytes(pdf_url)
    data = io.BytesIO(pdf_bytes)

    observed_at = datetime.now(timezone.utc)
    updated = None
    rows = None

    with pdfplumber.open(data) as pdf:
        page = pdf.pages[0]
        full_text = page.extract_text() or ""
        updated = parse_updated_date(full_text)

        tables = page.extract_tables()
        if not tables:
            print("No tables detected in PDF. We will fall back to text only.")
            return

        rows = tables[0]

    if not rows or len(rows) < 2:
        print("No usable rows found in the PDF table.")
        return

    inserted_raw = 0
    upserted_stg = 0
    upserted_debug = 0

    with engine.begin() as conn:
        for row in rows[1:]:
            if not row or len(row) < 3:
                continue

            prop_block = clean(row[0])
            vac_block = clean(row[1])
            mgr_block = clean(row[2])

            if not prop_block:
                continue

            property_name, address = split_property_name_address(prop_block)
            manager_name, manager_phone, manager_email = parse_manager_block(mgr_block)

            availability_status, available_date, is_current = normalize_mha_availability(vac_block)

            # explode bedrooms into multiple staged rows (or [None] if no bedrooms found)
            bedroom_list = extract_bedroom_list(vac_block) or [None]

            # ---- raw_listings (append-only): one record per property row (not per bedroom) ----
            raw_payload = {
                "property_name": property_name,
                "address": address,
                "vacancies_text": vac_block,
                "manager_name": manager_name,
                "manager_phone": manager_phone,
                "manager_email": manager_email,
                "updated_date": updated.isoformat() if updated else None,
                "source_pdf_url": pdf_url,
            }

            raw_source_record_id = make_source_record_id(property_name, address, updated, vac_block)

            conn.execute(
                text(
                    """
                    INSERT INTO raw_listings (
                        source,
                        source_url,
                        source_record_id,
                        scraped_at,
                        raw_text,
                        raw_json
                    )
                    VALUES (
                        'mha',
                        :source_url,
                        :source_record_id,
                        :scraped_at,
                        :raw_text,
                        :raw_json
                    );
                    """
                ),
                {
                    "source_url": pdf_url,
                    "source_record_id": raw_source_record_id,
                    "scraped_at": observed_at,
                    "raw_text": (
                        f"property={property_name}; address={address}; "
                        f"vacancies={vac_block}; manager={manager_name}; "
                        f"phone={manager_phone}; email={manager_email}; updated={updated}"
                    ),
                    "raw_json": json.dumps(raw_payload, ensure_ascii=False, default=str),
                },
            )
            inserted_raw += 1

            # ---- stg_listings (upsert): one row per bedroom type ----
            for bedrooms in bedroom_list:
                # make source_record_id unique per bedroom type so conflict handling is stable
                stg_source_record_id = make_source_record_id(
                    property_name, address, updated, f"{vac_block}|beds={bedrooms}"
                )
                listing_fingerprint = make_listing_fingerprint(property_name, bedrooms, None)

                conn.execute(
                    text(
                        """
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
                            'mha',
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
                        ON CONFLICT (source, source_record_id) DO UPDATE SET
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
                    ),
                    {
                        "source_record_id": stg_source_record_id,
                        "listing_title": property_name,
                        "bedrooms": bedrooms,
                        "bathrooms": None,
                        "sqft": None,
                        "rent_min": None,
                        "rent_max": None,
                        "rent_period": "month",
                        "availability_status": availability_status,
                        "available_date": available_date,
                        "availability_text_raw": vac_block,
                        "is_currently_available": is_current,
                        "listing_url": pdf_url,
                        "observed_at": observed_at,
                        "listing_fingerprint": listing_fingerprint,
                    },
                )
                upserted_stg += 1

            # ---- optional debug table mha_vacancy_board (append-safe) ----
            conn.execute(
                text(
                    """
                    INSERT INTO mha_vacancy_board
                      (property_name, address, vacancies_text, manager_name, manager_phone, manager_email, updated_date, source_pdf_url)
                    VALUES
                      (:property_name, :address, :vacancies_text, :manager_name, :manager_phone, :manager_email, :updated_date, :source_pdf_url)
                    ON CONFLICT (property_name, source_pdf_url, updated_date) DO UPDATE SET
                      vacancies_text = EXCLUDED.vacancies_text,
                      manager_name = COALESCE(EXCLUDED.manager_name, mha_vacancy_board.manager_name),
                      manager_phone = COALESCE(EXCLUDED.manager_phone, mha_vacancy_board.manager_phone),
                      manager_email = COALESCE(EXCLUDED.manager_email, mha_vacancy_board.manager_email),
                      ingested_at = NOW();
                    """
                ),
                {
                    "property_name": property_name,
                    "address": address,
                    "vacancies_text": vac_block,
                    "manager_name": manager_name,
                    "manager_phone": manager_phone,
                    "manager_email": manager_email,
                    "updated_date": updated,
                    "source_pdf_url": pdf_url,
                },
            )
            upserted_debug += 1

    print(f"raw_listings inserted: {inserted_raw}")
    print(f"stg_listings upserted (includes bedroom explode): {upserted_stg}")
    print(f"mha_vacancy_board upserted: {upserted_debug}")
    print(f"PDF updated_date={updated}")


if __name__ == "__main__":
    main()