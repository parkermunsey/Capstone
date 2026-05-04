import os
import re
import io
import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone, date

import requests
import pdfplumber
from dateutil import parser as dtparser
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from bs4 import BeautifulSoup
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in your .env file.")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

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
def make_source_record_id(property_name: str | None, address: str | None, updated: date | None, vacancies_text: str | None):
    base = (
        f"{(property_name or '').strip().lower()}|"
        f"{(address or '').strip().lower()}|"
        f"{(updated.isoformat() if updated else '')}|"
        f"{(vacancies_text or '').strip().lower()}"
    )
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

            # ---- raw_listings (append-only): one record per property row ----
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
    print(f"mha_vacancy_board upserted: {upserted_debug}")
    print(f"PDF updated_date={updated}")
    print("Next step: run normalize_raw_to_stg.py to refresh stg_listings.")


if __name__ == "__main__":
    main()
