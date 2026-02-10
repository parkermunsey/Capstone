import os
import re
import io
from datetime import date

import requests
import pdfplumber
from dateutil import parser as dtparser
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

PDF_URL = "https://static1.squarespace.com/static/63ca2057f38ca022de3416c7/t/69861c5b96719b01b04f0570/1770396763034/vacancy+board+2.6.pdf"

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
    # Example in PDF: "Information updated: 2/6/2026"
    m = re.search(r"Information updated:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", full_text, re.IGNORECASE)
    if not m:
        return None
    try:
        return dtparser.parse(m.group(1)).date()
    except Exception:
        return None

def main():
    r = requests.get(PDF_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()

    data = io.BytesIO(r.content)

    with pdfplumber.open(data) as pdf:
        page = pdf.pages[0]
        full_text = page.extract_text() or ""
        updated = parse_updated_date(full_text)

        tables = page.extract_tables()
        if not tables:
            print("No tables detected in PDF. We will fall back to text only.")
            return

        # First table is typically the main one
        rows = tables[0]

    inserted = 0
    with engine.begin() as conn:
        for row in rows[1:]:
            if not row or len(row) < 3:
                continue

            prop_block = clean(row[0])
            vac_block = clean(row[1])
            mgr_block = clean(row[2])

            if not prop_block:
                continue

            # Property block often contains name and address on separate lines
            lines = prop_block.split(" ")
            property_name = prop_block
            address = None

            # Try to split by common pattern: name then address number
            m = re.search(r"\b\d{1,5}\s", prop_block)
            if m:
                idx = m.start()
                property_name = clean(prop_block[:idx])
                address = clean(prop_block[idx:])

            manager_name = None
            manager_phone = None
            manager_email = None

            if mgr_block:
                manager_phone = PHONE_RE.search(mgr_block).group(0) if PHONE_RE.search(mgr_block) else None
                manager_email = EMAIL_RE.search(mgr_block).group(0) if EMAIL_RE.search(mgr_block) else None
                # manager name is usually first line, before phone
                manager_name = clean(mgr_block.split("(")[0])

            conn.execute(
                text("""
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
                """),
                {
                    "property_name": property_name,
                    "address": address,
                    "vacancies_text": vac_block,
                    "manager_name": manager_name,
                    "manager_phone": manager_phone,
                    "manager_email": manager_email,
                    "updated_date": updated,
                    "source_pdf_url": PDF_URL,
                }
            )
            inserted += 1

    print(f"Inserted or updated {inserted} vacancy rows (updated_date={updated})")

if __name__ == "__main__":
    main()
