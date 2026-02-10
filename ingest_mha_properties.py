import os
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

PAGE_URL = "https://www.missoulahousing.org/affordable-housing"
BASE = "https://www.missoulahousing.org"

HEADERS = {
    "User-Agent": "UMCapstoneHousing/0.1 (academic project; contact: parker.munsey)",
}

def norm_bool_from_text(s: str, keyword: str) -> bool | None:
    if not s:
        return None
    t = s.lower()
    if keyword.lower() in t:
        return True
    return None

def main():
    r = requests.get(PAGE_URL, headers=HEADERS, timeout=25)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    cards = soup.select("h1, h2, h3, h4, a")
    # The page is Squarespace and structure changes. We use the reliable “Read More” links.
    read_more_links = []
    for a in soup.select("a[href]"):
        txt = (a.get_text(" ", strip=True) or "").strip()
        href = a.get("href", "").strip()
        if "read more" in txt.lower() and href:
            read_more_links.append(urljoin(BASE, href))

    read_more_links = list(dict.fromkeys(read_more_links))
    if not read_more_links:
        print("No Read More links found. The page layout may have changed.")
        return

    inserted = 0

    with engine.begin() as conn:
        for url in read_more_links[:30]:
            time.sleep(1.2)
            pr = requests.get(url, headers=HEADERS, timeout=25)
            pr.raise_for_status()
            psoup = BeautifulSoup(pr.text, "html.parser")

            title = (psoup.select_one("h1") or psoup.select_one("h2"))
            name = title.get_text(" ", strip=True) if title else url.split("/")[-1]

            page_text = psoup.get_text(" ", strip=True)

            address = None
            m = re.search(r"\b\d{1,5}\s+[A-Za-z0-9\.\- ]+\b", page_text)
            if m:
                address = m.group(0).strip()

            allows_cats = True if "cats allowed" in page_text.lower() else None
            allows_dogs = True if "dogs allowed" in page_text.lower() else None
            senior = True if "55" in page_text and "senior" in page_text.lower() else None

            conn.execute(
                text("""
                    INSERT INTO mha_properties
                      (property_name, address, details_url, allows_cats, allows_dogs, senior_55_plus, source_url, last_seen)
                    VALUES
                      (:name, :address, :details_url, :cats, :dogs, :senior, :source_url, NOW())
                    ON CONFLICT (details_url) DO UPDATE SET
                      property_name = EXCLUDED.property_name,
                      address = COALESCE(EXCLUDED.address, mha_properties.address),
                      allows_cats = COALESCE(EXCLUDED.allows_cats, mha_properties.allows_cats),
                      allows_dogs = COALESCE(EXCLUDED.allows_dogs, mha_properties.allows_dogs),
                      senior_55_plus = COALESCE(EXCLUDED.senior_55_plus, mha_properties.senior_55_plus),
                      last_seen = NOW();
                """),
                {
                    "name": name,
                    "address": address,
                    "details_url": url,
                    "cats": allows_cats,
                    "dogs": allows_dogs,
                    "senior": senior,
                    "source_url": PAGE_URL,
                }
            )
            inserted += 1

    print(f"Ingested {inserted} property detail pages into mha_properties")

if __name__ == "__main__":
    main()
