import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

CREATE_VIEWS_SQL = """

-- View: MPM available units only
CREATE OR REPLACE VIEW vw_mpm_available_units AS
SELECT
  id,
  listing_title,
  available_text,
  beds,
  baths,
  sqft,
  rent,
  details_url,
  apply_url,
  scraped_at
FROM mpm_listings
WHERE listing_title IS NOT NULL
ORDER BY rent NULLS LAST, beds NULLS LAST, listing_title;


-- Unified view: All current listings from all sources
CREATE OR REPLACE VIEW vw_all_current_listings AS

-- Missoula Property Management
SELECT
  'MPM' AS source,
  listing_title AS title,
  beds,
  baths,
  sqft,
  rent,
  available_text AS availability,
  details_url,
  apply_url,
  scraped_at AS as_of
FROM mpm_listings

UNION ALL

-- Missoula Housing Authority
SELECT
  'MHA' AS source,
  property_name AS title,
  NULL::integer AS beds,
  NULL::numeric AS baths,
  NULL::integer AS sqft,
  NULL::numeric AS rent,
  vacancies_text AS availability,
  NULL::text AS details_url,
  NULL::text AS apply_url,
  updated_date::timestamp AS as_of
FROM mha_vacancy_board;

"""

def main():
    with engine.begin() as conn:
        conn.execute(text(CREATE_VIEWS_SQL))
    print("Views created successfully:")
    print("- vw_mpm_available_units")
    print("- vw_all_current_listings")

if __name__ == "__main__":
    main()
