import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

sql = """
CREATE TABLE IF NOT EXISTS mha_properties (
  id BIGSERIAL PRIMARY KEY,
  property_name TEXT NOT NULL,
  address TEXT,
  city TEXT DEFAULT 'Missoula',
  details_url TEXT UNIQUE,
  allows_cats BOOLEAN,
  allows_dogs BOOLEAN,
  senior_55_plus BOOLEAN,
  source_url TEXT NOT NULL,
  last_seen TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mha_vacancy_board (
  id BIGSERIAL PRIMARY KEY,
  property_name TEXT NOT NULL,
  address TEXT,
  vacancies_text TEXT,
  manager_name TEXT,
  manager_phone TEXT,
  manager_email TEXT,
  updated_date DATE,
  source_pdf_url TEXT NOT NULL,
  ingested_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(property_name, source_pdf_url, updated_date)
);
"""

with engine.begin() as conn:
    conn.execute(text(sql))

print("mha_properties and mha_vacancy_board tables created")

