import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

schema_sql = """
CREATE TABLE IF NOT EXISTS listings_mvp (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    listing_url TEXT UNIQUE NOT NULL,
    price_monthly INTEGER,
    bedrooms NUMERIC(2,1),
    neighborhood TEXT,
    city TEXT DEFAULT 'Missoula',
    pets_allowed BOOLEAN,
    available_date DATE,
    date_collected TIMESTAMP DEFAULT NOW()
);
"""

with engine.begin() as conn:
    conn.execute(text(schema_sql))
    print("✅ listings_mvp table created")
