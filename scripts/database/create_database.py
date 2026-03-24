from sqlalchemy import create_engine, text

# Connect to the DEFAULT postgres database
ADMIN_DB_URL = "postgresql+psycopg://postgres:MasonJar!123@localhost:5432/postgres"

CAPSTONE_DB_NAME = "affordable_housing_missoula"

engine = create_engine(ADMIN_DB_URL, isolation_level="AUTOCOMMIT")

with engine.connect() as conn:
    conn.execute(
        text(f"CREATE DATABASE {CAPSTONE_DB_NAME}")
    )
    print(f"✅ Database '{CAPSTONE_DB_NAME}' created successfully")
