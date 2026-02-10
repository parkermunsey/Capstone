import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT property_name, vacancies_text, manager_email, updated_date
        FROM mha_vacancy_board
        ORDER BY updated_date DESC NULLS LAST, property_name
    """)).fetchall()

for r in rows:
    print(r)
