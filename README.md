# Affordable Housing Aggregator for Missoula, Montana

## Overview
Missoula has a limited and fragmented rental housing market, especially for affordable one-bedroom units. Listings are spread across multiple property management websites, PDFs, and independent platforms, making it difficult for renters or housing organizations to see what is actually available at any given time.

This project builds a centralized data pipeline and dashboard that aggregates rental listings from multiple sources into one place. The goal is to provide a clear, real-time view of available housing options in Missoula.

---

## Why this project matters
Right now, finding housing in Missoula often means checking multiple websites, PDFs, and listings manually. This is time consuming and makes it difficult for individuals and organizations to understand what is truly available.

This project simplifies that process by bringing everything into one place. It is designed to support renters, housing advocates, and local organizations by making housing data easier to access and understand.

---

## Current data sources
- Missoula Property Management
- Missoula Housing Authority (Vacancy PDF)
- Caras Property Management
- Plum Property Management
- Craigslist (planned)
- Adea Property Management 
---

## System architecture
Raw → Staging → Views → Dashboard

- Raw layer stores original scraped data  
- Staging layer standardizes and cleans data  
- Views provide query-ready datasets  
- Dashboard presents the final results to users  

---

## Repository structure
- scripts/ – Python ETL pipelines for each data source  
- sql/ – SQL queries, views, and analysis  
- data/ – Sample outputs and example data  
- docs/ – Project documentation and diagrams  
- archive/ – Older or unused code  

---

## Tech stack
- Python  
- PostgreSQL (Supabase)  
- SQLAlchemy (engine + text, no ORM)  
- BeautifulSoup (HTML parsing)  
- pdfplumber (PDF parsing)  
- python-dotenv (environment variables)  

---

## How to run

1. Clone the repository  
2. Create a virtual environment  
   ```python
   python -m venv venv
   ```
3. Activate the environment  
   ```python
   venv\Scripts\activate
   ```
4. Install dependencies  
   ```python
   pip install -r requirements.txt
   ```
5. Set environment variables in a `.env` file  
   ```python
   DATABASE_URL=your_connection_string
   ```
6. Run database setup scripts  
7. Run ingestion scripts inside `scripts/ingestion/`  
8. Run SQL queries in `sql/analysis/` to view results  
 

---

## Project status

### Current
- Multiple sources integrated into a single normalized table  
- Raw to Staging pipeline implemented  
- Data is queryable across all sources  
- Demo queries available for dashboard use  

### Next steps
- Improve address extraction and standardization  
- Implement deduplication logic  
- Add additional data sources  
- Integrate Craigslist  
- Build final dashboard (likely Looker Studio)  

---

## Author
Parker Munsey  
University of Montana  
MSBA Capstone Project
