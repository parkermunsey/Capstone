<h1 align="center">Affordable Housing Aggregator</h1>
<h3 align="center">Missoula, Montana</h3>

<p align="center">
  <img src="housing_logo.jpg" alt="Housing Logo" width="200"/>
</p>

<p align="center">
  <em>
    Project by Parker Munsey<br>
    University of Montana MSBA Capstone
  </em>
</p>

<hr>

## Project Overview

Missoula has a limited and fragmented rental housing market, especially for affordable one-bedroom units. Rental listings are spread across multiple property management websites, PDFs, and independent platforms, making it difficult to understand what housing is actually available at any given time.

This project builds a centralized data pipeline and dashboard that aggregates rental listings from multiple sources into a single, queryable system. The goal is to provide a clear and up-to-date view of available housing options for renters, housing organizations, and local stakeholders.

---

## Why This Project Matters

Currently, finding housing in Missoula requires manually checking multiple websites and documents. This process is time-consuming and often incomplete.

This project addresses that problem by:

- Centralizing rental listings across multiple sources  
- Standardizing inconsistent data into a unified format  
- Enabling real-time visibility into housing availability  

The result is a system that improves accessibility, transparency, and decision-making around housing.

---

## Data Sources

The pipeline currently integrates data from multiple sources:

- Missoula Property Management  
- Missoula Housing Authority (Vacancy PDF)  
- Caras Property Management  
- Plum Property Management  
- ADEA Property Management  
- Craigslist 

Each source has a different structure and level of data quality, requiring custom ingestion and normalization logic.

---

## System Architecture

The project follows a structured data engineering workflow:

**Raw → Staging → Views → Dashboard**

- **Raw Layer**  
  Stores append-only scraped data exactly as collected from each source  

- **Staging Layer**  
  Cleans and standardizes data into a consistent schema across all sources  

- **Views Layer**  
  Provides query-ready datasets for analysis and visualization  

- **Dashboard Layer**  
  Presents filtered and aggregated housing data to end users  

This modular design allows for scalable ingestion and easier debugging across sources.

---

## Data Pipeline

The system uses Python-based ETL pipelines to ingest and transform data:

- Each source has a dedicated ingestion script  
- Data is stored in a centralized PostgreSQL database (Supabase)  
- A shared normalization script standardizes all sources into a single staging table  
- Data is prepared for downstream analytics and dashboarding  

This approach ensures consistency while allowing flexibility for source-specific parsing.

---
## How to Run

### 1. Install dependencies
pip install -r requirements.txt

### 2. Add environment variables
Create a `.env` file with:
DATABASE_URL=your_database_url_here

### 3. Run ingestion
python scripts/ingestion/run_all_ingestion.py

### 4. Normalize into staging
python scripts/staging/normalize_raw_to_stg.py --source all --lookback-days 14

### 5. Launch dashboard
streamlit run scripts/dashboard/app.py

## Repository Structure

scripts/
ingestion/ # Source-specific ETL pipelines
staging/ # Shared normalization logic

sql/
views/ # Query-ready datasets
analysis/ # Exploration and validation queries

data/ # Sample outputs and test data
docs/ # Diagrams and documentation
archive/ # Deprecated or experimental code


---

## Technologies Used

**Data Processing**
- Python  
- BeautifulSoup (HTML parsing)  
- pdfplumber (PDF parsing)  
- SQLAlchemy (database interaction)  

**Database**
- PostgreSQL (Supabase)

**Development Tools**
- python-dotenv (environment management)  
- Git / GitHub  

**Visualization (Planned)**
- Looker Studio or Python-based dashboard (Dash / Streamlit)

---

## How It Works

1. Scrape rental listings from multiple sources  
2. Store raw data in `raw_listings` (append-only)  
3. Normalize data into `stg_listings` using shared logic  
4. Create views for filtering and aggregation  
5. Build a dashboard for end-user interaction  

---

## Project Status

### Current Progress

- Multiple sources successfully integrated  
- Raw → Staging pipeline implemented  
- Data standardized into a shared schema  
- Cross-source querying enabled  

---

## Author

**Parker Munsey**  
University of Montana  
Master of Science in Business Analytics (MSBA)  
