# Amazon Egypt Retail Scraper & ETL Pipeline

This repository contains an end-to-end Extract, Transform, Load (ETL) pipeline that scrapes near real-time retail data from Amazon Egypt (`amazon.eg`), cleans and transforms it with `pandas`, and loads it into PostgreSQL and Snowflake. The pipeline is orchestrated and scheduled with Apache Airflow and runs through Docker Compose.

## Project Overview

The project demonstrates a data engineering workflow for collecting product search data without relying on an external API. It uses `requests` and `BeautifulSoup` with User-Agent rotation to fetch live Amazon Egypt search results for categories such as laptops, smartphones, headphones, and televisions.

## Key Features

- Web scraping for ASINs, titles, prices, ratings, review counts, image URLs, and product URLs.
- Data transformation with `pandas` for numeric price, rating, and review fields.
- PostgreSQL warehouse modeled with `dim_products` and `fact_product_snapshots`.
- Optional Snowflake warehouse load using the same dimensional model.
- Upsert loading for product dimension records and snapshot inserts for price/rating history.
- Data quality validation before loading warehouse tables.
- Automated warehouse freshness checks after loading.
- Analytical SQL queries for business insights.
- Power BI dashboard guide for core metrics and trends.
- Airflow DAG for scheduled extract, transform, and load tasks.
- Docker Compose setup for PostgreSQL, Airflow webserver, scheduler, and initialization.

## Limitations

Amazon uses strict bot protection. This scraper is intended for education and portfolio demonstration. It includes delays and User-Agent rotation, but it may still receive 503/CAPTCHA responses. A successful run is expected to collect a limited sample rather than a large catalog.

## Technology Stack

- Python 3.9+
- pandas
- requests
- BeautifulSoup
- SQLAlchemy
- PostgreSQL 13
- Snowflake
- Apache Airflow 2.8+
- Docker and Docker Compose

## Project Structure

```text
retail-data-warehouse-etl/
|-- dags/                        # Airflow DAGs
|   `-- amazon_eg_etl_dag.py
|-- src/                         # Core Python modules
|   |-- extract.py               # Amazon EG web scraper logic
|   |-- transform.py             # Data cleaning logic
|   |-- load.py                  # PostgreSQL insertion/upsert logic
|   `-- load_snowflake.py        # Optional Snowflake insertion/upsert logic
|-- sql/                         # SQL scripts
|   |-- create_tables.sql        # DDL for the data warehouse
|   |-- analytical_queries.sql    # Insight queries for Snowflake/BI
|   `-- init_db.sh               # PostgreSQL initialization script
|-- docs/                        # Dashboard and usage documentation
|   |-- system_check_report.md
|   |-- snowflake_setup_guide.md
|   `-- powerbi_dashboard_guide.md
|-- config/                      # Configuration files
|-- data/                        # Local storage for intermediate CSV/JSON files
|-- docker-compose.yml           # Docker services configuration
|-- requirements.txt             # Python dependencies
|-- .env.example                 # Example environment variables
`-- .env                         # Local environment variables, not committed
```

## Setup

### Prerequisites

- Docker
- Docker Compose

### 1. Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` if you want different local credentials.

Data quality and freshness controls:

```env
DQ_MIN_ROWS=1
DATA_FRESHNESS_MAX_HOURS=30
```

`DQ_MIN_ROWS` prevents empty or blocked scrape runs from silently loading. `DATA_FRESHNESS_MAX_HOURS` controls the maximum allowed warehouse data age after the load step.

To enable Snowflake loading, set `SNOWFLAKE_ENABLED=true` and fill in these values:

```env
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DW
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=
```

`SNOWFLAKE_ACCOUNT` is the account identifier from your Snowflake URL, for example the part before `.snowflakecomputing.com`.

Full Snowflake setup and load instructions are available in `docs/snowflake_setup_guide.md`.

### 2. Start the Services

```bash
docker-compose up -d
```

This starts PostgreSQL, creates the `retail_dw` warehouse schema, initializes Airflow, and runs the Airflow webserver and scheduler. If Snowflake is enabled, the DAG creates the Snowflake tables automatically during the load step.

## Running the Pipeline

1. Open the Airflow UI at `http://localhost:8080`.
2. Log in with the default credentials from `.env.example`: `admin` / `admin`.
3. Find the `amazon_eg_etl` DAG.
4. Unpause the DAG.
5. Trigger the DAG manually or wait for the daily schedule.

## Output

The scraper writes raw JSON into `data/`, the transform step writes cleaned CSV output into the same folder, and the load steps insert the cleaned records into PostgreSQL and Snowflake when enabled.

## Data Quality and Freshness

The Airflow DAG includes:

- `validate_clean_product_data`: checks required columns, minimum row count, nulls in critical fields, invalid prices, invalid ratings, and invalid review counts.
- `check_warehouse_freshness`: verifies that warehouse snapshots were updated recently after the load step.

The data quality task writes `data/data_quality_report.json` for auditability.

The latest local system validation summary is available in `docs/system_check_report.md`.

## Analytics and Dashboard

Useful SQL queries are available in `sql/analytical_queries.sql`, including KPI summary, category performance, price trends, top reviewed products, price movement, and freshness monitoring.

Power BI dashboard setup is documented in `docs/powerbi_dashboard_guide.md`. Use Snowflake as the main live dashboard source and connect `DIM_PRODUCTS` to `FACT_PRODUCT_SNAPSHOTS` by `PRODUCT_ID`.

## License

This project is licensed under the MIT License.
