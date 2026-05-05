# Retail Data Warehouse ETL - Amazon Egypt

An end-to-end data engineering project that extracts near real-time retail product data from Amazon Egypt (`amazon.eg`) using web scraping, cleans and transforms it with `pandas`, and loads it into PostgreSQL and Snowflake data warehouses. The workflow is orchestrated and scheduled with Apache Airflow.

## Overview

This project demonstrates a production-style ETL pipeline for retail analytics without relying on a public API. The scraper collects product search results from Amazon Egypt, the transformation layer standardizes the raw data, and the load layer persists historical product snapshots for analysis and dashboarding.

The pipeline includes data quality checks, warehouse freshness validation, analytical SQL queries, and a Power BI dashboard guide.

## Architecture

```text
Amazon Egypt Search Pages
        |
        v
Python Scraper (requests + BeautifulSoup)
        |
        v
Raw JSON in data/
        |
        v
pandas Transformation + Data Quality Checks
        |
        +--------------------+
        |                    |
        v                    v
PostgreSQL DW          Snowflake DW
        |                    |
        +---------+----------+
                  v
          Analytical SQL / Power BI
```

## Features

- Scrapes Amazon Egypt product listings for ASIN, title, price, rating, review count, image URL, product URL, and category.
- Cleans and standardizes product data using `pandas`.
- Removes duplicate products within each batch before warehouse loading.
- Loads product dimensions and snapshot facts into PostgreSQL.
- Loads the same dimensional model into Snowflake.
- Uses Airflow to orchestrate extraction, transformation, validation, loading, and freshness checks.
- Includes data quality validation before loading.
- Includes warehouse freshness checks after loading.
- Provides analytical SQL queries for business insights.
- Provides Power BI dashboard guidance for metrics and trends.

## Technology Stack

- Python
- pandas
- requests
- BeautifulSoup
- SQLAlchemy
- PostgreSQL 13
- Snowflake
- Apache Airflow 2.8
- Docker Compose
- Power BI

## Project Structure

```text
retail-data-warehouse-etl/
|-- dags/
|   `-- amazon_eg_etl_dag.py
|-- src/
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   |-- load_snowflake.py
|   `-- data_quality.py
|-- sql/
|   |-- create_tables.sql
|   |-- analytical_queries.sql
|   `-- init_db.sh
|-- docs/
|   |-- snowflake_setup_guide.md
|   |-- powerbi_dashboard_guide.md
|   `-- system_check_report.md
|-- config/
|-- data/
|-- docker-compose.yml
|-- requirements.txt
|-- .env.example
`-- .gitignore
```

## Data Model

The warehouse uses a simple dimensional model:

- `DIM_PRODUCTS`: one row per platform/product SKU.
- `FACT_PRODUCT_SNAPSHOTS`: historical observations of price, rating, and review count.

This design supports trend analysis, price movement tracking, category comparisons, and dashboard freshness monitoring.

## Airflow DAG

DAG name:

```text
amazon_eg_etl
```

Task flow:

```text
scrape_amazon_eg_data
    -> transform_amazon_eg_data
    -> validate_clean_product_data
    -> [load_amazon_eg_data_to_postgres, load_amazon_eg_data_to_snowflake]
    -> check_warehouse_freshness
```

## Setup

### 1. Create Environment File

Copy the example environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

### 2. Configure Environment Variables

PostgreSQL is preconfigured for Docker Compose:

```env
DW_CONN_STR=postgresql+psycopg2://dw_user:dw_pass@postgres/retail_dw
```

Data quality and freshness controls:

```env
DQ_MIN_ROWS=1
DATA_FRESHNESS_MAX_HOURS=30
```

Snowflake configuration:

```env
SNOWFLAKE_ENABLED=true
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DW
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=
```

`SNOWFLAKE_ACCOUNT` is the account identifier from the Snowflake URL. Example:

```text
https://abc12345.us-east-1.snowflakecomputing.com
```

Use:

```env
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
```

Full Snowflake instructions are available in [docs/snowflake_setup_guide.md](docs/snowflake_setup_guide.md).

### 3. Start Services

```bash
docker compose up -d
```

This starts:

- PostgreSQL
- Airflow webserver
- Airflow scheduler
- Airflow initialization service

Airflow UI:

```text
http://localhost:8080
```

Default credentials:

```text
admin / admin
```

## Running the Pipeline

From the Airflow UI:

1. Open `http://localhost:8080`.
2. Log in with `admin / admin`.
3. Open the `amazon_eg_etl` DAG.
4. Unpause the DAG.
5. Trigger it manually or wait for the scheduled run.

From the terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger amazon_eg_etl
```

Check task states:

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run amazon_eg_etl <dag_run_id>
```

## Data Quality

The `validate_clean_product_data` task checks:

- Required columns exist.
- Row count is above `DQ_MIN_ROWS`.
- Required fields are not null.
- Prices are not negative.
- Ratings are between 0 and 5.
- Review counts are not negative.
- Duplicate `platform`/`sku` rows are removed during transformation.

The latest quality report is written to:

```text
data/data_quality_report.json
```

## Freshness Checks

The `check_warehouse_freshness` task verifies that warehouse snapshots were updated recently. The maximum allowed age is controlled by:

```env
DATA_FRESHNESS_MAX_HOURS=30
```

## Snowflake Validation

After a successful run, validate Snowflake with:

```sql
USE DATABASE RETAIL_DW;
USE SCHEMA PUBLIC;

SELECT COUNT(*) AS product_count
FROM DIM_PRODUCTS;

SELECT COUNT(*) AS snapshot_count
FROM FACT_PRODUCT_SNAPSHOTS;

SELECT MAX(SNAPSHOT_TIMESTAMP) AS latest_snapshot
FROM FACT_PRODUCT_SNAPSHOTS;
```

## Analytics

Analytical SQL queries are available in:

```text
sql/analytical_queries.sql
```

Included query themes:

- Core KPI summary
- Average price and rating by category
- Daily price trends
- Top reviewed products
- Largest observed price changes
- Warehouse freshness monitoring

## Power BI Dashboard

Use Snowflake as the primary dashboard source.

Recommended tables:

- `RETAIL_DW.PUBLIC.DIM_PRODUCTS`
- `RETAIL_DW.PUBLIC.FACT_PRODUCT_SNAPSHOTS`

Dashboard setup, relationships, and DAX measures are documented in:

```text
docs/powerbi_dashboard_guide.md
```

## Important Notes

- Amazon may return 503/CAPTCHA responses because of bot protection.
- Empty scrape results are blocked by the data quality task instead of being loaded silently.
- `.env` is intentionally ignored by Git and must not be committed.
- `.env.example` contains placeholders only.
- Snowflake credentials should be rotated if they are ever shared publicly.

## System Check

The latest local validation summary is available in:

```text
docs/system_check_report.md
```

## License

This project is licensed under the MIT License.
