# Retail Data Warehouse ETL - Amazon Egypt

An end-to-end data engineering project that extracts near real-time retail product data from Amazon Egypt (`amazon.eg`) using web scraping, cleans and transforms it with `pandas`, and loads it into PostgreSQL and Snowflake data warehouses.

The full workflow is orchestrated and scheduled with Apache Airflow. Prefect is not used in this version.

## Overview

This project demonstrates a complete ETL pipeline for retail analytics without relying on a public API. The scraper collects Amazon Egypt product search results, the transformation layer standardizes the raw data, and the loading layer persists product dimensions and historical product snapshots for analytics and dashboarding.

The pipeline includes automated execution, Airflow logging, data quality checks, warehouse freshness validation, analytical SQL queries, and Power BI dashboard guidance.

## Architecture

```text
Amazon Egypt Search Pages
        |
        v
Python Scraper (requests + BeautifulSoup)
        |
        v
Raw JSON Files in data/
        |
        v
pandas Transformation
        |
        v
Data Quality Validation
        |
        +--------------------+
        |                    |
        v                    v
PostgreSQL DW          Snowflake DW
        |                    |
        +---------+----------+
                  v
          SQL Analytics / Power BI
```

## Key Features

- Scrapes product listings from Amazon Egypt for SKU/ASIN, title, price, rating, review count, product URL, image URL, brand/category, and platform.
- Cleans and standardizes raw product data using `pandas`.
- Removes duplicate products within each batch before warehouse loading.
- Loads dimensional warehouse tables into PostgreSQL.
- Loads the same warehouse model into Snowflake.
- Uses upsert logic for product dimension records.
- Inserts historical snapshot facts for price, rating, and review tracking.
- Runs data quality validation before loading.
- Runs freshness checks after warehouse loading.
- Provides analytical SQL queries for business insights.
- Includes Power BI dashboard setup guidance.
- Uses Apache Airflow for scheduling, orchestration, retries, and task logging.

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
|   |-- assets/
|   |   |-- dim_products.png
|   |   `-- fact_product_snapshots.png
|   |-- snowflake_setup_guide.md
|   |-- powerbi_dashboard_guide.md
|   `-- system_check_report.md
|-- config/
|-- data/
|   `-- .gitkeep
|-- docker-compose.yml
|-- requirements.txt
|-- .env.example
`-- .gitignore
```

## Data Model

The warehouse uses a simple dimensional model designed for retail product monitoring.

### `DIM_PRODUCTS`

Stores one row per platform/product SKU.

Main fields:

- `PRODUCT_ID`
- `PLATFORM`
- `SKU`
- `TITLE`
- `BRAND`
- `PRODUCT_URL`
- `IMAGE_URL`
- `CREATED_AT`
- `UPDATED_AT`

### `FACT_PRODUCT_SNAPSHOTS`

Stores historical observations for each product.

Main fields:

- `SNAPSHOT_ID`
- `PRODUCT_ID`
- `PRICE`
- `RATING`
- `REVIEW_COUNT`
- `SNAPSHOT_DATE`
- `SNAPSHOT_TIMESTAMP`

This model supports price trend analysis, category comparisons, product monitoring, freshness checks, and dashboard reporting.

## Snowflake Warehouse Preview

### `DIM_PRODUCTS`

![DIM_PRODUCTS table preview](docs/assets/dim_products.png)

### `FACT_PRODUCT_SNAPSHOTS`

![FACT_PRODUCT_SNAPSHOTS table preview](docs/assets/fact_product_snapshots.png)

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

Airflow handles scheduling, task logs, retries, and run history.

## Setup

### 1. Create the Environment File

Copy the example environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

### 2. Configure PostgreSQL

PostgreSQL is preconfigured for Docker Compose:

```env
DW_CONN_STR=postgresql+psycopg2://dw_user:dw_pass@postgres/retail_dw
```

### 3. Configure Data Quality and Freshness

```env
DQ_MIN_ROWS=1
DATA_FRESHNESS_MAX_HOURS=30
```

`DQ_MIN_ROWS` prevents empty or blocked scrape runs from loading silently. `DATA_FRESHNESS_MAX_HOURS` controls the maximum accepted data age after loading.

### 4. Configure Snowflake

To enable Snowflake loading:

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

`SNOWFLAKE_ACCOUNT` is the account identifier from your Snowflake URL. For example, if the URL is:

```text
https://abc12345.us-east-1.snowflakecomputing.com
```

Use:

```env
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
```

Full Snowflake setup and load instructions are available in [docs/snowflake_setup_guide.md](docs/snowflake_setup_guide.md).

### 5. Start Services

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

Default local credentials:

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

## Pipeline Output

The scraper writes raw JSON files to `data/`. The transformation step writes cleaned CSV files to `data/`. The load steps insert cleaned records into PostgreSQL and Snowflake when Snowflake is enabled.

## Data Quality

The `validate_clean_product_data` task checks:

- Required columns exist.
- Row count is above `DQ_MIN_ROWS`.
- Critical fields are not null.
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

## Analytical SQL

Analytical SQL queries are available in [sql/analytical_queries.sql](sql/analytical_queries.sql).

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

Connect `DIM_PRODUCTS` to `FACT_PRODUCT_SNAPSHOTS` by `PRODUCT_ID`.

Dashboard setup, relationships, and DAX measures are documented in [docs/powerbi_dashboard_guide.md](docs/powerbi_dashboard_guide.md).

## Important Notes

- Amazon may return 503/CAPTCHA responses because of bot protection.
- The scraper is intended for education and portfolio demonstration.
- Empty scrape results are blocked by the data quality task instead of being loaded silently.
- `.env` is intentionally ignored by Git and must not be committed.
- `.env.example` contains placeholders only.
- Rotate Snowflake credentials if they are ever exposed publicly.

## System Check

The latest local validation summary is available in [docs/system_check_report.md](docs/system_check_report.md).

## License

This project is licensed under the MIT License.
