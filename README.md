# MarketPulse ETL
### Retail Intelligence Data Pipeline

An end-to-end data engineering project that extracts near real-time retail product data from Amazon Egypt (`amazon.eg`) using web scraping, cleans and transforms it with `pandas`, and loads it into PostgreSQL and Snowflake data warehouses.

The full workflow is orchestrated and scheduled with Apache Airflow. Prefect is not used in this version.

## Overview

This project demonstrates a complete ETL pipeline for retail analytics without relying on a public API. The scraper collects Amazon Egypt product search results, the transformation layer standardizes the raw data, and the loading layer persists product dimensions and historical product snapshots for analytics and dashboarding.

The pipeline includes automated execution, Airflow logging, data quality checks, warehouse freshness validation, analytical SQL queries, and Power BI dashboard guidance.

<img width="2400" height="1400" alt="etl-overview" src="https://github.com/user-attachments/assets/57c32d74-9fb7-4468-a47d-431b58bcc8a9" />


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
<img width="2200" height="2600" alt="etl-architecture" src="https://github.com/user-attachments/assets/357a1a40-7663-46f0-b3d6-d5323c7cb6be" />


## Key Features

- Scrapes Amazon Egypt listing pages and optionally enriches products from detail pages.
- Collects SKU/ASIN, title, product name, brand, device type, prices, discount, rating, seller, availability, and technical specs.
- Cleans and standardizes raw product data using `pandas`.
- Creates a compact `Product Name` field from the first useful words in the full title, trimming trailing prepositions, conjunctions, and standalone numbers.
- Runs local AI-style semantic validation to detect column mismatches, such as a device type being loaded as a brand.
- Removes duplicate products within each batch before warehouse loading.
- Loads dimensional warehouse tables into PostgreSQL.
- Loads the same warehouse model into Snowflake.
- Uses upsert logic for product dimension records.
- Backfills `PRODUCT_NAME` for existing warehouse rows when the naming rule changes.
- Inserts historical snapshot facts for price, discount, rating, seller, and availability tracking.
- Runs data quality validation before loading.
- Runs freshness checks after warehouse loading.
- Provides analytical SQL queries for business insights.
- Includes a completed Power BI dashboard guide with report screenshots and business insights.
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
|   |-- ai_validation.py
|   |-- load.py
|   |-- load_snowflake.py
|   `-- data_quality.py
|-- sql/
|   |-- create_tables.sql
|   |-- analytical_queries.sql
|   `-- init_db.sh
|-- docs/
|   |-- assets/
|   |   `-- powerbi/
|   |-- snowflake_setup_guide.md
|   |-- powerbi_dashboard_guide.md
|   `-- system_check_report.md
|-- notebooks/
|   `-- amazon_eg_eda.ipynb
|-- config/
|-- data/
|   `-- .gitkeep
|-- run_etl.py
|-- docker-compose.yml
|-- requirements.txt
|-- .env.example
`-- .gitignore
```

## Data Model

The warehouse uses a simple dimensional model designed for retail product monitoring.

### `DIM_PRODUCTS`

Stores one row per product SKU.

Main fields:

- `PRODUCT_ID`
- `SKU`
- `TITLE`
- `PRODUCT_NAME`
- `BRAND`
- `DEVICE_TYPE`
- `MODEL_NUMBER`
- `COLOR`
- `SCREEN_SIZE`
- `RAM_MEMORY`
- `STORAGE_CAPACITY`
- `PROCESSOR`
- `GPU`
- `OPERATING_SYSTEM`
- `DISPLAY_RESOLUTION`
- `CONNECTIVITY`
- `PRODUCT_DIMENSIONS`
- `ITEM_WEIGHT`
- `PRODUCT_URL`
- `IMAGE_URL`
- `CREATED_AT`
- `UPDATED_AT`

### `FACT_PRODUCT_SNAPSHOTS`

Stores historical price and rating observations for each product.

Main fields:

- `SNAPSHOT_ID`
- `PRODUCT_ID`
- `PRICE`
- `CURRENCY`
- `ORIGINAL_PRICE`
- `DISCOUNT_PERCENT`
- `RATING`
- `AVAILABILITY`
- `SELLER`
- `SNAPSHOT_DATE`
- `SNAPSHOT_TIMESTAMP`

This model supports price trend analysis, category comparisons, product monitoring, freshness checks, and dashboard reporting.

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

### 4. Configure Scraping Depth

```env
SCRAPE_SEARCH_TERMS=laptop,tablet,ipad,android tablet,samsung tablet,lenovo tablet,huawei tablet,xiaomi tablet,smartphone,mobile phone,android phone,iphone,samsung phone,xiaomi phone,redmi phone,oppo phone,realme phone,infinix phone,tecno phone,nokia phone,honor phone,huawei phone,oneplus phone,feature phone,foldable phone
SCRAPE_SEARCH_PAGES=2
SCRAPE_DETAIL_PAGES=true
SCRAPE_DETAIL_LIMIT_PER_RUN=300
SCRAPE_DETAIL_DELAY_MIN_SECONDS=2
SCRAPE_DETAIL_DELAY_MAX_SECONDS=5
SCRAPE_MERGE_WITH_EXISTING_RAW=true
```

Detail-page enrichment extracts brand, manufacturer, model, color, memory, storage, display, seller, availability, list price, and discount fields. Listing pages are collected before detail pages so broad mobile/tablet searches are not blocked by early deep enrichment. `SCRAPE_DETAIL_LIMIT_PER_RUN` caps new detail pages per run so broad searches can finish without over-hitting Amazon. Detail results are cached across runs, and `SCRAPE_MERGE_WITH_EXISTING_RAW=true` prevents a blocked scrape from replacing a richer previous raw dataset with a smaller one. The delay controls reduce the risk of Amazon blocking the scraper.

### 5. Configure Snowflake

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
SNOWFLAKE_FAIL_ON_ERROR=false
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

By default, temporary Snowflake connection failures are logged and skipped so the PostgreSQL load can still complete. Set `SNOWFLAKE_FAIL_ON_ERROR=true` if Snowflake should fail the pipeline.

### 6. Start Services

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

The pipeline can be run in two ways:

- Airflow, recommended for scheduled warehouse refreshes.
- `run_etl.py`, useful for local development, debugging, or one-off refreshes.

Before running either option, make sure `.env` exists and contains the PostgreSQL and optional Snowflake settings described above.

### Option A: Run with Airflow

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

The DAG runs this flow:

```text
scrape_amazon_eg_data
    -> transform_amazon_eg_data
    -> validate_clean_product_data
    -> [load_amazon_eg_data_to_postgres, load_amazon_eg_data_to_snowflake]
    -> check_warehouse_freshness
```

### Option B: Run Locally with `run_etl.py`

Create and activate a Python environment, then install dependencies:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run the full pipeline:

```powershell
python run_etl.py
```

Reuse the existing raw JSON and rerun transform, validation, and load:

```powershell
python run_etl.py --skip-scrape
```

Validate and load the existing clean CSV only:

```powershell
python run_etl.py --load-only
```

`run_etl.py` executes the same core stages as the Airflow DAG:

1. Extract Amazon Egypt product data into `data/raw_amazon_eg_products.json`.
2. Transform the raw JSON into `data/clean_amazon_eg_products.csv`.
3. Validate required fields, row counts, ratings, prices, product names, and duplicates.
4. Load PostgreSQL and Snowflake when Snowflake is enabled.

### Useful Validation Commands

Check generated local files:

```powershell
Get-ChildItem data
```

Check the latest data quality report:

```powershell
Get-Content data\data_quality_report.json
```

Validate Snowflake after a successful load:

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

## Pipeline Output

The scraper writes raw JSON files to `data/`. The transformation step writes cleaned CSV files to `data/` with these columns only:

```text
sku,title,Product Name,brand,price,original_price,discount_percent,rating,availability,seller,product_url,image_url,Device type,model_number,color,screen_size,ram_memory,storage_capacity,processor,gpu,operating_system,display_resolution,connectivity,product_dimensions,item_weight
```

The load steps insert cleaned records into PostgreSQL and Snowflake when Snowflake is enabled. Internal warehouse metadata columns are generated during load and are not added back to the cleaned CSV.

## Data Quality

The `validate_clean_product_data` task checks:

- Required columns exist.
- Row count is above `DQ_MIN_ROWS`.
- Critical fields are not null.
- Prices are not negative.
- Ratings are between 0 and 5.
- Missing text values use `Info is not available now.` instead of null or old placeholders.
- `Availability` does not contain shipping or delivery text.
- `Product Name` is not blank.
- `Product Name` is capped at five words.
- `Product Name` does not end with a preposition, conjunction, or standalone number.
- Duplicate `sku` rows are removed during transformation.

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
- Average price and rating by device type
- Daily price trends
- Highest rated products
- Largest observed price changes
- Warehouse freshness monitoring
- Product name quality audit
- Discount and seller opportunity analysis

## Power BI Dashboard

Use Snowflake as the primary dashboard source.

Recommended tables:

- `RETAIL_DW.PUBLIC.DIM_PRODUCTS`
- `RETAIL_DW.PUBLIC.FACT_PRODUCT_SNAPSHOTS`

Connect `DIM_PRODUCTS` to `FACT_PRODUCT_SNAPSHOTS` by `PRODUCT_ID`.

The completed report includes six pages:

- Executive Overview
- Brand and Device Analysis
- Data Quality and Freshness
- Discount Opportunity
- Price Trends
- Product Trends

Captured dashboard highlights from May 10, 2026:

- About `7K` products and `96K` to `103K` product snapshots.
- Average price around `5.00K`.
- Average rating `2.93`.
- Average discount `2.95`.
- Data quality score `100.0%`.
- Warehouse freshness status `Fresh`.
- Smartphones are the largest device type at about `67.62%` of products.
- Apple has the highest product coverage in the captured report, followed by Samsung, Redmi, Xiaomi, Huawei, Honor, Infinix, Oppo, Realme, and Tecno.

![Executive Overview](docs/assets/powerbi/executive-overview.png)

![Data Quality and Freshness](docs/assets/powerbi/data-quality-freshness.png)

Dashboard setup, relationships, DAX measures, page descriptions, screenshots, and insights are documented in [docs/powerbi_dashboard_guide.md](docs/powerbi_dashboard_guide.md).

## Important Notes

- Amazon may return 503/CAPTCHA responses because of bot protection.
- The scraper is intended for education and portfolio demonstration.
- Empty scrape results are blocked by the data quality task instead of being loaded silently.
- `.env` is intentionally ignored by Git and must not be committed.
- `.env.example` contains placeholders only.
- Rotate Snowflake credentials if they are ever exposed publicly.

## Responsible Scraping Notice

This project was developed for educational and portfolio purposes only. The scraper is designed to follow responsible scraping practices by collecting only publicly available product information and avoiding aggressive request patterns.

To reduce unnecessary load on the target website, the scraping workflow should use request delays, limited request rates, and retry/backoff handling when pages return errors such as CAPTCHA, bot protection, or 503 responses.

No login-protected, private, personal, or sensitive user data is collected. Credentials, API keys, and environment variables are excluded from the repository.

Anyone reusing or modifying this project is responsible for reviewing and complying with the target website's Terms of Service, robots.txt rules, and applicable laws before running any scraping workflow.

## System Check

The latest local validation summary is available in [docs/system_check_report.md](docs/system_check_report.md).

## License

This project is licensed under the MIT License.

