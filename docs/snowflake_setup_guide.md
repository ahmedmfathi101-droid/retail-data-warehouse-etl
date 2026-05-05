# Snowflake Setup and Load Guide

This project can create the Snowflake warehouse objects and load the cleaned Amazon Egypt product data through the Airflow DAG.

## 1. Get the Snowflake Account Identifier

Open Snowflake in your browser and copy the account identifier from the URL.

Example URL:

```text
https://abc12345.us-east-1.snowflakecomputing.com
```

Use this value:

```env
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
```

The exact format depends on your Snowflake account and cloud region.

## 2. Configure `.env`

Update `.env`:

```env
SNOWFLAKE_ENABLED=true
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=fathi
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DW
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=
```

Leave `SNOWFLAKE_ROLE` blank unless your Snowflake user needs a specific role, such as `SYSADMIN`.

## 3. Required Snowflake Privileges

The configured role needs permission to:

- Create or use warehouse `COMPUTE_WH`
- Create or use database `RETAIL_DW`
- Create or use schema `PUBLIC`
- Create tables
- Insert and merge data

If your role cannot create warehouse/database objects, create them manually in Snowflake first:

```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS RETAIL_DW;
CREATE SCHEMA IF NOT EXISTS RETAIL_DW.PUBLIC;
```

## 4. Restart Airflow With the New Environment

```bash
docker compose up -d
```

## 5. Trigger the Airflow DAG

From the Airflow UI:

1. Open `http://localhost:8080`.
2. Log in with `admin` / `admin`.
3. Open DAG `amazon_eg_etl`.
4. Trigger it manually.

Or from the terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger amazon_eg_etl
```

## 6. Verify Snowflake Tables

Run these queries in Snowflake:

```sql
USE DATABASE RETAIL_DW;
USE SCHEMA PUBLIC;

SELECT COUNT(*) FROM DIM_PRODUCTS;
SELECT COUNT(*) FROM FACT_PRODUCT_SNAPSHOTS;
SELECT MAX(SNAPSHOT_TIMESTAMP) FROM FACT_PRODUCT_SNAPSHOTS;
```

## 7. Dashboard Source

Use Power BI to connect to Snowflake and load:

- `RETAIL_DW.PUBLIC.DIM_PRODUCTS`
- `RETAIL_DW.PUBLIC.FACT_PRODUCT_SNAPSHOTS`

See `docs/powerbi_dashboard_guide.md` for dashboard measures and visuals.
