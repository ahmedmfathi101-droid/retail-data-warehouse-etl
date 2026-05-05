# System Check Report

Last checked: 2026-05-05 17:23 Africa/Cairo

## Scope

- Docker Compose services
- Airflow DAG loading and task execution
- Python runtime imports inside Airflow
- PostgreSQL warehouse tables and freshness
- Data quality validation
- Analytical SQL and Power BI documentation
- Prefect removal verification

## Results

| Area | Status | Notes |
| --- | --- | --- |
| Docker services | Passed | PostgreSQL, Airflow webserver, and Airflow scheduler are running. |
| Airflow DAG imports | Passed | `airflow dags list-import-errors` returned no import errors. |
| Airflow DAG tasks | Passed | DAG contains extract, transform, data quality, PostgreSQL load, Snowflake load, and freshness tasks. |
| End-to-end DAG run | Passed | Manual run `manual__2026-05-04T23:56:52+00:00` completed successfully. |
| Data quality | Passed | Latest clean batch has 403 rows, no failures, and no warnings. |
| PostgreSQL warehouse | Passed | `dim_products` has 494 rows and `fact_product_snapshots` has 5899 rows. |
| Freshness check | Passed | Latest PostgreSQL snapshot timestamp: `2026-05-05 14:07:43.895895`. |
| Snowflake warehouse load | Passed | Snowflake setup, schema migration, product name backfill, and bulk load completed successfully with `SNOWFLAKE_ENABLED=true`. |
| Analytical SQL | Passed | Insight queries are available in `sql/analytical_queries.sql`. |
| Power BI guide | Passed | Dashboard setup is documented in `docs/powerbi_dashboard_guide.md`. |
| Prefect removal | Passed | No Prefect files or references remain. |

## Important Notes

- Snowflake is enabled in `.env` with `SNOWFLAKE_ENABLED=true`.
- Snowflake loaded tables:
  - `RETAIL_DW.PUBLIC.DIM_PRODUCTS`: 493 rows
  - `RETAIL_DW.PUBLIC.FACT_PRODUCT_SNAPSHOTS`: 3193 rows
  - Latest Snowflake snapshot timestamp: `2026-05-05 07:22:31.694000`
- The transform step now removes duplicate `sku` rows before loading to prevent duplicate snapshots from duplicated search results in the same batch.
- `Product Name` is generated from the title, capped at five words, and cleaned so it does not end with a preposition, conjunction, or standalone number.
- PostgreSQL and Snowflake loaders backfill `Product Name` for existing dimension rows when the naming rule changes.
- Amazon may intermittently return 503/CAPTCHA responses. If that results in an empty clean file, the data quality task is expected to fail the DAG instead of loading empty warehouse data.
