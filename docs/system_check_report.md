# System Check Report

Last checked: 2026-05-06 22:53 +03:00

## Scope

- Docker Compose services
- Airflow DAG schedule, task execution, and recent runs
- Python runtime imports inside Airflow
- Clean CSV quality validation
- PostgreSQL warehouse schema, data cleanup, and freshness
- Snowflake warehouse schema, data cleanup, and freshness
- Analytical SQL, Power BI guide, README, and Jupyter EDA notebook
- Removal of stale temporary files and obsolete schema references

## Results

| Area | Status | Notes |
| --- | --- | --- |
| Docker services | Passed | PostgreSQL, Airflow webserver, and Airflow scheduler are running. |
| Airflow DAG imports | Passed | `airflow dags list-import-errors` returned no import errors. |
| Airflow schedule | Passed | `amazon_eg_etl` runs every hour with `schedule_interval=timedelta(hours=1)` and `max_active_runs=1`. |
| Recent Airflow runs | Passed | Latest manual cleanup run and the latest scheduled run both completed successfully. |
| Clean CSV | Passed | `data/clean_amazon_eg_products.csv` has 4,937 rows, 25 columns, no null cells, no duplicate SKU rows, and no removed columns. |
| Data quality | Passed | `data/data_quality_report.json` was regenerated and validation passed for 4,937 rows. |
| Availability/seller cleanup | Passed | CSV, PostgreSQL, and Snowflake have 0 old missing markers, 0 bad seller markers, and 0 availability values containing shipping/delivery terms. |
| PostgreSQL warehouse | Passed | `dim_products`: 5,212 rows. `fact_product_snapshots`: 52,790 rows. Old columns remaining: 0. Bad text cells: 0. |
| PostgreSQL freshness | Passed | Latest PostgreSQL snapshot timestamp: `2026-05-06 19:51:16.205371`. |
| Snowflake warehouse | Passed | `DIM_PRODUCTS`: 5,212 rows. `FACT_PRODUCT_SNAPSHOTS`: 54,933 rows. Old columns remaining: 0. Bad text cells: 0. |
| Snowflake freshness | Passed | Latest Snowflake snapshot timestamp: `2026-05-06 12:52:31.143000` in the Snowflake session timezone. |
| Analytical SQL | Passed | Insight queries are available in `sql/analytical_queries.sql` and no longer reference removed columns. |
| Power BI guide | Passed | Dashboard guidance is aligned with the current warehouse schema. |
| Jupyter notebook | Passed | `notebooks/amazon_eg_eda.ipynb` is valid JSON and ready for exploratory analysis. |
| Cleanup | Passed | Removed Python cache folders, stale schema screenshots, the enrichment probe report, and the leftover raw partial file. |

## Current Clean Dataset

Device coverage in the latest clean batch:

- Smartphone: 3,256 rows
- Tablet: 1,026 rows
- Laptop: 523 rows
- Google pixel: 132 rows

The cleaned CSV columns are:

```text
sku,title,Product Name,brand,price,original_price,discount_percent,rating,availability,seller,product_url,image_url,Device type,model_number,color,screen_size,ram_memory,storage_capacity,processor,gpu,operating_system,display_resolution,connectivity,product_dimensions,item_weight
```

## Important Notes

- Removed columns are absent from the CSV, PostgreSQL, Snowflake, SQL schema, docs, and Power BI guide. The only remaining references are intentional `DROP COLUMN IF EXISTS` migration lines in the loaders.
- Missing text values are standardized to `Info is not available now.`.
- `seller` no longer contains `Not available`, `Unknown Seller`, or `Unkown Seller`.
- `availability` is treated as stock status only; shipping and delivery text is filtered out.
- The scraper covers laptops, mobiles, smartphones, feature phones, foldable phones, iPhones, Android phones, tablets, and major phone/tablet brands from the configured search terms.
- Snowflake is enabled with `SNOWFLAKE_ENABLED=true`.
