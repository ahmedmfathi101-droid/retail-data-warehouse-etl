# Power BI Dashboard Guide

Use Snowflake as the primary source for the live dashboard.

## Connection

1. Open Power BI Desktop.
2. Select `Get Data` > `Snowflake`.
3. Enter your Snowflake server/account and warehouse.
4. Select the database and schema configured in `.env`.
5. Import or DirectQuery these tables:
   - `DIM_PRODUCTS`
   - `FACT_PRODUCT_SNAPSHOTS`

DirectQuery is preferred when you want the dashboard to stay close to live warehouse data.

## Data Model

Create a relationship:

```text
DIM_PRODUCTS[PRODUCT_ID] 1 -> * FACT_PRODUCT_SNAPSHOTS[PRODUCT_ID]
```

Recommended date column:

```text
FACT_PRODUCT_SNAPSHOTS[SNAPSHOT_DATE]
```

## Core Measures

```DAX
Total Products = DISTINCTCOUNT(DIM_PRODUCTS[PRODUCT_ID])

Total Snapshots = COUNTROWS(FACT_PRODUCT_SNAPSHOTS)

Average Price = AVERAGE(FACT_PRODUCT_SNAPSHOTS[PRICE])

Average Rating = AVERAGE(FACT_PRODUCT_SNAPSHOTS[RATING])

Latest Snapshot = MAX(FACT_PRODUCT_SNAPSHOTS[SNAPSHOT_TIMESTAMP])
```

## Suggested Pages

### Executive Overview

- Cards: Total Products, Average Price, Average Rating, Latest Snapshot.
- Line chart: Average Price by Snapshot Date.
- Bar chart: Product Count by Device Type.

### Product Trends

- Matrix: Product Name, Product Title, Device Type, Latest Price, Latest Rating.
- Line chart: Price trend by Device Type.
- Slicer: Device Type, Snapshot Date.

### Data Quality and Freshness

- Card: Latest Snapshot.
- Card: Snapshot Count.
- Table: products with null rating or missing product name.
- Alert rule in Power BI Service when Latest Snapshot is older than the expected schedule.
