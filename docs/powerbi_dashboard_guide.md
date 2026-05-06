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

Recommended product display column:

```text
DIM_PRODUCTS[PRODUCT_NAME]
```

`PRODUCT_NAME` is generated from the full title and cleaned to avoid trailing prepositions, conjunctions, and standalone numbers.

Recommended analysis columns:

```text
DIM_PRODUCTS[BRAND]
DIM_PRODUCTS[DEVICE_TYPE]
DIM_PRODUCTS[STORAGE_CAPACITY]
DIM_PRODUCTS[RAM_MEMORY]
FACT_PRODUCT_SNAPSHOTS[DISCOUNT_PERCENT]
FACT_PRODUCT_SNAPSHOTS[AVAILABILITY]
FACT_PRODUCT_SNAPSHOTS[SELLER]
```

## Core Measures

```DAX
Total Products = DISTINCTCOUNT(DIM_PRODUCTS[PRODUCT_ID])

Total Snapshots = COUNTROWS(FACT_PRODUCT_SNAPSHOTS)

Average Price = AVERAGE(FACT_PRODUCT_SNAPSHOTS[PRICE])

Average Rating = AVERAGE(FACT_PRODUCT_SNAPSHOTS[RATING])

Average Discount = AVERAGE(FACT_PRODUCT_SNAPSHOTS[DISCOUNT_PERCENT])

Latest Snapshot = MAX(FACT_PRODUCT_SNAPSHOTS[SNAPSHOT_TIMESTAMP])
```

## Suggested Pages

### Executive Overview

- Cards: Total Products, Average Price, Average Rating, Average Discount, Latest Snapshot.
- Line chart: Average Price by Snapshot Date.
- Bar chart: Product Count by Brand and Device Type.

### Product Trends

- Matrix: Product Name, Brand, Device Type, Latest Price, Discount Percent, Latest Rating, Seller, Availability.
- Line chart: Price trend by Brand and Device Type.
- Slicer: Brand, Device Type, Seller, Snapshot Date.

### Data Quality and Freshness

- Card: Latest Snapshot.
- Card: Snapshot Count.
- Table: products with placeholder values, missing ratings, or availability that needs review.
- Bar chart: Product Count by Availability.
- Alert rule in Power BI Service when Latest Snapshot is older than the expected schedule.
