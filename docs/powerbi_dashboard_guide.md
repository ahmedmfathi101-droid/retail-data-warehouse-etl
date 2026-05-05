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
DIM_PRODUCTS[DATA_QUALITY_SCORE]
DIM_PRODUCTS[BRAND_VALIDATION_STATUS]
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

Average Data Quality Score = AVERAGE(DIM_PRODUCTS[DATA_QUALITY_SCORE])

Latest Snapshot = MAX(FACT_PRODUCT_SNAPSHOTS[SNAPSHOT_TIMESTAMP])
```

## Suggested Pages

### Executive Overview

- Cards: Total Products, Average Price, Average Rating, Average Discount, Average Data Quality Score, Latest Snapshot.
- Line chart: Average Price by Snapshot Date.
- Bar chart: Product Count by Brand and Device Type.

### Product Trends

- Matrix: Product Name, Brand, Device Type, Latest Price, Discount Percent, Latest Rating, Seller, Availability.
- Line chart: Price trend by Brand and Device Type.
- Slicer: Brand, Device Type, Seller, Snapshot Date.

### Data Quality and Freshness

- Card: Latest Snapshot.
- Card: Snapshot Count.
- Table: products with null rating, missing product name, low data quality score, or validation status not equal to `valid`.
- Bar chart: Product Count by Brand Validation Status.
- Alert rule in Power BI Service when Latest Snapshot is older than the expected schedule.
