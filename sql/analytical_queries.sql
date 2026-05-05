-- Analytical SQL for Amazon Egypt Retail Warehouse
-- These queries work as dashboard sources in Snowflake and are easy to adapt for PostgreSQL.

-- 1. Core KPI summary
SELECT
    COUNT(DISTINCT p.product_id) AS total_products,
    COUNT(*) AS total_snapshots,
    ROUND(AVG(f.price), 2) AS avg_price,
    ROUND(AVG(f.rating), 2) AS avg_rating,
    MAX(f.snapshot_timestamp) AS latest_snapshot
FROM fact_product_snapshots f
JOIN dim_products p
    ON f.product_id = p.product_id;

-- 2. Average price and rating by device type
SELECT
    p.device_type,
    COUNT(DISTINCT p.product_id) AS product_count,
    ROUND(AVG(f.price), 2) AS avg_price,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact_product_snapshots f
JOIN dim_products p
    ON f.product_id = p.product_id
GROUP BY p.device_type
ORDER BY product_count DESC;

-- 3. Daily price trend
SELECT
    f.snapshot_date,
    p.device_type,
    ROUND(AVG(f.price), 2) AS avg_price,
    COUNT(*) AS snapshot_count
FROM fact_product_snapshots f
JOIN dim_products p
    ON f.product_id = p.product_id
GROUP BY f.snapshot_date, p.device_type
ORDER BY f.snapshot_date, p.device_type;

-- 4. Highest rated products
SELECT
    p.product_name,
    p.title,
    p.device_type,
    ROUND(AVG(f.rating), 2) AS avg_rating,
    ROUND(AVG(f.price), 2) AS avg_price
FROM fact_product_snapshots f
JOIN dim_products p
    ON f.product_id = p.product_id
GROUP BY p.product_name, p.title, p.device_type
ORDER BY avg_rating DESC, avg_price DESC
LIMIT 20;

-- 5. Products with the largest observed price changes
WITH product_prices AS (
    SELECT
        p.product_id,
        p.product_name,
        p.title,
        p.device_type,
        MIN(f.price) AS min_price,
        MAX(f.price) AS max_price
    FROM fact_product_snapshots f
    JOIN dim_products p
        ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.title, p.device_type
)
SELECT
    product_name,
    title,
    device_type,
    min_price,
    max_price,
    ROUND(max_price - min_price, 2) AS price_change
FROM product_prices
WHERE min_price IS NOT NULL
ORDER BY price_change DESC
LIMIT 20;

-- 6. Freshness check for dashboard monitoring
SELECT
    MAX(snapshot_timestamp) AS latest_snapshot,
    DATEDIFF('hour', MAX(snapshot_timestamp), CURRENT_TIMESTAMP()) AS age_hours
FROM fact_product_snapshots;
