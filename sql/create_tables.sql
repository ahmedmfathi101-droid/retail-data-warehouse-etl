-- Drop tables if they exist
DROP TABLE IF EXISTS fact_product_snapshots CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;

-- Create Dimension Table: dim_products
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    sku VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    brand VARCHAR(255),
    product_url TEXT,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (platform, sku)
);

-- Create Fact Table: fact_product_snapshots
CREATE TABLE fact_product_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES dim_products(product_id),
    price DECIMAL(10, 2),
    rating DECIMAL(3, 2),
    review_count INTEGER,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_dim_products_sku ON dim_products(sku);
CREATE INDEX idx_dim_products_platform ON dim_products(platform);
CREATE INDEX idx_fact_snapshots_date ON fact_product_snapshots(snapshot_date);
CREATE INDEX idx_fact_snapshots_product ON fact_product_snapshots(product_id);
