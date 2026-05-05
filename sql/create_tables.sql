-- Drop tables if they exist
DROP TABLE IF EXISTS fact_product_snapshots CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;

-- Create Dimension Table: dim_products
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    product_name TEXT,
    device_type VARCHAR(255),
    product_url TEXT,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sku)
);

-- Create Fact Table: fact_product_snapshots
CREATE TABLE fact_product_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES dim_products(product_id),
    price DECIMAL(10, 2),
    rating DECIMAL(3, 2),
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_dim_products_sku ON dim_products(sku);
CREATE INDEX idx_dim_products_device_type ON dim_products(device_type);
CREATE INDEX idx_fact_snapshots_date ON fact_product_snapshots(snapshot_date);
CREATE INDEX idx_fact_snapshots_product ON fact_product_snapshots(product_id);
