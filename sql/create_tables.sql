-- Drop tables if they exist
DROP TABLE IF EXISTS fact_product_snapshots CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;

-- Create Dimension Table: dim_products
CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    product_name TEXT,
    brand VARCHAR(255),
    device_type VARCHAR(255),
    model_number TEXT,
    color TEXT,
    screen_size TEXT,
    ram_memory TEXT,
    storage_capacity TEXT,
    processor TEXT,
    gpu TEXT,
    operating_system TEXT,
    display_resolution TEXT,
    connectivity TEXT,
    product_dimensions TEXT,
    item_weight TEXT,
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
    currency VARCHAR(10),
    original_price DECIMAL(10, 2),
    discount_percent DECIMAL(5, 2),
    rating DECIMAL(3, 2),
    availability TEXT,
    seller TEXT,
    snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_dim_products_sku ON dim_products(sku);
CREATE INDEX idx_dim_products_brand ON dim_products(brand);
CREATE INDEX idx_dim_products_device_type ON dim_products(device_type);
CREATE INDEX idx_fact_snapshots_date ON fact_product_snapshots(snapshot_date);
CREATE INDEX idx_fact_snapshots_product ON fact_product_snapshots(product_id);
CREATE INDEX idx_fact_snapshots_discount ON fact_product_snapshots(discount_percent);
