import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch database credentials from environment
conn_str = os.getenv("DW_CONN_STR", "postgresql+psycopg2://dw_user:dw_pass@postgres/retail_dw")
engine = create_engine(conn_str)


def _clean_value(value):
    return None if pd.isna(value) else value


def _ensure_current_schema(conn):
    """
    Keeps existing PostgreSQL warehouses compatible with the latest cleaned CSV
    shape without requiring the user to rebuild the Docker volume.
    """
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS product_name TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS device_type VARCHAR(255);"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"))
    conn.execute(text("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'dim_products'
                  AND column_name = 'brand'
            ) THEN
                UPDATE dim_products
                SET device_type = brand
                WHERE device_type IS NULL AND brand IS NOT NULL;
            END IF;
        END $$;
    """))
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS platform CASCADE;"))
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS brand;"))
    conn.execute(text("ALTER TABLE fact_product_snapshots DROP COLUMN IF EXISTS review_count;"))
    conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_products_sku ON dim_products(sku);"))


def load_data(clean_file_path):
    """
    Loads data into PostgreSQL Data Warehouse using an Upsert strategy.
    """
    logger.info(f"Loading data from {clean_file_path} into DW")
    
    if not os.path.exists(clean_file_path):
        logger.error(f"File not found: {clean_file_path}")
        return

    try:
        df = pd.read_csv(clean_file_path)
        
        with engine.begin() as conn:
            _ensure_current_schema(conn)

            for index, row in df.iterrows():
                # 1. Upsert into dim_products
                # Conflict on sku
                upsert_dim_sql = text("""
                    INSERT INTO dim_products (sku, title, product_name, device_type, product_url, image_url)
                    VALUES (:sku, :title, :product_name, :device_type, :product_url, :image_url)
                    ON CONFLICT (sku) DO UPDATE
                    SET title = EXCLUDED.title,
                        product_name = EXCLUDED.product_name,
                        device_type = EXCLUDED.device_type,
                        product_url = EXCLUDED.product_url,
                        image_url = EXCLUDED.image_url,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING product_id;
                """)
                
                result = conn.execute(upsert_dim_sql, {
                    "sku": str(row['sku']),
                    "title": row['title'],
                    "product_name": _clean_value(row['Product Name']),
                    "device_type": _clean_value(row['Device type']),
                    "product_url": _clean_value(row['product_url']),
                    "image_url": _clean_value(row['image_url'])
                })
                
                product_id = result.scalar()
                
                # 2. Insert into fact_product_snapshots
                insert_fact_sql = text("""
                    INSERT INTO fact_product_snapshots (product_id, price, rating)
                    VALUES (:product_id, :price, :rating);
                """)
                
                conn.execute(insert_fact_sql, {
                    "product_id": product_id,
                    "price": _clean_value(row['price']),
                    "rating": _clean_value(row['rating']),
                })
            
            logger.info("Successfully loaded data into Data Warehouse.")

    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    # Test path
    test_path = "/opt/airflow/data/clean_amazon_eg_products.csv"
    if os.path.exists(test_path):
        load_data(test_path)
