import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch database credentials from environment
conn_str = os.getenv("DW_CONN_STR", "postgresql+psycopg2://dw_user:dw_pass@postgres/retail_dw")
engine = create_engine(conn_str)

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
            for index, row in df.iterrows():
                # 1. Upsert into dim_products
                # Conflict on (platform, sku)
                upsert_dim_sql = text("""
                    INSERT INTO dim_products (platform, sku, title, brand, product_url, image_url)
                    VALUES (:platform, :sku, :title, :brand, :product_url, :image_url)
                    ON CONFLICT (platform, sku) DO UPDATE 
                    SET title = EXCLUDED.title,
                        brand = EXCLUDED.brand,
                        product_url = EXCLUDED.product_url,
                        image_url = EXCLUDED.image_url
                    RETURNING product_id;
                """)
                
                result = conn.execute(upsert_dim_sql, {
                    "platform": row['platform'],
                    "sku": str(row['sku']),
                    "title": row['title'],
                    "brand": row['brand'],
                    "product_url": row['product_url'],
                    "image_url": row['image_url']
                })
                
                product_id = result.scalar()
                
                # 2. Insert into fact_product_snapshots
                insert_fact_sql = text("""
                    INSERT INTO fact_product_snapshots (product_id, price, rating, review_count)
                    VALUES (:product_id, :price, :rating, :review_count);
                """)
                
                price = None if pd.isna(row['price']) else row['price']
                rating = None if pd.isna(row['rating']) else row['rating']
                review_count = None if pd.isna(row['review_count']) else int(row['review_count'])

                conn.execute(insert_fact_sql, {
                    "product_id": product_id,
                    "price": price,
                    "rating": rating,
                    "review_count": review_count
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
