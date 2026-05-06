import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging

from src.ai_validation import validate_product_record
from src.transform import (
    INFO_UNAVAILABLE_TEXT,
    PRODUCT_NAME_MAX_WORDS,
    extract_product_name,
    has_trailing_product_name_noise,
    product_name_word_count,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fetch database credentials from environment
conn_str = os.getenv("DW_CONN_STR", "postgresql+psycopg2://dw_user:dw_pass@postgres/retail_dw")
engine = create_engine(conn_str)


def _clean_value(value):
    return None if pd.isna(value) else value


def _clean_text_value(value):
    if pd.isna(value):
        return INFO_UNAVAILABLE_TEXT

    value = str(value).strip()
    return value or INFO_UNAVAILABLE_TEXT


LOAD_DEFAULT_COLUMNS = {
    "currency": "EGP",
    "model_number": None,
    "color": None,
    "screen_size": None,
    "ram_memory": None,
    "storage_capacity": None,
    "processor": None,
    "gpu": None,
    "operating_system": None,
    "display_resolution": None,
    "connectivity": None,
    "product_dimensions": None,
    "item_weight": None,
}


def _ensure_load_columns(df):
    for column, default_value in LOAD_DEFAULT_COLUMNS.items():
        if column not in df.columns:
            df[column] = default_value

    df["currency"] = df["currency"].fillna("EGP")
    return df


def _product_name_needs_refresh(product_name):
    if pd.isna(product_name):
        return True

    product_name = str(product_name).strip()
    return (
        not product_name
        or has_trailing_product_name_noise(product_name)
        or product_name_word_count(product_name) > PRODUCT_NAME_MAX_WORDS
    )


def _backfill_product_metadata(conn):
    rows = conn.execute(
        text(
            """
            SELECT
                product_id,
                title,
                product_name,
                brand,
                device_type
            FROM dim_products
            """
        )
    ).mappings().all()
    updated_count = 0

    for row in rows:
        validation_result = validate_product_record(row)
        product_name = row["product_name"]
        expected_brand = validation_result["brand"]
        expected_device_type = validation_result["Device type"]
        expected_brand = expected_brand or INFO_UNAVAILABLE_TEXT
        expected_device_type = expected_device_type or INFO_UNAVAILABLE_TEXT
        needs_refresh = (
            _product_name_needs_refresh(product_name)
            or row["brand"] != expected_brand
            or row["device_type"] != expected_device_type
        )
        if not needs_refresh:
            continue

        product_name = extract_product_name(row["title"]) or product_name or INFO_UNAVAILABLE_TEXT

        conn.execute(
            text(
                """
                UPDATE dim_products
                SET product_name = :product_name,
                    brand = :brand,
                    device_type = :device_type,
                    updated_at = CURRENT_TIMESTAMP
                WHERE product_id = :product_id
                """
            ),
            {
                "product_name": product_name,
                "brand": expected_brand,
                "device_type": expected_device_type,
                "product_id": row["product_id"],
            },
        )
        updated_count += 1

    if updated_count:
        logger.info("Backfilled product metadata for %s existing PostgreSQL products.", updated_count)


def _replace_missing_text_values(conn):
    missing_markers = (
        "not available",
        "n/a",
        "na",
        "none",
        "null",
        "unknown seller",
        "unkown seller",
    )
    marker_list = ", ".join(f"'{marker}'" for marker in missing_markers)
    dim_text_columns = [
        "title",
        "product_name",
        "brand",
        "device_type",
        "model_number",
        "color",
        "screen_size",
        "ram_memory",
        "storage_capacity",
        "processor",
        "gpu",
        "operating_system",
        "display_resolution",
        "connectivity",
        "product_dimensions",
        "item_weight",
        "product_url",
        "image_url",
    ]
    fact_text_columns = ["availability", "seller"]

    for column in dim_text_columns:
        conn.execute(
            text(
                f"""
                UPDATE dim_products
                SET {column} = :placeholder
                WHERE {column} IS NULL
                   OR BTRIM({column}::text) = ''
                   OR LOWER(BTRIM({column}::text)) IN ({marker_list})
                """
            ),
            {"placeholder": INFO_UNAVAILABLE_TEXT},
        )

    conn.execute(
        text(
            """
            UPDATE fact_product_snapshots
            SET currency = 'EGP'
            WHERE currency IS NULL OR BTRIM(currency::text) = ''
            """
        )
    )

    for column in fact_text_columns:
        conn.execute(
            text(
                f"""
                UPDATE fact_product_snapshots
                SET {column} = :placeholder
                WHERE {column} IS NULL
                   OR BTRIM({column}::text) = ''
                   OR LOWER(BTRIM({column}::text)) IN ({marker_list})
                """
            ),
            {"placeholder": INFO_UNAVAILABLE_TEXT},
        )

    conn.execute(
        text(
            """
            UPDATE fact_product_snapshots
            SET availability = :placeholder
            WHERE LOWER(COALESCE(availability, '')) ~ '(ship|delivery|deliver|ships|shipping)'
            """
        ),
        {"placeholder": INFO_UNAVAILABLE_TEXT},
    )


def _ensure_current_schema(conn):
    """
    Keeps existing PostgreSQL warehouses compatible with the latest cleaned CSV
    shape without requiring the user to rebuild the Docker volume.
    """
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS product_name TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS brand VARCHAR(255);"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS device_type VARCHAR(255);"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS model_number TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS color TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS screen_size TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS ram_memory TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS storage_capacity TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS processor TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS gpu TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS operating_system TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS display_resolution TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS connectivity TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS product_dimensions TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS item_weight TEXT;"))
    conn.execute(text("ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;"))
    conn.execute(text("ALTER TABLE fact_product_snapshots ADD COLUMN IF NOT EXISTS currency VARCHAR(10);"))
    conn.execute(text("ALTER TABLE fact_product_snapshots ADD COLUMN IF NOT EXISTS original_price DECIMAL(10, 2);"))
    conn.execute(text("ALTER TABLE fact_product_snapshots ADD COLUMN IF NOT EXISTS discount_percent DECIMAL(5, 2);"))
    conn.execute(text("ALTER TABLE fact_product_snapshots ADD COLUMN IF NOT EXISTS availability TEXT;"))
    conn.execute(text("ALTER TABLE fact_product_snapshots ADD COLUMN IF NOT EXISTS seller TEXT;"))
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
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS best_sellers_rank CASCADE;"))
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS brand_validation_status CASCADE;"))
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS device_type_validation_status CASCADE;"))
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS data_quality_score CASCADE;"))
    conn.execute(text("ALTER TABLE dim_products DROP COLUMN IF EXISTS validation_notes CASCADE;"))
    conn.execute(text("ALTER TABLE fact_product_snapshots DROP COLUMN IF EXISTS review_count;"))
    conn.execute(text("ALTER TABLE fact_product_snapshots DROP COLUMN IF EXISTS is_sponsored;"))
    conn.execute(text("ALTER TABLE fact_product_snapshots DROP COLUMN IF EXISTS is_prime;"))
    conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_products_sku ON dim_products(sku);"))
    _backfill_product_metadata(conn)
    _replace_missing_text_values(conn)


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
        df = _ensure_load_columns(df)
        
        with engine.begin() as conn:
            _ensure_current_schema(conn)

            for index, row in df.iterrows():
                # 1. Upsert into dim_products
                # Conflict on sku
                upsert_dim_sql = text("""
                    INSERT INTO dim_products (
                        sku, title, product_name, brand, device_type, model_number, color,
                        screen_size, ram_memory, storage_capacity, processor, gpu, operating_system,
                        display_resolution, connectivity, product_dimensions, item_weight,
                        product_url, image_url
                    )
                    VALUES (
                        :sku, :title, :product_name, :brand, :device_type, :model_number, :color,
                        :screen_size, :ram_memory, :storage_capacity, :processor, :gpu, :operating_system,
                        :display_resolution, :connectivity, :product_dimensions, :item_weight,
                        :product_url, :image_url
                    )
                    ON CONFLICT (sku) DO UPDATE
                    SET title = EXCLUDED.title,
                        product_name = EXCLUDED.product_name,
                        brand = EXCLUDED.brand,
                        device_type = EXCLUDED.device_type,
                        model_number = EXCLUDED.model_number,
                        color = EXCLUDED.color,
                        screen_size = EXCLUDED.screen_size,
                        ram_memory = EXCLUDED.ram_memory,
                        storage_capacity = EXCLUDED.storage_capacity,
                        processor = EXCLUDED.processor,
                        gpu = EXCLUDED.gpu,
                        operating_system = EXCLUDED.operating_system,
                        display_resolution = EXCLUDED.display_resolution,
                        connectivity = EXCLUDED.connectivity,
                        product_dimensions = EXCLUDED.product_dimensions,
                        item_weight = EXCLUDED.item_weight,
                        product_url = EXCLUDED.product_url,
                        image_url = EXCLUDED.image_url,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING product_id;
                """)
                
                result = conn.execute(upsert_dim_sql, {
                    "sku": str(row['sku']),
                    "title": _clean_text_value(row['title']),
                    "product_name": _clean_text_value(row['Product Name']),
                    "brand": _clean_text_value(row['brand']),
                    "device_type": _clean_text_value(row['Device type']),
                    "model_number": _clean_text_value(row['model_number']),
                    "color": _clean_text_value(row['color']),
                    "screen_size": _clean_text_value(row['screen_size']),
                    "ram_memory": _clean_text_value(row['ram_memory']),
                    "storage_capacity": _clean_text_value(row['storage_capacity']),
                    "processor": _clean_text_value(row['processor']),
                    "gpu": _clean_text_value(row['gpu']),
                    "operating_system": _clean_text_value(row['operating_system']),
                    "display_resolution": _clean_text_value(row['display_resolution']),
                    "connectivity": _clean_text_value(row['connectivity']),
                    "product_dimensions": _clean_text_value(row['product_dimensions']),
                    "item_weight": _clean_text_value(row['item_weight']),
                    "product_url": _clean_text_value(row['product_url']),
                    "image_url": _clean_text_value(row['image_url']),
                })
                
                product_id = result.scalar()
                
                # 2. Insert into fact_product_snapshots
                insert_fact_sql = text("""
                    INSERT INTO fact_product_snapshots (
                        product_id, price, currency, original_price, discount_percent, rating,
                        availability, seller
                    )
                    VALUES (
                        :product_id, :price, :currency, :original_price, :discount_percent, :rating,
                        :availability, :seller
                    );
                """)
                
                conn.execute(insert_fact_sql, {
                    "product_id": product_id,
                    "price": _clean_value(row['price']),
                    "currency": _clean_value(row['currency']),
                    "original_price": _clean_value(row['original_price']),
                    "discount_percent": _clean_value(row['discount_percent']),
                    "rating": _clean_value(row['rating']),
                    "availability": _clean_text_value(row['availability']),
                    "seller": _clean_text_value(row['seller']),
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
