import logging
import os
import re
from itertools import islice

import pandas as pd

from src.ai_validation import validate_product_record
from src.transform import (
    PRODUCT_NAME_MAX_WORDS,
    extract_product_name,
    has_trailing_product_name_noise,
    product_name_word_count,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]


def _is_enabled():
    return os.getenv("SNOWFLAKE_ENABLED", "false").strip().lower() in {"1", "true", "yes"}


def _missing_env_vars():
    return [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]


def _clean_value(value):
    return None if pd.isna(value) else value


def _snowflake_identifier(env_var_name):
    value = os.getenv(env_var_name, "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(
            f"{env_var_name} must be a valid Snowflake identifier. "
            "Use letters, numbers, underscores, or dollar signs, and start with a letter or underscore."
        )
    return value.upper()


def _connect_to_snowflake(include_context=True):
    import snowflake.connector

    connect_kwargs = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
    }

    if include_context:
        connect_kwargs.update(
            {
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            }
        )

    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        connect_kwargs["role"] = role

    return snowflake.connector.connect(**connect_kwargs)


def setup_snowflake_warehouse():
    """
    Creates the configured Snowflake warehouse, database, and schema if possible.
    Requires a Snowflake role with CREATE WAREHOUSE and CREATE DATABASE privileges.
    """
    if not _is_enabled():
        logger.info("Snowflake setup skipped because SNOWFLAKE_ENABLED is not true.")
        return

    missing = _missing_env_vars()
    if missing:
        raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")

    warehouse = _snowflake_identifier("SNOWFLAKE_WAREHOUSE")
    database = _snowflake_identifier("SNOWFLAKE_DATABASE")
    schema = _snowflake_identifier("SNOWFLAKE_SCHEMA")

    conn = _connect_to_snowflake(include_context=False)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                CREATE WAREHOUSE IF NOT EXISTS {warehouse}
                WAREHOUSE_SIZE = XSMALL
                AUTO_SUSPEND = 60
                AUTO_RESUME = TRUE
                INITIALLY_SUSPENDED = TRUE
                """
            )
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
            logger.info("Snowflake setup complete for %s.%s using warehouse %s.", database, schema, warehouse)
        finally:
            cursor.close()
    finally:
        conn.close()


def _create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            sku VARCHAR(100) NOT NULL,
            title VARCHAR NOT NULL,
            product_name VARCHAR,
            brand VARCHAR(255),
            device_type VARCHAR(255),
            model_number VARCHAR,
            color VARCHAR,
            screen_size VARCHAR,
            ram_memory VARCHAR,
            storage_capacity VARCHAR,
            processor VARCHAR,
            gpu VARCHAR,
            operating_system VARCHAR,
            display_resolution VARCHAR,
            connectivity VARCHAR,
            product_dimensions VARCHAR,
            item_weight VARCHAR,
            best_sellers_rank VARCHAR,
            product_url VARCHAR,
            image_url VARCHAR,
            brand_validation_status VARCHAR(50),
            device_type_validation_status VARCHAR(50),
            data_quality_score NUMBER(5, 2),
            validation_notes VARCHAR,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_product_snapshots (
            snapshot_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            product_id NUMBER NOT NULL,
            price NUMBER(10, 2),
            currency VARCHAR(10),
            original_price NUMBER(10, 2),
            discount_percent NUMBER(5, 2),
            rating NUMBER(3, 2),
            availability VARCHAR,
            seller VARCHAR,
            is_sponsored BOOLEAN,
            is_prime BOOLEAN,
            snapshot_date DATE DEFAULT CURRENT_DATE(),
            snapshot_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def _column_exists(cursor, table_name, column_name):
    cursor.execute(f"SHOW COLUMNS LIKE '{column_name.upper()}' IN TABLE {table_name}")
    return cursor.fetchone() is not None


def _add_column_if_missing(cursor, table_name, column_name, column_type):
    if not _column_exists(cursor, table_name, column_name):
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _drop_column_if_exists(cursor, table_name, column_name):
    if _column_exists(cursor, table_name, column_name):
        cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")


def _chunked(items, size):
    iterator = iter(items)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


def _product_name_needs_refresh(product_name):
    if pd.isna(product_name):
        return True

    product_name = str(product_name).strip()
    return (
        not product_name
        or has_trailing_product_name_noise(product_name)
        or product_name_word_count(product_name) > PRODUCT_NAME_MAX_WORDS
    )


def _backfill_product_metadata(cursor):
    cursor.execute(
        """
        SELECT
            product_id,
            title,
            product_name,
            brand,
            device_type,
            brand_validation_status,
            device_type_validation_status,
            data_quality_score
        FROM dim_products
        """
    )
    rows = cursor.fetchall()
    updates = []

    for (
        product_id,
        title,
        product_name,
        brand,
        device_type,
        brand_validation_status,
        device_type_validation_status,
        data_quality_score,
    ) in rows:
        validation_result = validate_product_record(
            {
                "title": title,
                "Product Name": product_name,
                "brand": brand,
                "Device type": device_type,
            }
        )
        needs_refresh = (
            _product_name_needs_refresh(product_name)
            or validation_result["brand_validation_status"] in {"invalid_device_type", "corrected", "missing"}
            or brand_validation_status is None
            or device_type_validation_status is None
            or data_quality_score is None
        )
        if not needs_refresh:
            continue

        generated_product_name = extract_product_name(title)
        updates.append(
            {
                "product_id": product_id,
                "product_name": generated_product_name or product_name,
                "brand": validation_result["brand"],
                "device_type": validation_result["Device type"],
                "brand_validation_status": validation_result["brand_validation_status"],
                "device_type_validation_status": validation_result["device_type_validation_status"],
                "data_quality_score": validation_result["data_quality_score"],
                "validation_notes": validation_result["validation_notes"],
            }
        )

    if not updates:
        return

    for chunk_index, chunk in enumerate(_chunked(updates, 100)):
        params = {}
        case_lines = []
        brand_case_lines = []
        device_type_case_lines = []
        brand_status_case_lines = []
        device_status_case_lines = []
        quality_score_case_lines = []
        notes_case_lines = []
        id_placeholders = []

        for row_index, row in enumerate(chunk):
            suffix = f"{chunk_index}_{row_index}"
            product_id_key = f"product_id_{suffix}"
            product_name_key = f"product_name_{suffix}"
            brand_key = f"brand_{suffix}"
            device_type_key = f"device_type_{suffix}"
            brand_status_key = f"brand_status_{suffix}"
            device_status_key = f"device_status_{suffix}"
            quality_score_key = f"quality_score_{suffix}"
            notes_key = f"notes_{suffix}"
            params[product_id_key] = row["product_id"]
            params[product_name_key] = row["product_name"]
            params[brand_key] = row["brand"]
            params[device_type_key] = row["device_type"]
            params[brand_status_key] = row["brand_validation_status"]
            params[device_status_key] = row["device_type_validation_status"]
            params[quality_score_key] = row["data_quality_score"]
            params[notes_key] = row["validation_notes"]
            case_lines.append(
                "WHEN product_id = %({product_id})s THEN %({product_name})s".format(
                    product_id=product_id_key,
                    product_name=product_name_key,
                )
            )
            brand_case_lines.append(f"WHEN product_id = %({product_id_key})s THEN %({brand_key})s")
            device_type_case_lines.append(f"WHEN product_id = %({product_id_key})s THEN %({device_type_key})s")
            brand_status_case_lines.append(f"WHEN product_id = %({product_id_key})s THEN %({brand_status_key})s")
            device_status_case_lines.append(f"WHEN product_id = %({product_id_key})s THEN %({device_status_key})s")
            quality_score_case_lines.append(f"WHEN product_id = %({product_id_key})s THEN %({quality_score_key})s")
            notes_case_lines.append(f"WHEN product_id = %({product_id_key})s THEN %({notes_key})s")
            id_placeholders.append(f"%({product_id_key})s")

        cursor.execute(
            f"""
            UPDATE dim_products
            SET product_name = CASE
                    {' '.join(case_lines)}
                    ELSE product_name
                END,
                brand = CASE
                    {' '.join(brand_case_lines)}
                    ELSE brand
                END,
                device_type = CASE
                    {' '.join(device_type_case_lines)}
                    ELSE device_type
                END,
                brand_validation_status = CASE
                    {' '.join(brand_status_case_lines)}
                    ELSE brand_validation_status
                END,
                device_type_validation_status = CASE
                    {' '.join(device_status_case_lines)}
                    ELSE device_type_validation_status
                END,
                data_quality_score = CASE
                    {' '.join(quality_score_case_lines)}
                    ELSE data_quality_score
                END,
                validation_notes = CASE
                    {' '.join(notes_case_lines)}
                    ELSE validation_notes
                END,
                updated_at = CURRENT_TIMESTAMP()
            WHERE product_id IN ({', '.join(id_placeholders)})
            """,
            params,
        )

    logger.info("Backfilled product metadata for %s existing Snowflake products.", len(updates))


def _create_clean_products_stage(cursor):
    cursor.execute("DROP TABLE IF EXISTS clean_products_stage")
    cursor.execute(
        """
        CREATE TEMPORARY TABLE clean_products_stage (
            sku VARCHAR,
            title VARCHAR,
            product_name VARCHAR,
            brand VARCHAR(255),
            device_type VARCHAR(255),
            model_number VARCHAR,
            color VARCHAR,
            screen_size VARCHAR,
            ram_memory VARCHAR,
            storage_capacity VARCHAR,
            processor VARCHAR,
            gpu VARCHAR,
            operating_system VARCHAR,
            display_resolution VARCHAR,
            connectivity VARCHAR,
            product_dimensions VARCHAR,
            item_weight VARCHAR,
            best_sellers_rank VARCHAR,
            product_url VARCHAR,
            image_url VARCHAR,
            brand_validation_status VARCHAR(50),
            device_type_validation_status VARCHAR(50),
            data_quality_score NUMBER(5, 2),
            validation_notes VARCHAR,
            price NUMBER(10, 2),
            currency VARCHAR(10),
            original_price NUMBER(10, 2),
            discount_percent NUMBER(5, 2),
            rating NUMBER(3, 2),
            availability VARCHAR,
            seller VARCHAR,
            is_sponsored BOOLEAN,
            is_prime BOOLEAN
        )
        """
    )


def _insert_stage_rows(cursor, df):
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "sku": str(row["sku"]),
                "title": str(row["title"]),
                "product_name": _clean_value(row["Product Name"]),
                "brand": _clean_value(row["brand"]),
                "device_type": _clean_value(row["Device type"]),
                "model_number": _clean_value(row["model_number"]),
                "color": _clean_value(row["color"]),
                "screen_size": _clean_value(row["screen_size"]),
                "ram_memory": _clean_value(row["ram_memory"]),
                "storage_capacity": _clean_value(row["storage_capacity"]),
                "processor": _clean_value(row["processor"]),
                "gpu": _clean_value(row["gpu"]),
                "operating_system": _clean_value(row["operating_system"]),
                "display_resolution": _clean_value(row["display_resolution"]),
                "connectivity": _clean_value(row["connectivity"]),
                "product_dimensions": _clean_value(row["product_dimensions"]),
                "item_weight": _clean_value(row["item_weight"]),
                "best_sellers_rank": _clean_value(row["best_sellers_rank"]),
                "product_url": _clean_value(row["product_url"]),
                "image_url": _clean_value(row["image_url"]),
                "brand_validation_status": _clean_value(row["brand_validation_status"]),
                "device_type_validation_status": _clean_value(row["device_type_validation_status"]),
                "data_quality_score": _clean_value(row["data_quality_score"]),
                "validation_notes": _clean_value(row["validation_notes"]),
                "price": _clean_value(row["price"]),
                "currency": _clean_value(row["currency"]),
                "original_price": _clean_value(row["original_price"]),
                "discount_percent": _clean_value(row["discount_percent"]),
                "rating": _clean_value(row["rating"]),
                "availability": _clean_value(row["availability"]),
                "seller": _clean_value(row["seller"]),
                "is_sponsored": _clean_value(row["is_sponsored"]),
                "is_prime": _clean_value(row["is_prime"]),
            }
        )

    for chunk_index, chunk in enumerate(_chunked(rows, 100)):
        params = {}
        select_lines = []
        columns = [
            "sku",
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
            "best_sellers_rank",
            "product_url",
            "image_url",
            "brand_validation_status",
            "device_type_validation_status",
            "data_quality_score",
            "validation_notes",
            "price",
            "currency",
            "original_price",
            "discount_percent",
            "rating",
            "availability",
            "seller",
            "is_sponsored",
            "is_prime",
        ]

        for row_index, row in enumerate(chunk):
            suffix = f"{chunk_index}_{row_index}"
            placeholders = []
            for column in columns:
                key = f"{column}_{suffix}"
                params[key] = row[column]
                placeholders.append(f"%({key})s")
            select_lines.append("SELECT " + ", ".join(placeholders))

        cursor.execute(
            f"""
            INSERT INTO clean_products_stage (
                sku, title, product_name, brand, device_type, model_number, color,
                screen_size, ram_memory, storage_capacity, processor, gpu, operating_system,
                display_resolution, connectivity, product_dimensions, item_weight,
                best_sellers_rank, product_url, image_url, brand_validation_status,
                device_type_validation_status, data_quality_score, validation_notes,
                price, currency, original_price, discount_percent, rating, availability,
                seller, is_sponsored, is_prime
            )
            {' UNION ALL '.join(select_lines)}
            """,
            params,
        )


def _merge_staged_products(cursor):
    cursor.execute(
        """
        MERGE INTO dim_products AS target
        USING (
            SELECT
                sku,
                title,
                product_name,
                brand,
                device_type,
                model_number,
                color,
                screen_size,
                ram_memory,
                storage_capacity,
                processor,
                gpu,
                operating_system,
                display_resolution,
                connectivity,
                product_dimensions,
                item_weight,
                best_sellers_rank,
                product_url,
                image_url,
                brand_validation_status,
                device_type_validation_status,
                data_quality_score,
                validation_notes
            FROM clean_products_stage
            QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY sku) = 1
        ) AS source
        ON target.sku = source.sku
        WHEN MATCHED THEN UPDATE SET
            title = source.title,
            product_name = source.product_name,
            brand = source.brand,
            device_type = source.device_type,
            model_number = source.model_number,
            color = source.color,
            screen_size = source.screen_size,
            ram_memory = source.ram_memory,
            storage_capacity = source.storage_capacity,
            processor = source.processor,
            gpu = source.gpu,
            operating_system = source.operating_system,
            display_resolution = source.display_resolution,
            connectivity = source.connectivity,
            product_dimensions = source.product_dimensions,
            item_weight = source.item_weight,
            best_sellers_rank = source.best_sellers_rank,
            product_url = source.product_url,
            image_url = source.image_url,
            brand_validation_status = source.brand_validation_status,
            device_type_validation_status = source.device_type_validation_status,
            data_quality_score = source.data_quality_score,
            validation_notes = source.validation_notes,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            sku, title, product_name, brand, device_type, model_number, color,
            screen_size, ram_memory, storage_capacity, processor, gpu, operating_system,
            display_resolution, connectivity, product_dimensions, item_weight,
            best_sellers_rank, product_url, image_url, brand_validation_status,
            device_type_validation_status, data_quality_score, validation_notes
        ) VALUES (
            source.sku, source.title, source.product_name, source.brand, source.device_type,
            source.model_number, source.color, source.screen_size, source.ram_memory,
            source.storage_capacity, source.processor, source.gpu, source.operating_system,
            source.display_resolution, source.connectivity, source.product_dimensions,
            source.item_weight, source.best_sellers_rank, source.product_url, source.image_url,
            source.brand_validation_status, source.device_type_validation_status,
            source.data_quality_score, source.validation_notes
        )
        """
    )


def _insert_staged_snapshots(cursor):
    cursor.execute(
        """
        INSERT INTO fact_product_snapshots (
            product_id, price, currency, original_price, discount_percent, rating,
            availability, seller, is_sponsored, is_prime
        )
        SELECT
            products.product_id,
            stage.price,
            stage.currency,
            stage.original_price,
            stage.discount_percent,
            stage.rating,
            stage.availability,
            stage.seller,
            stage.is_sponsored,
            stage.is_prime
        FROM clean_products_stage AS stage
        JOIN dim_products AS products
            ON products.sku = stage.sku
        """
    )


def _load_staged_products(cursor, df):
    if df.empty:
        logger.info("No rows to load into Snowflake.")
        return

    _create_clean_products_stage(cursor)
    _insert_stage_rows(cursor, df)
    _merge_staged_products(cursor)
    _insert_staged_snapshots(cursor)


def _ensure_current_schema(cursor):
    """
    Evolves existing Snowflake tables from the older platform/brand/review_count
    shape to the current cleaned dataset shape.
    """
    _add_column_if_missing(cursor, "dim_products", "product_name", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "brand", "VARCHAR(255)")
    _add_column_if_missing(cursor, "dim_products", "device_type", "VARCHAR(255)")
    _add_column_if_missing(cursor, "dim_products", "model_number", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "color", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "screen_size", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "ram_memory", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "storage_capacity", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "processor", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "gpu", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "operating_system", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "display_resolution", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "connectivity", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "product_dimensions", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "item_weight", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "best_sellers_rank", "VARCHAR")
    _add_column_if_missing(cursor, "dim_products", "brand_validation_status", "VARCHAR(50)")
    _add_column_if_missing(cursor, "dim_products", "device_type_validation_status", "VARCHAR(50)")
    _add_column_if_missing(cursor, "dim_products", "data_quality_score", "NUMBER(5, 2)")
    _add_column_if_missing(cursor, "dim_products", "validation_notes", "VARCHAR")
    _add_column_if_missing(cursor, "fact_product_snapshots", "currency", "VARCHAR(10)")
    _add_column_if_missing(cursor, "fact_product_snapshots", "original_price", "NUMBER(10, 2)")
    _add_column_if_missing(cursor, "fact_product_snapshots", "discount_percent", "NUMBER(5, 2)")
    _add_column_if_missing(cursor, "fact_product_snapshots", "availability", "VARCHAR")
    _add_column_if_missing(cursor, "fact_product_snapshots", "seller", "VARCHAR")
    _add_column_if_missing(cursor, "fact_product_snapshots", "is_sponsored", "BOOLEAN")
    _add_column_if_missing(cursor, "fact_product_snapshots", "is_prime", "BOOLEAN")

    if _column_exists(cursor, "dim_products", "brand"):
        cursor.execute(
            """
            UPDATE dim_products
            SET device_type = brand
            WHERE device_type IS NULL AND brand IS NOT NULL
            """
        )

    _drop_column_if_exists(cursor, "dim_products", "platform")
    _drop_column_if_exists(cursor, "fact_product_snapshots", "review_count")
    _backfill_product_metadata(cursor)


def load_data_to_snowflake(clean_file_path):
    """
    Loads transformed product data into Snowflake when SNOWFLAKE_ENABLED=true.
    """
    if not _is_enabled():
        logger.info("Snowflake load skipped because SNOWFLAKE_ENABLED is not true.")
        return

    missing = _missing_env_vars()
    if missing:
        raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")

    if not os.path.exists(clean_file_path):
        raise FileNotFoundError(f"File not found: {clean_file_path}")

    df = pd.read_csv(clean_file_path)
    logger.info("Loading %s rows into Snowflake.", len(df))

    setup_snowflake_warehouse()

    conn = _connect_to_snowflake()
    try:
        conn.autocommit(False)
        cursor = conn.cursor()
        try:
            _create_tables(cursor)
            _ensure_current_schema(cursor)
            _load_staged_products(cursor, df)
            conn.commit()
            logger.info("Successfully loaded data into Snowflake.")
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    test_path = "/opt/airflow/data/clean_amazon_eg_products.csv"
    if os.path.exists(test_path):
        load_data_to_snowflake(test_path)
