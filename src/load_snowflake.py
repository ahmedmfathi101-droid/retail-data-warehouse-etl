import logging
import os
import re

import pandas as pd

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
            platform VARCHAR(50) NOT NULL,
            sku VARCHAR(100) NOT NULL,
            title VARCHAR NOT NULL,
            brand VARCHAR(255),
            product_url VARCHAR,
            image_url VARCHAR,
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
            rating NUMBER(3, 2),
            review_count NUMBER,
            snapshot_date DATE DEFAULT CURRENT_DATE(),
            snapshot_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def _upsert_product(cursor, row):
    params = {
        "platform": str(row["platform"]),
        "sku": str(row["sku"]),
        "title": str(row["title"]),
        "brand": _clean_value(row["brand"]),
        "product_url": _clean_value(row["product_url"]),
        "image_url": _clean_value(row["image_url"]),
    }

    cursor.execute(
        """
        MERGE INTO dim_products AS target
        USING (
            SELECT
                %(platform)s AS platform,
                %(sku)s AS sku,
                %(title)s AS title,
                %(brand)s AS brand,
                %(product_url)s AS product_url,
                %(image_url)s AS image_url
        ) AS source
        ON target.platform = source.platform AND target.sku = source.sku
        WHEN MATCHED THEN UPDATE SET
            title = source.title,
            brand = source.brand,
            product_url = source.product_url,
            image_url = source.image_url,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            platform, sku, title, brand, product_url, image_url
        ) VALUES (
            source.platform, source.sku, source.title, source.brand, source.product_url, source.image_url
        )
        """,
        params,
    )

    cursor.execute(
        """
        SELECT product_id
        FROM dim_products
        WHERE platform = %(platform)s AND sku = %(sku)s
        """,
        {"platform": params["platform"], "sku": params["sku"]},
    )
    return cursor.fetchone()[0]


def _insert_snapshot(cursor, product_id, row):
    cursor.execute(
        """
        INSERT INTO fact_product_snapshots (product_id, price, rating, review_count)
        VALUES (%(product_id)s, %(price)s, %(rating)s, %(review_count)s)
        """,
        {
            "product_id": product_id,
            "price": _clean_value(row["price"]),
            "rating": _clean_value(row["rating"]),
            "review_count": None if pd.isna(row["review_count"]) else int(row["review_count"]),
        },
    )


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
            for _, row in df.iterrows():
                product_id = _upsert_product(cursor, row)
                _insert_snapshot(cursor, product_id, row)
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
