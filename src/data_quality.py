import json
import logging
import os
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

from src.load_snowflake import _connect_to_snowflake, _is_enabled as snowflake_is_enabled
from src.transform import (
    INFO_UNAVAILABLE_TEXT,
    INVALID_AVAILABILITY_PATTERN,
    MISSING_TEXT_MARKERS,
    PRODUCT_NAME_MAX_WORDS,
    has_trailing_product_name_noise,
    product_name_word_count,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


REQUIRED_COLUMNS = [
    "sku",
    "title",
    "Product Name",
    "brand",
    "price",
    "original_price",
    "discount_percent",
    "rating",
    "availability",
    "seller",
    "product_url",
    "image_url",
    "Device type",
]


def _data_dir():
    return os.getenv("DATA_DIR") or ("data" if os.name == "nt" else "/opt/airflow/data")


def _write_report(report):
    os.makedirs(_data_dir(), exist_ok=True)
    report_path = os.path.join(_data_dir(), "data_quality_report.json")
    with open(report_path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, default=str)
    logger.info("Data quality report written to %s", report_path)
    return report_path


def validate_clean_file(clean_file_path):
    """
    Validates transformed product data before loading it into the warehouse.
    Raises ValueError when critical quality rules fail.
    """
    if not os.path.exists(clean_file_path):
        raise FileNotFoundError(f"Clean file not found: {clean_file_path}")

    df = pd.read_csv(clean_file_path)
    min_rows = int(os.getenv("DQ_MIN_ROWS", "1"))
    failures = []
    warnings = []

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        failures.append(f"Missing required columns: {', '.join(missing_columns)}")

    row_count = len(df)
    if row_count < min_rows:
        failures.append(f"Row count {row_count} is below DQ_MIN_ROWS={min_rows}")

    if not missing_columns and row_count > 0:
        required_non_null = ["sku", "title", "Product Name", "price", "Device type"]
        null_counts = df[required_non_null].isna().sum().to_dict()
        bad_nulls = {key: int(value) for key, value in null_counts.items() if value > 0}
        if bad_nulls:
            failures.append(f"Null values found in required fields: {bad_nulls}")

        null_cell_counts = df.isna().sum()
        null_cells = {key: int(value) for key, value in null_cell_counts.to_dict().items() if value > 0}
        if null_cells:
            failures.append(f"Missing cells should be replaced with '{INFO_UNAVAILABLE_TEXT}': {null_cells}")

        text_columns = df.select_dtypes(include=["object", "string"]).columns
        bad_missing_markers = MISSING_TEXT_MARKERS - {INFO_UNAVAILABLE_TEXT.lower()}
        missing_marker_counts = {}
        blank_counts = {}
        for column in text_columns:
            normalized = df[column].astype(str).str.strip().str.lower()
            marker_count = int(normalized.isin(bad_missing_markers).sum())
            blank_count = int((normalized == "").sum())
            if marker_count:
                missing_marker_counts[column] = marker_count
            if blank_count:
                blank_counts[column] = blank_count
        if missing_marker_counts:
            failures.append(
                f"Old missing-value markers found instead of '{INFO_UNAVAILABLE_TEXT}': {missing_marker_counts}"
            )
        if blank_counts:
            failures.append(f"Blank text values found: {blank_counts}")

        invalid_availability_terms = int(
            df["availability"]
            .astype(str)
            .str.contains(INVALID_AVAILABILITY_PATTERN, na=False)
            .sum()
        )
        if invalid_availability_terms:
            failures.append(
                "Availability contains shipping/delivery terms that should not be used as stock status: "
                f"{invalid_availability_terms}"
            )

        blank_product_names = int((df["Product Name"].astype(str).str.strip() == "").sum())
        if blank_product_names:
            failures.append(f"Blank Product Name values found: {blank_product_names}")

        trailing_noise_count = int(df["Product Name"].apply(has_trailing_product_name_noise).sum())
        if trailing_noise_count:
            failures.append(
                "Product Name values ending with a preposition, conjunction, or standalone number "
                f"found: {trailing_noise_count}"
            )

        long_product_names = int((df["Product Name"].apply(product_name_word_count) > PRODUCT_NAME_MAX_WORDS).sum())
        if long_product_names:
            failures.append(
                f"Product Name values longer than {PRODUCT_NAME_MAX_WORDS} words found: {long_product_names}"
            )

        duplicated_skus = int(df.duplicated(subset=["sku"]).sum())
        if duplicated_skus:
            warnings.append(f"Duplicate sku rows in current batch: {duplicated_skus}")

        negative_prices = int((df["price"] < 0).sum())
        if negative_prices:
            failures.append(f"Negative prices found: {negative_prices}")

        invalid_ratings = int((df["rating"].notna() & ~df["rating"].between(0, 5)).sum())
        if invalid_ratings:
            failures.append(f"Ratings outside 0-5 range found: {invalid_ratings}")

        invalid_original_prices = int(
            (
                df["original_price"].notna()
                & df["price"].notna()
                & (df["original_price"] < df["price"])
            ).sum()
        )
        if invalid_original_prices:
            warnings.append(f"Original price lower than current price rows: {invalid_original_prices}")

        invalid_discount_percent = int(
            (
                df["discount_percent"].notna()
                & ~df["discount_percent"].between(0, 100)
            ).sum()
        )
        if invalid_discount_percent:
            failures.append(f"Discount percent outside 0-100 range found: {invalid_discount_percent}")

    report = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_path": clean_file_path,
        "row_count": row_count,
        "min_rows": min_rows,
        "required_columns": REQUIRED_COLUMNS,
        "failures": failures,
        "warnings": warnings,
        "status": "failed" if failures else "passed",
    }
    _write_report(report)

    if failures:
        raise ValueError("Data quality validation failed: " + "; ".join(failures))

    logger.info("Data quality validation passed for %s rows.", row_count)
    return report


def _check_postgres_freshness(max_age_hours):
    conn_str = os.getenv("DW_CONN_STR", "postgresql+psycopg2://dw_user:dw_pass@postgres/retail_dw")
    engine = create_engine(conn_str)
    query = text(
        """
        SELECT
            MAX(snapshot_timestamp) AS latest_snapshot,
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(snapshot_timestamp))) / 3600 AS age_hours
        FROM fact_product_snapshots
        """
    )

    with engine.begin() as conn:
        row = conn.execute(query).mappings().first()

    latest_snapshot = row["latest_snapshot"] if row else None
    age_hours = row["age_hours"] if row else None
    if latest_snapshot is None:
        raise ValueError("PostgreSQL freshness check failed: no snapshots found.")
    if float(age_hours) > max_age_hours:
        raise ValueError(
            f"PostgreSQL freshness check failed: latest snapshot is {float(age_hours):.2f} hours old."
        )

    logger.info("PostgreSQL freshness passed. Latest snapshot: %s", latest_snapshot)
    return {"warehouse": "postgres", "latest_snapshot": latest_snapshot, "age_hours": float(age_hours)}


def _check_snowflake_freshness(max_age_hours):
    if not snowflake_is_enabled():
        logger.info("Snowflake freshness skipped because SNOWFLAKE_ENABLED is not true.")
        return {"warehouse": "snowflake", "status": "skipped"}

    conn = _connect_to_snowflake()
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    MAX(snapshot_timestamp) AS latest_snapshot,
                    DATEDIFF('hour', MAX(snapshot_timestamp), CURRENT_TIMESTAMP()) AS age_hours
                FROM fact_product_snapshots
                """
            )
            latest_snapshot, age_hours = cursor.fetchone()
        finally:
            cursor.close()
    finally:
        conn.close()

    if latest_snapshot is None:
        raise ValueError("Snowflake freshness check failed: no snapshots found.")
    if float(age_hours) > max_age_hours:
        raise ValueError(
            f"Snowflake freshness check failed: latest snapshot is {float(age_hours):.2f} hours old."
        )

    logger.info("Snowflake freshness passed. Latest snapshot: %s", latest_snapshot)
    return {"warehouse": "snowflake", "latest_snapshot": latest_snapshot, "age_hours": float(age_hours)}


def check_warehouse_freshness():
    """
    Verifies that warehouse snapshots were updated recently after the load step.
    """
    max_age_hours = float(os.getenv("DATA_FRESHNESS_MAX_HOURS", "30"))
    results = [
        _check_postgres_freshness(max_age_hours),
        _check_snowflake_freshness(max_age_hours),
    ]
    logger.info("Warehouse freshness checks completed: %s", results)
    return results
