"""
Standalone Amazon EG ETL runner.

Runs the same core steps as the Airflow DAG without starting Airflow:

  1. Extract: scrape listing and detail pages from Amazon Egypt.
  2. Transform: clean raw JSON into the final CSV shape.
  3. Validate: run the data quality checks.
  4. Load: upsert PostgreSQL and Snowflake warehouses.

Usage:
    python run_etl.py
    python run_etl.py --skip-scrape
    python run_etl.py --load-only
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=env_path, override=True)
    print(f"[OK] Loaded environment from {env_path}")
else:
    print("[WARN] No .env file found; relying on system environment variables.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_etl")

project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

DATA_DIR = project_root / "data"
RAW_JSON = DATA_DIR / "raw_amazon_eg_products.json"
CLEAN_CSV = DATA_DIR / "clean_amazon_eg_products.csv"


def _separator(label):
    line = "=" * 60
    logger.info(line)
    logger.info("  %s", label)
    logger.info(line)


def run_scrape():
    """Phase 1: scrape Amazon Egypt and save raw JSON."""
    _separator("STEP 1 / 4 : EXTRACT (scraping Amazon EG)")

    os.environ["DATA_DIR"] = str(DATA_DIR)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    from src.extract import scrape_amazon_eg_data

    t0 = time.time()
    output_path = scrape_amazon_eg_data()
    elapsed = time.time() - t0

    logger.info("[OK] Scraping complete in %.1f s -> %s", elapsed, output_path)
    return output_path


def run_transform(raw_json_path=None):
    """Phase 2: clean raw JSON and produce a CSV."""
    _separator("STEP 2 / 4 : TRANSFORM (cleaning data)")

    raw_path = raw_json_path or str(RAW_JSON)
    if not os.path.exists(raw_path):
        logger.error("Raw JSON not found: %s", raw_path)
        sys.exit(1)

    from src.transform import transform_amazon_eg_data

    t0 = time.time()
    output_path = transform_amazon_eg_data(raw_path)
    elapsed = time.time() - t0

    logger.info("[OK] Transform complete in %.1f s -> %s", elapsed, output_path)
    return output_path


def run_validate(clean_csv_path=None):
    """Phase 3: validate the clean CSV."""
    _separator("STEP 3 / 4 : VALIDATE (data quality checks)")

    csv_path = clean_csv_path or str(CLEAN_CSV)
    if not os.path.exists(csv_path):
        logger.error("Clean CSV not found: %s", csv_path)
        sys.exit(1)

    from src.data_quality import validate_clean_file

    t0 = time.time()
    report = validate_clean_file(csv_path)
    elapsed = time.time() - t0

    logger.info("[OK] Validation complete in %.1f s -> %s", elapsed, report["status"])
    return report


def run_load(clean_csv_path=None):
    """Phase 4: upsert clean CSV into PostgreSQL and Snowflake."""
    _separator("STEP 4 / 4 : LOAD (uploading to PostgreSQL and Snowflake)")

    csv_path = clean_csv_path or str(CLEAN_CSV)
    if not os.path.exists(csv_path):
        logger.error("Clean CSV not found: %s", csv_path)
        sys.exit(1)

    from src.load import load_data
    from src.load_snowflake import load_data_to_snowflake

    t0 = time.time()
    load_data(csv_path)
    load_data_to_snowflake(csv_path)
    elapsed = time.time() - t0

    logger.info("[OK] Warehouse load complete in %.1f s", elapsed)


def _check_required_env():
    required = ["DW_CONN_STR"]
    if os.getenv("SNOWFLAKE_ENABLED", "false").strip().lower() in {"1", "true", "yes"}:
        required.extend(
            [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER",
                "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE",
                "SNOWFLAKE_SCHEMA",
            ]
        )

    missing = [name for name in required if not os.getenv(name)]
    if missing:
        logger.error("Missing environment variables: %s", ", ".join(missing))
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Amazon EG ETL Pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scraping and re-use existing raw_amazon_eg_products.json.",
    )
    group.add_argument(
        "--load-only",
        action="store_true",
        help="Skip scraping and transform, then validate and load the existing clean CSV.",
    )
    args = parser.parse_args()

    logger.info(">>> Amazon EG ETL starting ...")
    logger.info("    Snowflake enabled : %s", os.getenv("SNOWFLAKE_ENABLED", "false"))
    logger.info("    Database          : %s", os.getenv("SNOWFLAKE_DATABASE", ""))
    logger.info("    Schema            : %s", os.getenv("SNOWFLAKE_SCHEMA", ""))
    logger.info("    Warehouse         : %s", os.getenv("SNOWFLAKE_WAREHOUSE", ""))

    _check_required_env()
    total_start = time.time()

    if args.load_only:
        logger.info("[MODE] Load-only: skipping scrape and transform.")
        run_validate()
        run_load()
    elif args.skip_scrape:
        logger.info("[MODE] Skip-scrape: re-using existing raw JSON.")
        csv_path = run_transform()
        run_validate(csv_path)
        run_load(csv_path)
    else:
        logger.info("[MODE] Full pipeline: scrape + transform + validate + load.")
        raw_path = run_scrape()
        csv_path = run_transform(raw_path)
        run_validate(csv_path)
        run_load(csv_path)

    total_elapsed = time.time() - total_start
    logger.info("")
    logger.info("[DONE] ETL pipeline finished in %.1f s", total_elapsed)
    logger.info("       dim_products and fact_product_snapshots updated.")


if __name__ == "__main__":
    main()
