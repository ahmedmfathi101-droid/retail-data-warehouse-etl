import json
import pandas as pd
import logging
import os
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CLEAN_COLUMNS = [
    'sku',
    'title',
    'Product Name',
    'price',
    'rating',
    'product_url',
    'image_url',
    'Device type',
]

PRODUCT_NAME_STOP_WORDS = {'for', 'to', 'up'}
PRODUCT_NAME_MAX_WORDS = 5
PRODUCT_NAME_TRAILING_STOP_WORDS = {
    'a',
    'an',
    'and',
    'as',
    'at',
    'but',
    'by',
    'for',
    'from',
    'in',
    'into',
    'nor',
    'of',
    'on',
    'onto',
    'or',
    'per',
    'so',
    'than',
    'the',
    'to',
    'up',
    'via',
    'vs',
    'with',
    'without',
    'yet',
}
PRODUCT_NAME_SEPARATOR_PATTERN = re.compile(r'[,:\-_]')
PRODUCT_NAME_NUMBER_PATTERN = re.compile(r'\d+(?:[.,]\d+)?')


def _is_trailing_product_name_noise(word):
    normalized_word = word.strip().lower()
    return (
        normalized_word in PRODUCT_NAME_TRAILING_STOP_WORDS
        or bool(PRODUCT_NAME_NUMBER_PATTERN.fullmatch(normalized_word))
    )


def _remove_trailing_product_name_noise(words):
    while words and _is_trailing_product_name_noise(words[-1]):
        words.pop()
    return words


def has_trailing_product_name_noise(product_name):
    if not isinstance(product_name, str) or not product_name.strip():
        return False
    return _is_trailing_product_name_noise(product_name.strip().split()[-1])


def product_name_word_count(product_name):
    if not isinstance(product_name, str) or not product_name.strip():
        return 0
    return len(product_name.strip().split())


def extract_product_name(title):
    """
    Builds a short product name from the title.

    The name is capped at five words and stops earlier when the title reaches
    compatibility/description separators such as "for", "to", "up", comma,
    dash, underscore, or colon.
    """
    if not isinstance(title, str):
        return None

    words = []
    for raw_token in title.strip().split():
        if PRODUCT_NAME_SEPARATOR_PATTERN.search(raw_token):
            before_separator = PRODUCT_NAME_SEPARATOR_PATTERN.split(raw_token, maxsplit=1)[0]
            before_separator = re.sub(r'^[^\w]+|[^\w]+$', '', before_separator)
            if before_separator and not _is_trailing_product_name_noise(before_separator):
                words.append(before_separator)
            if words:
                break
            continue

        token = raw_token.strip()
        cleaned_token = re.sub(r'^[^\w]+|[^\w]+$', '', token)
        if not cleaned_token:
            continue

        if cleaned_token.lower() in PRODUCT_NAME_STOP_WORDS:
            if words:
                break
            continue

        words.append(cleaned_token)
        if len(words) == PRODUCT_NAME_MAX_WORDS:
            break

    words = _remove_trailing_product_name_noise(words)
    return ' '.join(words) if words else None


def transform_amazon_eg_data(input_file_path):
    """
    Reads raw JSON from the Amazon EG scraper, cleans it, and outputs a cleaned CSV.
    """
    logger.info(f"Transforming data from {input_file_path}")
    
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            
        if not raw_data:
            logger.warning("No items found in the raw data to transform.")
            # Create an empty CSV with correct headers so load.py doesn't fail
            df_empty = pd.DataFrame(columns=CLEAN_COLUMNS)
            output_file_path = input_file_path.replace('raw_', 'clean_').replace('.json', '.csv')
            df_empty.to_csv(output_file_path, index=False)
            return output_file_path
            
        df = pd.DataFrame(raw_data)
        
        # Clean Price (e.g., "15000.50" -> float)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Clean Rating (e.g., "4.5 out of 5 stars" -> 4.5)
        def extract_rating(rating_str):
            if not isinstance(rating_str, str): return None
            match = re.search(r'([\d.]+)', rating_str)
            return float(match.group(1)) if match else None
            
        df['rating_score'] = df['rating'].apply(extract_rating)
        
        # Map to DW schema
        df['sku'] = df['asin']
        df['Product Name'] = df['title'].apply(extract_product_name)
        df['Device type'] = df['category'].str.capitalize()
        
        # Select and rename columns
        cols_to_keep = {
            'sku': 'sku',
            'title': 'title',
            'Product Name': 'Product Name',
            'price': 'price',
            'rating_score': 'rating',
            'product_url': 'product_url',
            'image_url': 'image_url',
            'Device type': 'Device type',
        }
        
        # Ensure all columns exist before renaming (in case scraper missed something)
        for col in cols_to_keep.keys():
            if col not in df.columns:
                df[col] = None
                
        df = df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
        
        # Drop rows without an SKU or Price
        df = df.dropna(subset=['sku', 'price'])

        duplicate_count = df.duplicated(subset=['sku']).sum()
        if duplicate_count:
            logger.warning(f"Dropping {duplicate_count} duplicate sku rows from the cleaned batch.")
            df = df.drop_duplicates(subset=['sku'], keep='first')
        
        output_file_path = input_file_path.replace('raw_', 'clean_').replace('.json', '.csv')
        df.to_csv(output_file_path, index=False)
        
        logger.info(f"Cleaned data saved to {output_file_path}. Final row count: {len(df)}")
        return output_file_path

    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        raise

if __name__ == "__main__":
    test_path = "/opt/airflow/data/raw_amazon_eg_products.json"
    if os.path.exists(test_path):
        transform_amazon_eg_data(test_path)
