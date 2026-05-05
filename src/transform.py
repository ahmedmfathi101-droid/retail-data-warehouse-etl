import json
import pandas as pd
import logging
import os
import re

from src.ai_validation import validate_product_record

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CLEAN_COLUMNS = [
    'sku',
    'title',
    'Product Name',
    'brand',
    'price',
    'currency',
    'original_price',
    'discount_percent',
    'rating',
    'availability',
    'seller',
    'is_sponsored',
    'is_prime',
    'model_number',
    'color',
    'screen_size',
    'ram_memory',
    'storage_capacity',
    'processor',
    'gpu',
    'operating_system',
    'display_resolution',
    'connectivity',
    'product_dimensions',
    'item_weight',
    'best_sellers_rank',
    'product_url',
    'image_url',
    'Device type',
    'brand_validation_status',
    'device_type_validation_status',
    'data_quality_score',
    'validation_notes',
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
PRICE_NUMBER_PATTERN = re.compile(r'[\d,.]+')


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


def clean_text(value):
    if value is None or pd.isna(value):
        return None
    text = re.sub(r'\s+', ' ', str(value)).strip()
    return text or None


def clean_price(value):
    text = clean_text(value)
    if not text:
        return None

    matches = PRICE_NUMBER_PATTERN.findall(text)
    if not matches:
        return None

    number = matches[0].replace(',', '')
    if number.count('.') > 1:
        number = number.replace('.', '')

    try:
        return float(number)
    except ValueError:
        return None


def extract_currency(*values):
    text = ' '.join(clean_text(value) or '' for value in values)
    if re.search(r'\bEGP\b|ج\.م|جنيه', text, re.I):
        return 'EGP'
    return 'EGP' if text else None


def clean_bool(value):
    if isinstance(value, bool):
        return value
    text = clean_text(value)
    if not text:
        return False
    return text.lower() in {'1', 'true', 'yes', 'y'}


def calculate_discount_percent(price, original_price):
    if price is None or original_price is None or original_price <= 0 or price > original_price:
        return None
    return round(((original_price - price) / original_price) * 100, 2)


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
        
        # Clean Price fields (e.g., "EGP 15,000.50" -> 15000.50)
        df['price'] = df['price'].apply(clean_price)
        if 'original_price' not in df.columns:
            df['original_price'] = None
        df['original_price'] = df['original_price'].apply(clean_price)
        df['currency'] = df.apply(lambda row: extract_currency(row.get('price'), row.get('original_price')), axis=1)
        df['discount_percent'] = df.apply(
            lambda row: calculate_discount_percent(row.get('price'), row.get('original_price')),
            axis=1,
        )
        
        # Clean Rating (e.g., "4.5 out of 5 stars" -> 4.5)
        def extract_rating(rating_str):
            if isinstance(rating_str, (int, float)) and not pd.isna(rating_str):
                return float(rating_str)
            if not isinstance(rating_str, str): return None
            match = re.search(r'([\d.]+)', rating_str)
            return float(match.group(1)) if match else None
            
        df['rating_score'] = df['rating'].apply(extract_rating)
        
        # Map to DW schema
        df['sku'] = df['asin']
        df['Product Name'] = df['title'].apply(extract_product_name)
        df['Device type'] = df['category'].str.capitalize()
        for optional_col in [
            'brand',
            'manufacturer',
            'model_number',
            'color',
            'screen_size',
            'ram_memory',
            'storage_capacity',
            'processor',
            'gpu',
            'operating_system',
            'display_resolution',
            'connectivity',
            'product_dimensions',
            'item_weight',
            'best_sellers_rank',
            'availability',
            'seller',
            'is_sponsored',
            'is_prime',
        ]:
            if optional_col not in df.columns:
                df[optional_col] = None

        validation_results = df.apply(lambda row: validate_product_record(row.to_dict()), axis=1)
        df['brand'] = validation_results.apply(lambda result: result['brand'])
        df['Device type'] = validation_results.apply(lambda result: result['Device type'])
        df['brand_validation_status'] = validation_results.apply(lambda result: result['brand_validation_status'])
        df['device_type_validation_status'] = validation_results.apply(
            lambda result: result['device_type_validation_status']
        )
        df['data_quality_score'] = validation_results.apply(lambda result: result['data_quality_score'])
        df['validation_notes'] = validation_results.apply(lambda result: result['validation_notes'])
        df['is_sponsored'] = df['is_sponsored'].apply(clean_bool)
        df['is_prime'] = df['is_prime'].apply(clean_bool)
        
        # Select and rename columns
        cols_to_keep = {
            'sku': 'sku',
            'title': 'title',
            'Product Name': 'Product Name',
            'brand': 'brand',
            'price': 'price',
            'currency': 'currency',
            'original_price': 'original_price',
            'discount_percent': 'discount_percent',
            'rating_score': 'rating',
            'availability': 'availability',
            'seller': 'seller',
            'is_sponsored': 'is_sponsored',
            'is_prime': 'is_prime',
            'model_number': 'model_number',
            'color': 'color',
            'screen_size': 'screen_size',
            'ram_memory': 'ram_memory',
            'storage_capacity': 'storage_capacity',
            'processor': 'processor',
            'gpu': 'gpu',
            'operating_system': 'operating_system',
            'display_resolution': 'display_resolution',
            'connectivity': 'connectivity',
            'product_dimensions': 'product_dimensions',
            'item_weight': 'item_weight',
            'best_sellers_rank': 'best_sellers_rank',
            'product_url': 'product_url',
            'image_url': 'image_url',
            'Device type': 'Device type',
            'brand_validation_status': 'brand_validation_status',
            'device_type_validation_status': 'device_type_validation_status',
            'data_quality_score': 'data_quality_score',
            'validation_notes': 'validation_notes',
        }
        
        # Ensure all columns exist before renaming (in case scraper missed something)
        for col in cols_to_keep.keys():
            if col not in df.columns:
                df[col] = None
                
        df = df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
        text_columns = [
            'title',
            'Product Name',
            'brand',
            'currency',
            'availability',
            'seller',
            'model_number',
            'color',
            'screen_size',
            'ram_memory',
            'storage_capacity',
            'processor',
            'gpu',
            'operating_system',
            'display_resolution',
            'connectivity',
            'product_dimensions',
            'item_weight',
            'best_sellers_rank',
            'product_url',
            'image_url',
            'Device type',
            'brand_validation_status',
            'device_type_validation_status',
            'validation_notes',
        ]
        for column in text_columns:
            df[column] = df[column].apply(clean_text)
        
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
