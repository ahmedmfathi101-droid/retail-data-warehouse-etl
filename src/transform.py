import json
import pandas as pd
import logging
import os
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            df_empty = pd.DataFrame(columns=['sku', 'title', 'price', 'rating', 'review_count', 'product_url', 'image_url', 'brand', 'platform'])
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
        
        # Clean Review Count (e.g., "1,234" or "1234" -> 1234)
        df['review_count'] = pd.to_numeric(df['review_count'], errors='coerce')
        
        # Map to DW schema
        df['sku'] = df['asin']
        df['brand'] = df['category'].str.capitalize()  # Scraped category used as brand for simplicity
        df['platform'] = 'Amazon EG'
        
        # Select and rename columns
        cols_to_keep = {
            'sku': 'sku',
            'title': 'title',
            'price': 'price',
            'rating_score': 'rating',
            'review_count': 'review_count',
            'product_url': 'product_url',
            'image_url': 'image_url',
            'brand': 'brand',
            'platform': 'platform'
        }
        
        # Ensure all columns exist before renaming (in case scraper missed something)
        for col in cols_to_keep.keys():
            if col not in df.columns:
                df[col] = None
                
        df = df[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
        
        # Drop rows without an SKU or Price
        df = df.dropna(subset=['sku', 'price'])

        duplicate_count = df.duplicated(subset=['platform', 'sku']).sum()
        if duplicate_count:
            logger.warning(f"Dropping {duplicate_count} duplicate platform/sku rows from the cleaned batch.")
            df = df.drop_duplicates(subset=['platform', 'sku'], keep='first')
        
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
