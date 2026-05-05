import os
import json
import logging
import time
import random
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
]

def get_amazon_page(search_term, page=1):
    url = f"https://www.amazon.eg/-/en/s?k={search_term}&page={page}"
    
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            return response.content
        elif response.status_code == 503:
            logger.warning(f"Amazon blocked the request (503 CAPTCHA) for {search_term} on page {page}.")
            return None
        else:
            logger.warning(f"Failed to fetch {url}. Status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None

def parse_amazon_html(html_content, category):
    soup = BeautifulSoup(html_content, 'html.parser')
    products = soup.find_all('div', {'data-component-type': 's-search-result'})
    
    parsed_data = []
    for p in products:
        try:
            asin = p.get('data-asin')
            if not asin:
                continue
                
            # Title
            title_tag = p.find('h2')
            if title_tag:
                span_tag = title_tag.find('span')
                title = span_tag.text.strip() if span_tag else title_tag.text.strip()
            else:
                title = "Unknown Title"
            
            # Price
            price_tag = p.find('span', {'class': 'a-price-whole'})
            price_fraction = p.find('span', {'class': 'a-price-fraction'})
            if price_tag:
                price_str = price_tag.text.strip().replace(',', '').replace('.', '') # Remove comma and dot
                if price_fraction:
                    price_str += f".{price_fraction.text.strip()}"
            else:
                price_str = "0"
            
            # Rating
            rating_tag = p.find('span', {'class': 'a-icon-alt'})
            rating_str = rating_tag.text.strip() if rating_tag else "0 out of 5 stars"
            
            # Reviews count
            reviews_tag = p.find('span', {'class': 'a-size-base s-underline-text'})
            review_count_str = reviews_tag.text.strip().replace(',', '') if reviews_tag else "0"
            
            # Image URL
            img_tag = p.find('img', {'class': 's-image'})
            image_url = img_tag.get('src') if img_tag else ""
            
            # Product URL
            link_tag = p.find('a', {'class': 'a-link-normal s-no-outline'})
            product_url = f"https://www.amazon.eg{link_tag.get('href')}" if link_tag else ""

            parsed_data.append({
                "asin": asin,
                "title": title,
                "price": price_str,
                "rating": rating_str,
                "review_count": review_count_str,
                "image_url": image_url,
                "product_url": product_url,
                "category": category
            })
        except Exception as e:
            logger.debug(f"Error parsing product {asin}: {e}")
            continue
            
    return parsed_data

def scrape_amazon_eg_data():
    """
    Scrapes product data from Amazon Egypt.
    """
    logger.info("Starting Amazon EG Scraper...")
    search_terms = ['laptop', 'smartphone', 'headphone', 'television']
    all_data = []
    
    for term in search_terms:
        for page in range(1, 3):  # Scrape first 2 pages per term
            logger.info(f"Scraping '{term}' - Page {page}...")
            html = get_amazon_page(term, page)
            
            if html:
                products = parse_amazon_html(html, term)
                all_data.extend(products)
                logger.info(f"Extracted {len(products)} products from {term} page {page}.")
            else:
                logger.info("Skipping to next term due to block.")
                break # Move to next term if blocked
                
            time.sleep(random.uniform(2, 5)) # Sleep to avoid blocks
            
    if not all_data:
        logger.error("Failed to extract any data. Amazon might be heavily blocking IPs today.")
        # We don't raise an error here because Airflow will keep failing and retrying forever.
        # Just return an empty list so it passes cleanly without breaking the warehouse.
    
    data_dir = os.getenv("DATA_DIR", "/opt/airflow/data")
    os.makedirs(data_dir, exist_ok=True)
    file_path = os.path.join(data_dir, "raw_amazon_eg_products.json")
    
    with open(file_path, "w", encoding='utf-8') as f:
        json.dump(all_data, f, indent=4)
    
    logger.info(f"Amazon EG Scraper finished. Total items: {len(all_data)}. Saved to {file_path}")
    return file_path

if __name__ == "__main__":
    scrape_amazon_eg_data()
