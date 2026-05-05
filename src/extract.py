import os
import json
import logging
import time
import random
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urljoin, urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
]


def _request_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }


def _clean_text(value):
    return " ".join(value.split()) if isinstance(value, str) else ""


def _is_truthy_env(name, default="false"):
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes"}


def fetch_url(url, context):
    try:
        response = requests.get(url, headers=_request_headers(), timeout=20)
        if response.status_code == 200:
            return response.content
        if response.status_code == 503:
            logger.warning("Amazon blocked the request (503 CAPTCHA) for %s.", context)
            return None
        logger.warning("Failed to fetch %s. Status: %s", url, response.status_code)
        return None
    except Exception as e:
        logger.error("Request error for %s: %s", context, e)
        return None


def get_amazon_page(search_term, page=1):
    url = f"https://www.amazon.eg/-/en/s?k={search_term}&page={page}"
    return fetch_url(url, f"{search_term} page {page}")


def normalize_product_url(url):
    if not url:
        return ""

    absolute_url = urljoin("https://www.amazon.eg", url)
    parsed_url = urlparse(absolute_url)
    query = parse_qs(parsed_url.query)
    if "url" in query and query["url"]:
        return urljoin("https://www.amazon.eg", query["url"][0])
    return absolute_url


def _text_from_first(element, selectors):
    for selector in selectors:
        tag = element.select_one(selector)
        if tag:
            return _clean_text(tag.get_text(" ", strip=True))
    return ""


def _parse_price_tags(product):
    price = _text_from_first(product, [".a-price .a-offscreen"])
    if not price:
        price_whole = product.find('span', {'class': 'a-price-whole'})
        price_fraction = product.find('span', {'class': 'a-price-fraction'})
        if price_whole:
            price = price_whole.text.strip()
            if price_fraction:
                price += f".{price_fraction.text.strip()}"

    original_price = ""
    for candidate in product.select(".a-price.a-text-price .a-offscreen, .a-text-price .a-offscreen"):
        candidate_text = _clean_text(candidate.get_text(" ", strip=True))
        if candidate_text and candidate_text != price:
            original_price = candidate_text
            break

    return price or "0", original_price


def _extract_listing_badges(product):
    return [
        _clean_text(tag.get_text(" ", strip=True))
        for tag in product.select(".a-badge-text, .s-label-popover-default")
        if _clean_text(tag.get_text(" ", strip=True))
    ]


def _extract_spec_table(soup):
    specs = {}

    for row in soup.select(
        "#productOverview_feature_div tr, "
        "#productDetails_techSpec_section_1 tr, "
        "#productDetails_detailBullets_sections1 tr, "
        "#prodDetails tr"
    ):
        key_tag = row.find(["th", "td"])
        value_tag = key_tag.find_next_sibling(["td", "th"]) if key_tag else None
        key = _clean_text(key_tag.get_text(" ", strip=True)) if key_tag else ""
        value = _clean_text(value_tag.get_text(" ", strip=True)) if value_tag else ""
        if key and value:
            specs[key] = value

    for bullet in soup.select("#detailBullets_feature_div li"):
        text = _clean_text(bullet.get_text(" ", strip=True))
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        key = _clean_text(key.replace("\u200e", ""))
        value = _clean_text(value.replace("\u200e", ""))
        if key and value:
            specs[key] = value

    return specs


def _get_spec(specs, candidates):
    normalized_specs = {re.sub(r"[^a-z0-9]+", " ", key.lower()).strip(): value for key, value in specs.items()}
    for candidate in candidates:
        normalized_candidate = re.sub(r"[^a-z0-9]+", " ", candidate.lower()).strip()
        for key, value in normalized_specs.items():
            if normalized_candidate in key:
                return value
    return ""


def parse_product_detail_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    specs = _extract_spec_table(soup)

    byline = _text_from_first(soup, ["#bylineInfo"])
    brand_from_byline = ""
    if byline:
        brand_from_byline = re.sub(r"^(visit the|brand:|store:)\s+", "", byline, flags=re.I).replace(" Store", "")

    bullet_points = [
        _clean_text(tag.get_text(" ", strip=True))
        for tag in soup.select("#feature-bullets li span.a-list-item")
        if _clean_text(tag.get_text(" ", strip=True))
    ]

    return {
        "brand": _get_spec(specs, ["brand"]) or brand_from_byline,
        "manufacturer": _get_spec(specs, ["manufacturer"]),
        "model_number": _get_spec(specs, ["item model number", "model number", "model name"]),
        "color": _get_spec(specs, ["color", "colour"]),
        "screen_size": _get_spec(specs, ["standing screen display size", "screen size", "display size"]),
        "ram_memory": _get_spec(specs, ["ram", "memory size", "installed ram"]),
        "storage_capacity": _get_spec(specs, ["hard drive size", "memory storage capacity", "storage capacity"]),
        "processor": _get_spec(specs, ["processor", "cpu"]),
        "gpu": _get_spec(specs, ["graphics coprocessor", "graphics card", "gpu"]),
        "operating_system": _get_spec(specs, ["operating system"]),
        "display_resolution": _get_spec(specs, ["resolution", "display resolution"]),
        "connectivity": _get_spec(specs, ["connectivity", "wireless communication technology"]),
        "product_dimensions": _get_spec(specs, ["product dimensions", "package dimensions"]),
        "item_weight": _get_spec(specs, ["item weight", "weight"]),
        "best_sellers_rank": _get_spec(specs, ["best sellers rank", "best seller rank"]),
        "availability": _text_from_first(soup, ["#availability"]),
        "seller": _text_from_first(soup, ["#sellerProfileTriggerId", "#merchant-info"]),
        "product_description": _text_from_first(soup, ["#productDescription"]),
        "bullet_points": bullet_points[:8],
        "product_specs": specs,
    }


def enrich_with_product_details(product):
    product_url = product.get("product_url")
    if not product_url:
        return product

    html = fetch_url(product_url, f"detail {product.get('asin')}")
    if not html:
        return product

    detail_fields = parse_product_detail_html(html)
    for key, value in detail_fields.items():
        if value and not product.get(key):
            product[key] = value
    return product

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
            price_str, original_price_str = _parse_price_tags(p)
            
            # Rating
            rating_tag = p.find('span', {'class': 'a-icon-alt'})
            rating_str = rating_tag.text.strip() if rating_tag else "0 out of 5 stars"
            
            # Image URL
            img_tag = p.find('img', {'class': 's-image'})
            image_url = img_tag.get('src') if img_tag else ""
            
            # Product URL
            link_tag = p.find('a', {'class': 'a-link-normal s-no-outline'})
            product_url = normalize_product_url(link_tag.get('href')) if link_tag else ""
            badges = _extract_listing_badges(p)

            parsed_data.append({
                "asin": asin,
                "title": title,
                "price": price_str,
                "original_price": original_price_str,
                "rating": rating_str,
                "image_url": image_url,
                "product_url": product_url,
                "category": category,
                "source_search_term": category,
                "listing_badges": badges,
                "is_sponsored": bool(p.find(string=re.compile(r"Sponsored", re.I))),
                "is_prime": bool(p.select_one(".s-prime, .a-icon-prime, [aria-label*='Prime']")),
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
    search_terms = os.getenv("SCRAPE_SEARCH_TERMS", "laptop,smartphone,headphone,television")
    search_terms = [term.strip() for term in search_terms.split(",") if term.strip()]
    search_pages = int(os.getenv("SCRAPE_SEARCH_PAGES", "2"))
    enrich_details = _is_truthy_env("SCRAPE_DETAIL_PAGES", "true")
    detail_limit = int(os.getenv("SCRAPE_DETAIL_LIMIT_PER_RUN", "80"))
    detail_delay_min = float(os.getenv("SCRAPE_DETAIL_DELAY_MIN_SECONDS", "1"))
    detail_delay_max = float(os.getenv("SCRAPE_DETAIL_DELAY_MAX_SECONDS", "2.5"))
    all_data = []
    detail_count = 0
    
    for term in search_terms:
        for page in range(1, search_pages + 1):
            logger.info(f"Scraping '{term}' - Page {page}...")
            html = get_amazon_page(term, page)
            
            if html:
                products = parse_amazon_html(html, term)
                if enrich_details:
                    for product in products:
                        if detail_count >= detail_limit:
                            break
                        enrich_with_product_details(product)
                        detail_count += 1
                        time.sleep(random.uniform(detail_delay_min, detail_delay_max))

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
