import os
import json
import logging
import time
import random
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PRICE_NUMBER_PATTERN = re.compile(r'[\d,.]+')
INVALID_AVAILABILITY_PATTERN = re.compile(r'\b(?:ship|ships|shipping|delivery|deliver|delivered)\b', re.I)
CAPTCHA_MARKERS = (
    "opfcaptcha",
    "enter the characters you see below",
    "type the characters you see in this image",
    "/errors/validatecaptcha",
)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15'
]

DEFAULT_SEARCH_TERMS = [
    "laptop",
    "tablet",
    "ipad",
    "android tablet",
    "samsung tablet",
    "lenovo tablet",
    "huawei tablet",
    "xiaomi tablet",
    "smartphone",
    "mobile phone",
    "android phone",
    "iphone",
    "samsung phone",
    "xiaomi phone",
    "redmi phone",
    "oppo phone",
    "realme phone",
    "infinix phone",
    "tecno phone",
    "nokia phone",
    "honor phone",
    "huawei phone",
    "oneplus phone",
    "feature phone",
    "foldable phone",
]
DEFAULT_SEARCH_TERMS_TEXT = ",".join(DEFAULT_SEARCH_TERMS)
DETAIL_CACHE_FIELDS = (
    "brand",
    "manufacturer",
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
    "availability",
    "seller",
    "price",
    "original_price",
    "discount_percent",
    "product_description",
    "bullet_points",
    "product_specs",
)


def _request_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-AE,en-US;q=0.9,en;q=0.8,ar-EG;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.amazon.eg/-/en/s?k=laptop&language=en_AE',
    }


def _request_cookies():
    return {
        'lc-acbeg': 'en_AE',
        'i18n-prefs': 'EGP',
    }


def _clean_text(value):
    return " ".join(value.split()) if isinstance(value, str) else ""


def _clean_availability_text(value):
    text = _clean_text(value)
    if not text or INVALID_AVAILABILITY_PATTERN.search(text):
        return ""

    normalized = re.sub(r'[^a-z]+', '', text.lower())
    if normalized in {'instock', 'available'}:
        return 'In Stock'
    if normalized in {'outofstock', 'currentlyunavailable', 'unavailable'}:
        return 'Currently unavailable'
    return text


def _looks_like_captcha(html_content):
    if not html_content:
        return False
    text = html_content.decode("utf-8", errors="ignore").lower()
    return any(marker in text for marker in CAPTCHA_MARKERS)


def _is_truthy_env(name, default="false"):
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes"}


def fetch_url(url, context):
    try:
        response = requests.get(url, headers=_request_headers(), cookies=_request_cookies(), timeout=20)
        if response.status_code == 200:
            if _looks_like_captcha(response.content):
                logger.warning("Amazon returned a CAPTCHA page for %s.", context)
                return None
            return response.content
        if response.status_code == 503:
            logger.warning("Amazon blocked the request (503 CAPTCHA) for %s.", context)
            return None
        logger.warning("Failed to fetch %s. Status: %s", url, response.status_code)
        return None
    except Exception as e:
        logger.error("Request error for %s: %s", context, e)
        return None


def _candidate_search_urls(search_term, page=1):
    query = quote_plus(search_term)
    return [
        f"https://www.amazon.eg/-/en/s?k={query}&page={page}&language=en_AE",
        f"https://www.amazon.eg/s?k={query}&page={page}&language=en_AE",
        f"https://www.amazon.eg/-/en/s?i=electronics&k={query}&page={page}&language=en_AE",
    ]


def get_amazon_page(search_term, page=1):
    for url in _candidate_search_urls(search_term, page):
        html = fetch_url(url, f"{search_term} page {page}")
        if html:
            return html
    return None


def _write_json_file(data, file_path):
    temp_path = f"{file_path}.tmp"
    with open(temp_path, "w", encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    os.replace(temp_path, file_path)


def _load_json_list(file_path):
    if not os.path.exists(file_path):
        return []

    try:
        with open(file_path, "r", encoding="utf-8") as data_file:
            data = json.load(data_file)
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.warning("Could not load existing raw data %s: %s", file_path, exc)
        return []


def _product_identity(product):
    asin = _clean_text(product.get("asin")).upper()
    if asin:
        return f"asin:{asin}"

    product_url = normalize_product_url(product.get("product_url"))
    return f"url:{product_url}" if product_url else ""


def _merge_product_batches(existing_products, new_products):
    merged = {}
    order = []
    for product in [*existing_products, *new_products]:
        identity = _product_identity(product)
        if not identity:
            identity = f"row:{len(order)}"
        if identity not in merged:
            merged[identity] = {}
            order.append(identity)

        for key, value in product.items():
            if value not in (None, "", [], {}):
                merged[identity][key] = value

    return [merged[identity] for identity in order]


def normalize_product_url(url):
    if not url:
        return ""

    absolute_url = urljoin("https://www.amazon.eg", url)
    parsed_url = urlparse(absolute_url)
    query = parse_qs(parsed_url.query)
    if "url" in query and query["url"]:
        return urljoin("https://www.amazon.eg", query["url"][0])
    return absolute_url


def canonical_product_url(asin):
    asin = _clean_text(asin)
    return f"https://www.amazon.eg/-/en/dp/{asin}" if asin else ""


def offer_listing_url(asin):
    asin = _clean_text(asin)
    if not asin:
        return ""
    return f"https://www.amazon.eg/gp/offer-listing/{asin}/ref=dp_olp_ALL_mbc?ie=UTF8&condition=all&language=en_AE"


def _candidate_product_urls(product):
    canonical_url = canonical_product_url(product.get("asin"))
    urls = [canonical_url or normalize_product_url(product.get("product_url"))]
    unique_urls = []
    seen = set()
    for url in urls:
        if url and url not in seen:
            unique_urls.append(url)
            seen.add(url)
    return unique_urls


def _text_from_first(element, selectors):
    for selector in selectors:
        tag = element.select_one(selector)
        if tag:
            return _clean_text(tag.get_text(" ", strip=True))
    return ""


def _first_text_from_selectors(element, selectors):
    for selector in selectors:
        for tag in element.select(selector):
            text = _clean_text(tag.get_text(" ", strip=True))
            if text:
                return text
    return ""


def _price_text_from_selectors(element, selectors):
    for selector in selectors:
        for tag in element.select(selector):
            text = _clean_text(tag.get_text(" ", strip=True))
            if text and _clean_price(text) is not None:
                return text
    return ""


def _clean_price(value):
    text = _clean_text(value)
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


def _calculate_discount_percent(price, original_price):
    if price is None or original_price is None or original_price <= 0 or price > original_price:
        return None
    return round(((original_price - price) / original_price) * 100, 2)


def _extract_labeled_price(soup, label_pattern):
    label_regex = re.compile(label_pattern, flags=re.I)
    for label in soup.find_all(string=label_regex):
        container = label.find_parent(["span", "div", "td", "tr", "li"])
        if not container:
            continue

        for candidate in container.select("[aria-hidden='true'], .a-offscreen, .a-price, span"):
            candidate_text = _clean_text(candidate.get_text(" ", strip=True))
            if candidate_text and _clean_price(candidate_text) is not None:
                return candidate_text

        container_text = _clean_text(container.get_text(" ", strip=True))
        if _clean_price(container_text) is not None:
            return container_text

        for sibling in container.find_next_siblings(limit=3):
            sibling_text = _clean_text(sibling.get_text(" ", strip=True))
            if sibling_text and _clean_price(sibling_text) is not None:
                return sibling_text

    return ""


def _extract_page_asin(soup):
    selectors = [
        "input#ASIN",
        "input[name='ASIN']",
        "input[name='asin']",
        "[data-asin]",
    ]
    for selector in selectors:
        tag = soup.select_one(selector)
        if not tag:
            continue
        asin = tag.get("value") or tag.get("data-asin")
        asin = _clean_text(asin)
        if asin:
            return asin

    canonical = soup.select_one("link[rel='canonical']")
    href = canonical.get("href", "") if canonical else ""
    match = re.search(r"/(?:dp|product)/([A-Z0-9]{10})", href)
    return match.group(1) if match else ""


def _extract_offer_listing_price(soup):
    asin = _extract_page_asin(soup)
    if not asin:
        return ""

    offer_links = [
        f"a[href*='/offer-listing/{asin}']",
        f"a[href*='/gp/offer-listing/{asin}']",
        f"a[href*='offer-listing/{asin}']",
    ]
    for selector in offer_links:
        for link in soup.select(selector):
            for candidate in link.select(".a-price .a-offscreen, .a-price [aria-hidden='true'], .a-price"):
                candidate_text = _clean_text(candidate.get_text(" ", strip=True))
                if candidate_text and _clean_price(candidate_text) is not None:
                    return candidate_text
            link_text = _clean_text(link.get_text(" ", strip=True))
            if link_text and _clean_price(link_text) is not None:
                return link_text
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


def _extract_detail_prices(soup):
    price_scope = soup.select_one(
        "#corePriceDisplay_desktop_feature_div, "
        "#corePriceDisplay_mobile_feature_div, "
        "#corePrice_feature_div, "
        "#apex_desktop, "
        "#apex_mobile, "
        "#buybox"
    )
    price_text = _price_text_from_selectors(
        price_scope or soup,
        [
            ".priceToPay .a-offscreen",
            ".priceToPay [aria-hidden='true']",
            ".a-price .a-offscreen",
            "#priceblock_dealprice",
            "#priceblock_ourprice",
        ],
    ) or _extract_offer_listing_price(soup)
    original_price_text = _price_text_from_selectors(
        price_scope or soup,
        [
            ".basisPrice .a-offscreen",
            ".basisPrice [aria-hidden='true']",
            ".a-price.a-text-price .a-offscreen",
            ".a-price.a-text-price [aria-hidden='true']",
            ".a-text-price .a-offscreen",
            ".a-text-price [aria-hidden='true']",
            "[data-a-strike='true'] .a-offscreen",
            "[data-a-strike='true'] [aria-hidden='true']",
            "#listPrice",
        ],
    ) or _extract_labeled_price(price_scope or soup, r"\b(List Price|Was|RRP)\b") or _extract_labeled_price(
        soup,
        r"\b(List Price|Was|RRP)\b",
    )
    savings_text = _first_text_from_selectors(
        price_scope or soup,
        [
            ".savingsPercentage",
            "#regularprice_savings .a-color-price",
        ],
    ) or _first_text_from_selectors(soup, [".savingsPercentage"])

    price = _clean_price(price_text)
    original_price = _clean_price(original_price_text)
    discount_percent = None
    if savings_text:
        match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", savings_text)
        if match:
            discount_percent = float(match.group(1).replace(",", "."))
    if discount_percent is None:
        discount_percent = _calculate_discount_percent(price, original_price)

    return {
        "price": price,
        "original_price": original_price,
        "discount_percent": discount_percent,
    }


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


def _extract_availability(soup):
    availability = _first_text_from_selectors(
        soup,
        [
            "#availability",
            "#availabilityInsideBuyBox_feature_div",
            "#desktop_buybox #availability",
            "#outOfStock",
            "#buybox .a-color-success",
            "#buybox .a-color-state",
        ],
    )
    if availability:
        return _clean_availability_text(availability)

    page_text = _clean_text(soup.get_text(" ", strip=True))
    patterns = [
        r"(Only \d+ left in stock[^.]*\.)",
        r"(In Stock)",
        r"(Currently unavailable\.[^.]*\.)",
    ]
    for pattern in patterns:
        match = re.search(pattern, page_text, flags=re.I)
        if match:
            return _clean_availability_text(match.group(1))
    return ""


def _clean_seller_text(text):
    text = _clean_text(text)
    if not text:
        return ""
    text = re.sub(r"^(sold by|seller|shipper\s*/\s*seller)\s*:?\s*", "", text, flags=re.I)
    text = re.sub(r"\bshipper\s*/\s*seller\b\s*:?\s*", "", text, flags=re.I)
    text = re.sub(r"\s+and\s+fulfilled\s+by\s+amazon.*$", "", text, flags=re.I)
    text = re.sub(r"\s+fulfilled\s+by\s+amazon.*$", "", text, flags=re.I)
    text = re.sub(r"\s+returns?.*$", "", text, flags=re.I)
    text = _clean_text(text)
    words = text.split()
    if len(words) % 2 == 0 and words[: len(words) // 2] == words[len(words) // 2 :]:
        text = " ".join(words[: len(words) // 2])
    if text.lower() in {"amazon.eg", "amazon"}:
        return "Amazon.eg"
    return text


def _extract_seller_from_offer_display(soup):
    selectors = [
        "#merchantInfoFeature_feature_div .offer-display-feature-text-message",
        "#merchantInfoFeature_feature_div .odf-popover-overflow-wrap",
        "#offer-display-features [data-feature-name='merchantInfoFeature'] .offer-display-feature-text-message",
        "#offer-display-features [offer-display-feature-name='desktop-merchant-info'] .offer-display-feature-text-message",
        "#offer-display-features [data-csa-c-content-id='desktop-merchant-info'] .offer-display-feature-text-message",
        "#sourceMerchantInfoFeature_feature_div .offer-display-feature-text-message",
        "#dynamicSourceMerchantInfoFeature_feature_div .offer-display-feature-text-message",
    ]
    for selector in selectors:
        for tag in soup.select(selector):
            seller = _clean_seller_text(tag.get_text(" ", strip=True))
            if seller:
                return seller

    for container in soup.select(
        "#merchantInfoFeature_feature_div, "
        "[data-feature-name='merchantInfoFeature'], "
        "[offer-display-feature-name='desktop-merchant-info']"
    ):
        label = _clean_text(container.get_text(" ", strip=True))
        if re.search(r"shipper\s*/\s*seller|seller", label, flags=re.I):
            seller = _clean_seller_text(label)
            if seller:
                return seller
    return ""


def _extract_seller_from_tabular_buybox(soup):
    for row in soup.select("#tabular-buybox tr, #tabular-buybox .tabular-buybox-container"):
        row_text = _clean_text(row.get_text(" ", strip=True))
        if not re.search(r"sold by|seller", row_text, flags=re.I):
            continue
        link = row.select_one("a")
        if link:
            seller = _clean_seller_text(link.get_text(" ", strip=True))
            if seller:
                return seller
        value = row.select_one(".tabular-buybox-text, .a-column:last-child")
        if value:
            seller = _clean_seller_text(value.get_text(" ", strip=True))
            if seller:
                return seller
        seller = _clean_seller_text(row_text)
        if seller and not re.fullmatch(r"sold by|seller", seller, flags=re.I):
            return seller
    return ""


def _extract_seller_from_scripts(soup):
    script_text = "\n".join(script.string or script.get_text(" ", strip=True) for script in soup.select("script"))
    patterns = [
        r'"sellerName"\s*:\s*"([^"]+)"',
        r'"merchantName"\s*:\s*"([^"]+)"',
        r'"sellerDisplayName"\s*:\s*"([^"]+)"',
        r'"soldBy"\s*:\s*"([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, script_text)
        if match:
            raw_seller = match.group(1)
            try:
                raw_seller = json.loads(f'"{raw_seller}"')
            except json.JSONDecodeError:
                pass
            seller = _clean_seller_text(raw_seller)
            if seller:
                return seller
    return ""


def _extract_seller(soup):
    seller = _first_text_from_selectors(
        soup,
        [
            "#sellerProfileTriggerId",
            "#merchantInfoFeature_feature_div .offer-display-feature-text-message",
            "#offer-display-features [data-feature-name='merchantInfoFeature'] .offer-display-feature-text-message",
            "#merchant-info a",
            "#merchant-info",
            "#shipsFromSoldByInsideBuyBox_feature_div #sellerProfileTriggerId",
            "#offerDisplayGroup #sellerProfileTriggerId",
        ],
    )
    seller = _clean_seller_text(seller)
    if seller:
        return seller

    seller = _extract_seller_from_offer_display(soup)
    if seller:
        return seller

    seller = _extract_seller_from_tabular_buybox(soup)
    if seller:
        return seller

    return _extract_seller_from_scripts(soup)


def parse_product_detail_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    specs = _extract_spec_table(soup)
    prices = _extract_detail_prices(soup)

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
        "availability": _extract_availability(soup),
        "seller": _extract_seller(soup),
        "price": prices["price"],
        "original_price": prices["original_price"],
        "discount_percent": prices["discount_percent"],
        "product_description": _text_from_first(soup, ["#productDescription"]),
        "bullet_points": bullet_points[:8],
        "product_specs": specs,
    }


def enrich_with_product_details(product):
    product_urls = _candidate_product_urls(product)
    if not product_urls:
        return False

    html = None
    for product_url in product_urls:
        html = fetch_url(product_url, f"detail {product.get('asin')}")
        if html:
            break
    if not html:
        return False

    detail_fields = parse_product_detail_html(html)
    for key, value in detail_fields.items():
        if value and not product.get(key):
            product[key] = value
    return True


def _detail_cache_key(product):
    return _clean_text(product.get("asin")).upper()


def _cache_detail_fields(product):
    return {key: product.get(key) for key in DETAIL_CACHE_FIELDS if product.get(key)}


def _apply_detail_cache(product, detail_fields):
    for key, value in detail_fields.items():
        if value and not product.get(key):
            product[key] = value


def _load_detail_cache(file_path):
    if not os.path.exists(file_path):
        return {}

    try:
        with open(file_path, "r", encoding="utf-8") as cache_file:
            data = json.load(cache_file)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning("Could not load detail cache %s: %s", file_path, exc)
        return {}

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
    search_terms = os.getenv("SCRAPE_SEARCH_TERMS", DEFAULT_SEARCH_TERMS_TEXT)
    search_terms = [term.strip() for term in search_terms.split(",") if term.strip()]
    search_pages = int(os.getenv("SCRAPE_SEARCH_PAGES", "2"))
    enrich_details = _is_truthy_env("SCRAPE_DETAIL_PAGES", "true")
    detail_limit = int(os.getenv("SCRAPE_DETAIL_LIMIT_PER_RUN", "0"))
    detail_delay_min = float(os.getenv("SCRAPE_DETAIL_DELAY_MIN_SECONDS", "1"))
    detail_delay_max = float(os.getenv("SCRAPE_DETAIL_DELAY_MAX_SECONDS", "2.5"))
    all_data = []
    detail_count = 0
    data_dir = os.getenv("DATA_DIR", "/opt/airflow/data")
    os.makedirs(data_dir, exist_ok=True)
    partial_file_path = os.path.join(data_dir, "raw_amazon_eg_products.partial.json")
    detail_cache_path = os.getenv(
        "SCRAPE_DETAIL_CACHE_PATH",
        os.path.join(data_dir, "amazon_eg_detail_cache.json"),
    )
    detail_cache = _load_detail_cache(detail_cache_path)
    logger.info("Loaded %s cached product detail records.", len(detail_cache))
    
    for term in search_terms:
        for page in range(1, search_pages + 1):
            logger.info(f"Scraping '{term}' - Page {page}...")
            html = get_amazon_page(term, page)
            
            if html:
                products = parse_amazon_html(html, term)
                all_data.extend(products)
                _write_json_file(all_data, partial_file_path)
                logger.info(
                    "Extracted %s listing products from %s page %s. Total listing items: %s.",
                    len(products),
                    term,
                    page,
                    len(all_data),
                )
            else:
                logger.info("Skipping to next term due to block.")
                break # Move to next term if blocked
                
            time.sleep(random.uniform(2, 5)) # Sleep to avoid blocks

    if enrich_details and all_data:
        logger.info("Starting detail enrichment for %s listing items.", len(all_data))
        attempted_detail_keys = set()
        limit_logged = False

        for index, product in enumerate(all_data, start=1):
            cache_key = _detail_cache_key(product)
            if cache_key and cache_key in detail_cache:
                _apply_detail_cache(product, detail_cache[cache_key])
                continue

            if cache_key and cache_key in attempted_detail_keys:
                continue

            if detail_limit > 0 and detail_count >= detail_limit:
                if not limit_logged:
                    logger.info(
                        "Detail scrape limit reached at %s unique products. "
                        "Remaining products will use listing-page data only.",
                        detail_count,
                    )
                    limit_logged = True
                break

            detail_enriched = enrich_with_product_details(product)
            detail_count += 1
            if cache_key:
                attempted_detail_keys.add(cache_key)
            if detail_enriched and cache_key:
                detail_cache[cache_key] = _cache_detail_fields(product)

            if detail_count % 25 == 0 or index == len(all_data):
                _write_json_file(all_data, partial_file_path)
                _write_json_file(detail_cache, detail_cache_path)
                logger.info(
                    "Detail progress: %s/%s listing items scanned, "
                    "%s new unique detail pages attempted this run, cache size %s.",
                    index,
                    len(all_data),
                    detail_count,
                    len(detail_cache),
                )

            time.sleep(random.uniform(detail_delay_min, detail_delay_max))

        _write_json_file(all_data, partial_file_path)
        _write_json_file(detail_cache, detail_cache_path)
            
    if not all_data:
        logger.error("Failed to extract any data. Amazon might be heavily blocking IPs today.")
        # We don't raise an error here because Airflow will keep failing and retrying forever.
        # Just return an empty list so it passes cleanly without breaking the warehouse.
    
    file_path = os.path.join(data_dir, "raw_amazon_eg_products.json")
    merge_with_existing = _is_truthy_env("SCRAPE_MERGE_WITH_EXISTING_RAW", "true")
    existing_data = _load_json_list(file_path) if merge_with_existing else []
    if existing_data and all_data:
        before_merge_count = len(all_data)
        all_data = _merge_product_batches(existing_data, all_data)
        logger.info(
            "Merged %s newly scraped rows with %s existing raw rows. Merged raw row count: %s.",
            before_merge_count,
            len(existing_data),
            len(all_data),
        )
    elif existing_data and not all_data:
        logger.warning("Using existing raw data because this scrape did not collect any rows.")
        all_data = existing_data
    
    _write_json_file(all_data, file_path)
    
    logger.info(f"Amazon EG Scraper finished. Total items: {len(all_data)}. Saved to {file_path}")
    return file_path

if __name__ == "__main__":
    scrape_amazon_eg_data()
