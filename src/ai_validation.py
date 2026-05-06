import re


DEVICE_TYPE_TERMS = {
    "Laptop": {
        "laptop",
        "notebook",
        "macbook",
        "chromebook",
        "computer",
        "pc",
    },
    "Smartphone": {
        "smartphone",
        "mobile",
        "phone",
        "iphone",
        "android",
        "galaxy",
        "foldable",
        "flip",
        "fold",
    },
    "Tablet": {
        "tablet",
        "ipad",
        "tab",
        "surface",
    },
    "Feature Phone": {
        "feature phone",
        "button phone",
        "basic phone",
    },
    "Headphone": {
        "headphone",
        "headphones",
        "headset",
        "earbuds",
        "earbud",
        "earphones",
        "earphone",
    },
    "Television": {
        "television",
        "tv",
        "smart tv",
        "led tv",
        "oled",
        "qled",
    },
}


BRAND_ALIASES = {
    "Acer": {"acer"},
    "Anker": {"anker", "soundcore"},
    "Apple": {"apple", "iphone", "macbook", "airpods", "ipad"},
    "ASUS": {"asus", "rog", "vivobook", "zenbook", "tuf"},
    "Benco": {"benco"},
    "Dell": {"dell", "latitude", "inspiron", "vostro", "alienware"},
    "Fresh": {"fresh"},
    "Gionee": {"gionee"},
    "Google": {"google", "pixel"},
    "HP": {"hp", "hewlett packard", "omen", "pavilion", "victus"},
    "Huawei": {"huawei"},
    "Honor": {"honor"},
    "Infinix": {"infinix"},
    "Itel": {"itel"},
    "JBL": {"jbl"},
    "Lava": {"lava"},
    "Lenovo": {"lenovo", "thinkpad", "ideapad", "loq", "legion"},
    "LG": {"lg"},
    "Microsoft": {"microsoft", "surface"},
    "Motorola": {"motorola", "moto"},
    "MSI": {"msi"},
    "Nokia": {"nokia"},
    "OnePlus": {"oneplus", "one plus"},
    "Oppo": {"oppo"},
    "Realme": {"realme"},
    "Redmi": {"redmi"},
    "Samsung": {"samsung", "galaxy"},
    "Sony": {"sony"},
    "TCL": {"tcl"},
    "Tecno": {"tecno"},
    "Tornado": {"tornado"},
    "Toshiba": {"toshiba"},
    "UGREEN": {"ugreen"},
    "Vivo": {"vivo"},
    "Xiaomi": {"xiaomi", "mi", "poco"},
}


GENERIC_BRAND_VALUES = {
    "",
    "unknown",
    "generic",
    "not available",
    "n/a",
    "na",
    "none",
}


def normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _searchable(value):
    return f" {normalize_text(value).lower()} "


def _contains_alias(text, alias):
    alias = alias.lower()
    pattern = r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])"
    return re.search(pattern, text) is not None


def infer_device_type(title, fallback_category=None):
    text = _searchable(f"{title} {fallback_category or ''}")
    for device_type, terms in DEVICE_TYPE_TERMS.items():
        if any(_contains_alias(text, term) for term in terms):
            return device_type

    fallback_category = normalize_text(fallback_category).lower()
    category_map = {
        "laptop": "Laptop",
        "smartphone": "Smartphone",
        "mobile phone": "Smartphone",
        "android phone": "Smartphone",
        "iphone": "Smartphone",
        "tablet": "Tablet",
        "ipad": "Tablet",
        "android tablet": "Tablet",
        "headphone": "Headphone",
        "television": "Television",
    }
    return category_map.get(fallback_category)


def looks_like_device_type(value):
    text = _searchable(value)
    if not text.strip():
        return False
    return any(
        _contains_alias(text, term)
        for terms in DEVICE_TYPE_TERMS.values()
        for term in terms
    )


def infer_brand(title, extracted_brand=None, manufacturer=None):
    candidates = [extracted_brand, manufacturer, title]
    for candidate in candidates:
        text = _searchable(candidate)
        if not text.strip():
            continue
        for brand, aliases in BRAND_ALIASES.items():
            if any(_contains_alias(text, alias) for alias in aliases):
                return brand
    return None


def is_known_brand(value):
    text = _searchable(value)
    if not text.strip():
        return False
    return any(
        _contains_alias(text, alias)
        for aliases in BRAND_ALIASES.values()
        for alias in aliases
    )


def validate_product_record(record):
    """
    Domain-aware validation for retail product columns.

    This is intentionally local and deterministic so Airflow can run without an
    external AI API key. It behaves like a lightweight semantic classifier:
    infer device type, infer brand, and correct brand/device-type mismatches.
    """
    title = normalize_text(record.get("title"))
    category = normalize_text(record.get("category"))
    scraped_brand = normalize_text(record.get("brand"))
    manufacturer = normalize_text(record.get("manufacturer"))

    inferred_device_type = infer_device_type(title, category)
    device_type = normalize_text(record.get("Device type")) or inferred_device_type
    inferred_brand = infer_brand(title, scraped_brand, manufacturer)
    brand = scraped_brand or inferred_brand

    if not device_type:
        device_type = inferred_device_type
    elif inferred_device_type and device_type != inferred_device_type:
        device_type = inferred_device_type

    normalized_brand = normalize_text(brand).lower()
    if normalized_brand in GENERIC_BRAND_VALUES:
        brand = inferred_brand

    if not brand:
        brand = inferred_brand
    elif looks_like_device_type(brand) and not is_known_brand(brand):
        brand = inferred_brand
    elif scraped_brand and inferred_brand and scraped_brand != inferred_brand:
        brand = inferred_brand

    if brand and device_type and brand.lower() == device_type.lower():
        brand = inferred_brand

    return {
        "brand": brand,
        "Device type": device_type,
    }
