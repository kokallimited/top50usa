"""
keepa_fetcher.py
────────────────
Fetches the top-rated best-seller from each category via the Keepa API
and writes deals_uk.json or deals_us.json.

API key is read from the KEEPA_API_KEY environment variable — NEVER
hard-code your key in this file.

Run manually:  KEEPA_API_KEY=xxx SITE=uk python keepa_fetcher.py
Run via CI:    Set secrets in GitHub → Actions picks them up automatically
"""

import json
import math
import os
import sys
from datetime import datetime, timezone

import requests

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

# These come from GitHub Secrets — never put real values here
KEEPA_API_KEY = os.environ.get("KEEPA_API_KEY", "")
SITE          = os.environ.get("SITE", "uk").lower()        # "uk" or "us"
AMAZON_TAG    = os.environ.get("AMAZON_AFFILIATE_TAG", "")  # e.g. yourtag-21

if not KEEPA_API_KEY:
    print("ERROR: KEEPA_API_KEY environment variable is not set.")
    sys.exit(1)

# Keepa domain codes:  1 = US, 2 = UK
KEEPA_DOMAIN = 2 if SITE == "uk" else 1

AMAZON_BASE = {
    "uk": "https://www.amazon.co.uk",
    "us": "https://www.amazon.com",
}[SITE]

CURRENCY = "£" if SITE == "uk" else "$"

OUTPUT_FILE = f"deals_{SITE}.json"

# ── CATEGORY → KEEPA NODE ID ──────────────────────────────────────────────────
# Each number is a Keepa category tree node ID for that marketplace.
# Full list: https://keepa.com/#!categorytree

CATEGORIES = {
    "uk": {
        "Electronics":           672123031,
        "Kitchen & Home":        11052681,
        "Books":                 349777031,
        "Toys & Games":          468292,
        "Sports & Outdoors":     318949011,
        "Beauty":                11057701,
        "Health & Personal Care":66280031,
        "Garden & Outdoors":     11052711,
        "Clothing":              1731727031,
        "Pet Supplies":          340832031,
        "Baby":                  11052761,
        "Tools & DIY":           11052691,
        "Automotive":            11052741,
        "Office Products":       11052681,
        "Grocery":               11052751,
        "Music":                 11052771,
        "Video Games":           637180031,
        "Luggage & Bags":        11052661,
        "Jewellery":             11052721,
        "Shoes":                 11052731,
        "Software":              11052781,
        "Movies & TV":           11052791,
        "Musical Instruments":   11052801,
        "PC & Accessories":      340831031,
        "Handmade":              9821944031,
    },
    "us": {
        "Electronics":           493964,
        "Kitchen & Dining":      284507,
        "Books":                 283155,
        "Toys & Games":          165793011,
        "Sports & Outdoors":     3375251,
        "Beauty":                11055981,
        "Health & Household":    3760901,
        "Patio, Lawn & Garden":  2972638011,
        "Clothing":              1036592,
        "Pet Supplies":          2619533011,
        "Baby":                  165796011,
        "Tools & Home":          228013,
        "Automotive":            15684181,
        "Office Products":       1064954,
        "Grocery":               16310101,
        "Music":                 5174,
        "Video Games":           468642,
        "Luggage & Travel":      9479199011,
        "Jewellery":             3367581,
        "Shoes":                 672123031,
        "Software":              409488,
        "Movies & TV":           2625373011,
        "Musical Instruments":   11091801,
        "Industrial & Scientific":552280011,
        "Arts, Crafts & Sewing": 2617941011,
    },
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def keepa_get(endpoint: str, params: dict) -> dict:
    """Make a GET request to the Keepa API with error handling."""
    params["key"] = KEEPA_API_KEY
    url = f"https://api.keepa.com/{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        print(f"  ⚠ Keepa timeout for {endpoint}")
        return {}
    except requests.exceptions.HTTPError as e:
        print(f"  ⚠ Keepa HTTP error {e.response.status_code} for {endpoint}")
        return {}
    except Exception as e:
        print(f"  ⚠ Keepa error: {e}")
        return {}


def fetch_bestseller_asins(category_id: int, limit: int = 8) -> list[str]:
    """Return top ASIN list for a category from the Keepa bestsellers endpoint."""
    data = keepa_get("bestsellers", {
        "domain":   KEEPA_DOMAIN,
        "category": category_id,
    })
    asins = data.get("bestSellersList", {}).get("asinList", [])
    return asins[:limit]


def fetch_product_details(asins: list[str]) -> list[dict]:
    """Return Keepa product objects for a list of ASINs."""
    if not asins:
        return []
    data = keepa_get("product", {
        "domain": KEEPA_DOMAIN,
        "asin":   ",".join(asins),
        "stats":  1,
        "rating": 1,
    })
    return data.get("products", [])


def keepa_price(raw) -> float | None:
    """Convert Keepa integer price (×100) to float, return None if invalid."""
    if raw and isinstance(raw, (int, float)) and raw > 0:
        return round(raw / 100, 2)
    return None


def score_product(product: dict) -> float:
    """
    Score a product for 'best pick' quality.
    Higher rating × log(reviews) = better score.
    """
    rating  = (product.get("rating") or 0) / 10   # Keepa stores ×10
    reviews = product.get("reviewCount") or 0
    return rating * math.log(max(reviews, 1) + 1)


def best_price(product: dict) -> float | None:
    """Try to find a current buyable price from Keepa stats.current array."""
    current = (product.get("stats") or {}).get("current", [])
    # Keepa current[] indices: 0=Amazon, 1=New, 7=FBA, 11=Buy Box
    for idx in [11, 0, 7, 1]:
        try:
            p = keepa_price(current[idx])
            if p:
                return p
        except (IndexError, TypeError):
            pass
    return None


def was_price(product: dict) -> float | None:
    """90-day average of the New price as the 'was' reference."""
    try:
        avg90 = (product.get("stats") or {}).get("avg90", [])
        return keepa_price(avg90[1])
    except (IndexError, TypeError):
        return None


def image_url(product: dict) -> str:
    """Build Amazon CDN image URL from the first image in Keepa's imagesCSV."""
    try:
        first = product.get("imagesCSV", "").split(",")[0].strip()
        if first:
            return f"https://images-na.ssl-images-amazon.com/images/I/{first}"
    except Exception:
        pass
    return ""


def affiliate_url(asin: str) -> str:
    url = f"{AMAZON_BASE}/dp/{asin}"
    if AMAZON_TAG:
        url += f"?tag={AMAZON_TAG}"
    return url


def build_deal(product: dict, category_name: str) -> dict:
    """Convert a raw Keepa product into our clean deal dict."""
    asin    = product.get("asin", "")
    title   = (product.get("title") or "").strip()
    if len(title) > 85:
        title = title[:82] + "…"

    now  = best_price(product)
    was  = was_price(product)
    # Only show 'was' if it's higher than current price
    if was and now and was <= now:
        was = None

    rating  = round((product.get("rating") or 0) / 10, 1)
    reviews = product.get("reviewCount") or 0

    return {
        "asin":     asin,
        "category": category_name,
        "title":    title,
        "price":    f"{CURRENCY}{now:.2f}" if now else "Check Price",
        "was_price":f"{CURRENCY}{was:.2f}" if was else None,
        "rating":   rating,
        "reviews":  reviews,
        "image":    image_url(product),
        "url":      affiliate_url(asin),
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    cats = CATEGORIES.get(SITE, {})
    print(f"🚀 Top50 Keepa Fetcher — site={SITE.upper()}, {len(cats)} categories")
    print(f"   Output → {OUTPUT_FILE}\n")

    deals = []

    for cat_name, cat_id in cats.items():
        print(f"  [{cat_name}]")
        asins = fetch_bestseller_asins(cat_id, limit=8)

        if not asins:
            print("    → no ASINs, skipping\n")
            continue

        print(f"    ASINs fetched: {len(asins)}")
        products = fetch_product_details(asins[:6])

        if not products:
            print("    → no product data, skipping\n")
            continue

        # Pick the single best product by score
        products.sort(key=score_product, reverse=True)
        best = products[0]
        deal = build_deal(best, cat_name)
        deals.append(deal)
        print(f"    ✅ {deal['title'][:55]}… @ {deal['price']}\n")

    now_utc = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")
    output  = {
        "site":    SITE,
        "updated": now_utc,
        "count":   len(deals),
        "deals":   deals,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Done — {len(deals)} deals written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
