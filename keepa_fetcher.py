"""
keepa_fetcher.py  —  Token-aware version for Keepa Pro (1 token/minute)
────────────────────────────────────────────────────────────────────────
Strategy:
  - Use the /bestsellers endpoint (costs 1 token, returns up to 100 ASINs)
  - Then use /product to get product data for the #1 ASIN per category
    (costs 1 token per call)
  - Total tokens needed: 22 categories × 2 calls = ~44 tokens
  - We sleep 65s between every API call so tokens always refill
  - If tokens run low, script waits and retries automatically

API key is read from KEEPA_API_KEY environment variable — never hard-coded.
"""

import json
import math
import os
import sys
import time
from datetime import datetime, timezone

import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
KEEPA_API_KEY = os.environ.get("KEEPA_API_KEY", "")
SITE          = os.environ.get("SITE", "uk").lower()
AMAZON_TAG    = os.environ.get("AMAZON_AFFILIATE_TAG", "")

if not KEEPA_API_KEY:
    print("ERROR: KEEPA_API_KEY environment variable is not set.")
    sys.exit(1)

KEEPA_DOMAIN = 2 if SITE == "uk" else 1
AMAZON_BASE  = "https://www.amazon.co.uk" if SITE == "uk" else "https://www.amazon.com"
CURRENCY     = "£" if SITE == "uk" else "$"
OUTPUT_FILE  = f"deals_{SITE}.json"

# ── TOKEN MANAGEMENT ──────────────────────────────────────────────────────────
# 1 token/minute plan. Sleep 65s between every API call to guarantee a fresh token.
SLEEP_BETWEEN_CALLS = 65
TOKEN_RETRY_WAIT    = 90   # if rate-limited, wait longer and retry

# ── CATEGORIES ────────────────────────────────────────────────────────────────
CATEGORIES = {
    "uk": {
        "Electronics":            672123031,
        "Kitchen & Home":         11052681,
        "Books":                  349777031,
        "Toys & Games":           468292,
        "Sports & Outdoors":      318949011,
        "Beauty":                 11057701,
        "Health & Personal Care": 66280031,
        "Garden & Outdoors":      11052711,
        "Clothing":               1731727031,
        "Pet Supplies":           340832031,
        "Baby":                   11052761,
        "Tools & DIY":            11052691,
        "Automotive":             11052741,
        "Grocery":                11052751,
        "Music":                  11052771,
        "Video Games":            637180031,
        "Luggage":                11052661,
        "Jewellery":              11052721,
        "Shoes":                  11052731,
        "Movies & TV":            11052791,
        "Musical Instruments":    11052801,
        "PC & Accessories":       340831031,
    },
    "us": {
        "Electronics":             493964,
        "Kitchen & Dining":        284507,
        "Books":                   283155,
        "Toys & Games":            165793011,
        "Sports & Outdoors":       3375251,
        "Beauty":                  11055981,
        "Health & Household":      3760901,
        "Patio & Garden":          2972638011,
        "Clothing":                1036592,
        "Pet Supplies":            2619533011,
        "Baby":                    165796011,
        "Tools & Home Improvement":228013,
        "Automotive":              15684181,
        "Office Products":         1064954,
        "Grocery":                 16310101,
        "Music":                   5174,
        "Video Games":             468642,
        "Luggage & Travel":        9479199011,
        "Jewellery":               3367581,
        "Shoes":                   672123031,
        "Movies & TV":             2625373011,
        "Musical Instruments":     11091801,
    },
}

# ── KEEPA API WRAPPER ─────────────────────────────────────────────────────────
def keepa_get(endpoint: str, params: dict) -> dict:
    """Call Keepa API. Waits and retries if tokens are exhausted."""
    params = dict(params)
    params["key"] = KEEPA_API_KEY
    url = f"https://api.keepa.com/{endpoint}"

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 429:
                wait = TOKEN_RETRY_WAIT * (attempt + 1)
                print(f"    ⏳ Rate limited — waiting {wait}s before retry...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            tokens_left = data.get("tokensLeft", "?")
            print(f"    [tokens left: {tokens_left}]")
            return data

        except requests.exceptions.Timeout:
            print(f"    ⚠ Timeout on attempt {attempt+1}, retrying...")
            time.sleep(30)
        except requests.exceptions.HTTPError as e:
            print(f"    ⚠ HTTP error: {e.response.status_code}")
            return {}
        except Exception as e:
            print(f"    ⚠ Unexpected error: {e}")
            return {}

    print("    ✗ All retries failed")
    return {}


# ── API CALLS ─────────────────────────────────────────────────────────────────
def fetch_bestsellers(category_id: int) -> list:
    """Get top ASIN list for a category. Costs 1 token."""
    data = keepa_get("bestsellers", {
        "domain":   KEEPA_DOMAIN,
        "category": category_id,
    })
    return data.get("bestSellersList", {}).get("asinList", [])[:5]


def fetch_product(asin: str) -> dict | None:
    """Get product details for one ASIN. Costs 1 token."""
    data = keepa_get("product", {
        "domain": KEEPA_DOMAIN,
        "asin":   asin,
        "stats":  1,
        "rating": 1,
    })
    products = data.get("products", [])
    return products[0] if products else None


# ── PRICE & DATA HELPERS ──────────────────────────────────────────────────────
def to_price(raw) -> float | None:
    if raw and isinstance(raw, (int, float)) and raw > 0:
        return round(raw / 100, 2)
    return None

def get_current_price(product: dict) -> float | None:
    current = (product.get("stats") or {}).get("current", [])
    for idx in [11, 0, 7, 1]:
        try:
            p = to_price(current[idx])
            if p:
                return p
        except (IndexError, TypeError):
            pass
    return None

def get_was_price(product: dict) -> float | None:
    try:
        avg90 = (product.get("stats") or {}).get("avg90", [])
        return to_price(avg90[1])
    except (IndexError, TypeError):
        return None

def get_image(product: dict) -> str:
    try:
        img = product.get("imagesCSV", "").split(",")[0].strip()
        if img:
            return f"https://images-na.ssl-images-amazon.com/images/I/{img}"
    except Exception:
        pass
    return ""

def affiliate_url(asin: str) -> str:
    url = f"{AMAZON_BASE}/dp/{asin}"
    if AMAZON_TAG:
        url += f"?tag={AMAZON_TAG}"
    return url

def build_deal(product: dict, category_name: str) -> dict:
    asin  = product.get("asin", "")
    title = (product.get("title") or "").strip()
    if len(title) > 85:
        title = title[:82] + "…"

    now = get_current_price(product)
    was = get_was_price(product)
    if was and now and was <= now:
        was = None

    return {
        "asin":      asin,
        "category":  category_name,
        "title":     title,
        "price":     f"{CURRENCY}{now:.2f}" if now else "Check Price",
        "was_price": f"{CURRENCY}{was:.2f}" if was else None,
        "rating":    round((product.get("rating") or 0) / 10, 1),
        "reviews":   product.get("reviewCount") or 0,
        "image":     get_image(product),
        "url":       affiliate_url(asin),
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    cats = CATEGORIES.get(SITE, {})
    total_calls = len(cats) * 2  # 2 API calls per category
    est_minutes = (total_calls * SLEEP_BETWEEN_CALLS) // 60

    print(f"\n🚀 Keepa Fetcher — {SITE.upper()} — {len(cats)} categories")
    print(f"   ~2 API calls per category, {SLEEP_BETWEEN_CALLS}s sleep between each")
    print(f"   Estimated runtime: ~{est_minutes} minutes\n")

    deals = []

    for i, (cat_name, cat_id) in enumerate(cats.items()):
        print(f"  [{i+1}/{len(cats)}] {cat_name}")

        # ── Call 1: Get bestseller ASINs ──────────────────────────────────────
        asins = fetch_bestsellers(cat_id)
        if not asins:
            print("    → no ASINs returned, skipping\n")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue
        print(f"    Top ASIN: {asins[0]}")

        # Sleep to let token refill
        print(f"    Sleeping {SLEEP_BETWEEN_CALLS}s…")
        time.sleep(SLEEP_BETWEEN_CALLS)

        # ── Call 2: Get product details for #1 ASIN ───────────────────────────
        product = fetch_product(asins[0])
        if not product:
            print("    → no product data returned, skipping\n")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        deal = build_deal(product, cat_name)
        deals.append(deal)
        print(f"    ✅ {deal['title'][:60]}…")
        print(f"       Price: {deal['price']} | Rating: {deal['rating']} | Reviews: {deal['reviews']}\n")

        # Sleep before next category (skip after last one)
        if i < len(cats) - 1:
            print(f"    Sleeping {SLEEP_BETWEEN_CALLS}s before next category…")
            time.sleep(SLEEP_BETWEEN_CALLS)

    # ── Write JSON output ─────────────────────────────────────────────────────
    now_utc = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")
    output = {
        "site":    SITE,
        "updated": now_utc,
        "count":   len(deals),
        "deals":   deals,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Complete — {len(deals)} deals written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
