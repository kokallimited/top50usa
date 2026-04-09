"""
keepa_fetcher.py  —  Smart token-aware version
───────────────────────────────────────────────
Key insight: Keepa banks up to 60 tokens (1/min plan).
When you run the script, you start with ~60 tokens available.
Each API call costs 1 token. So we can make ~60 calls before
we need to wait.

Strategy:
  - Check tokensLeft after every call
  - If tokens > 5: proceed immediately (no sleep needed)
  - If tokens <= 5: sleep 70s to let tokens refill before continuing
  - This means the first ~60 calls happen in seconds
  - Only slows down if we exhaust the token bank

With 22 categories × 2 calls = 44 total calls:
  - All 44 calls fit within the 60 token bank
  - Total runtime: ~2-3 minutes (instead of 50 minutes!)
  - Well within GitHub's 2 hour limit
"""

import json
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
    print("ERROR: KEEPA_API_KEY is not set.")
    sys.exit(1)

KEEPA_DOMAIN = 2 if SITE == "uk" else 1
AMAZON_BASE  = "https://www.amazon.co.uk" if SITE == "uk" else "https://www.amazon.com"
CURRENCY     = "£" if SITE == "uk" else "$"
OUTPUT_FILE  = f"deals_{SITE}.json"

# Token safety threshold — wait if we drop to this many tokens
TOKEN_SAFETY  = 5
TOKEN_WAIT_S  = 75   # seconds to sleep when tokens run low

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

# ── KEEPA API CALL ────────────────────────────────────────────────────────────
tokens_left = 60  # assume we start with a full bank

def keepa_get(endpoint: str, params: dict) -> dict:
    """
    Make a Keepa API call.
    - Checks token level before calling
    - Waits if tokens are low
    - Updates global token count from response
    """
    global tokens_left

    # If we're running low, wait for tokens to refill
    if tokens_left <= TOKEN_SAFETY:
        print(f"    ⏳ Tokens low ({tokens_left}) — waiting {TOKEN_WAIT_S}s to refill...")
        time.sleep(TOKEN_WAIT_S)

    params = dict(params)
    params["key"] = KEEPA_API_KEY
    url = f"https://api.keepa.com/{endpoint}"

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)

            # Rate limited — wait and retry
            if resp.status_code == 429:
                wait = TOKEN_WAIT_S * (attempt + 1)
                print(f"    ⏳ Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # Update our token counter from the actual response
            tokens_left = data.get("tokensLeft", tokens_left - 1)
            print(f"    [tokens left: {tokens_left}]")
            return data

        except requests.exceptions.Timeout:
            print(f"    ⚠ Timeout (attempt {attempt+1})")
            time.sleep(20)
        except requests.exceptions.HTTPError as e:
            print(f"    ⚠ HTTP {e.response.status_code}")
            return {}
        except Exception as e:
            print(f"    ⚠ Error: {e}")
            return {}

    return {}


# ── KEEPA CALLS ───────────────────────────────────────────────────────────────
def fetch_bestsellers(category_id: int) -> list:
    """Get top ASINs for a category. Costs 1 token."""
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

def get_current_price(p: dict) -> float | None:
    current = (p.get("stats") or {}).get("current", [])
    for idx in [11, 0, 7, 1]:
        try:
            v = to_price(current[idx])
            if v: return v
        except (IndexError, TypeError):
            pass
    return None

def get_was_price(p: dict) -> float | None:
    try:
        avg90 = (p.get("stats") or {}).get("avg90", [])
        return to_price(avg90[1])
    except (IndexError, TypeError):
        return None

def get_image(p: dict) -> str:
    try:
        img = p.get("imagesCSV", "").split(",")[0].strip()
        if img:
            return f"https://images-na.ssl-images-amazon.com/images/I/{img}"
    except Exception:
        pass
    return ""

def make_url(asin: str) -> str:
    url = f"{AMAZON_BASE}/dp/{asin}"
    if AMAZON_TAG:
        url += f"?tag={AMAZON_TAG}"
    return url

def build_deal(p: dict, cat: str) -> dict:
    title = (p.get("title") or "").strip()
    if len(title) > 85:
        title = title[:82] + "…"
    now = get_current_price(p)
    was = get_was_price(p)
    if was and now and was <= now:
        was = None
    return {
        "asin":      p.get("asin", ""),
        "category":  cat,
        "title":     title,
        "price":     f"{CURRENCY}{now:.2f}" if now else "Check Price",
        "was_price": f"{CURRENCY}{was:.2f}" if was else None,
        "rating":    round((p.get("rating") or 0) / 10, 1),
        "reviews":   p.get("reviewCount") or 0,
        "image":     get_image(p),
        "url":       make_url(p.get("asin", "")),
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    cats = CATEGORIES.get(SITE, {})
    needed = len(cats) * 2
    print(f"\n🚀 Keepa Fetcher — {SITE.upper()}")
    print(f"   {len(cats)} categories × 2 calls = {needed} tokens needed")
    print(f"   Starting token bank: ~{tokens_left} (will wait if we run low)\n")

    deals = []
    start = time.time()

    for i, (cat_name, cat_id) in enumerate(cats.items()):
        print(f"  [{i+1}/{len(cats)}] {cat_name}")

        # Call 1: bestsellers list
        asins = fetch_bestsellers(cat_id)
        if not asins:
            print("    → no ASINs, skipping\n")
            continue

        print(f"    Top ASIN: {asins[0]}")

        # Call 2: product details
        product = fetch_product(asins[0])
        if not product:
            print("    → no product data, skipping\n")
            continue

        deal = build_deal(product, cat_name)
        deals.append(deal)
        print(f"    ✅ {deal['title'][:60]}")
        print(f"       {deal['price']}  |  ⭐ {deal['rating']}  |  {deal['reviews']} reviews\n")

    elapsed = round((time.time() - start) / 60, 1)
    now_utc = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC")

    output = {
        "site":    SITE,
        "updated": now_utc,
        "count":   len(deals),
        "deals":   deals,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"✅ Done in {elapsed} mins — {len(deals)} deals written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
