"""
Extract bike list and detailed bike information from CarAndBike.

This script:
1. Fetches the list of new bikes from carandbike.com (using Playwright)
2. Downloads individual bike detail pages
3. Extracts comprehensive bike details (using JSON-LD and HTML parsing)
4. Saves results to JSON files in data/new_bike_details/
"""

import argparse
import json
import time
import re
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from playwright.sync_api import sync_playwright

def fetch_bike_links_playwright():
    """Fetch all new bike links using Playwright to handle dynamic loading"""
    print("🌐 Launching browser to fetch bike list from carandbike.com/new-bikes...")
    bikes = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Go to the new bikes page (this often lists popular/all new bikes)
        # Note: The site structure often changes. We'll target the main grid.
        url = "https://www.carandbike.com/new-bikes"
        print(f"   Navigating to {url}...")
        page.goto(url, timeout=60000)
        
        # Scroll to load more items if needed
        # Simplistic approach: scroll down a few times
        for _ in range(10):
            page.mouse.wheel(0, 5000)
            time.sleep(1)
        
        # Get page content
        content = page.content()
        browser.close()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Initial strategy: Look for the grid of bikes
        # Class names might match the car scraper: "grid grid-cols-1 md:grid-cols-3 gap-4"
        # Or standard list items.
        
        # Strategy A: Look for `li` items in specific grids
        # Based on car scraper: <ul class="grid grid-cols-1 md:grid-cols-3 gap-4">
        ul_tags = soup.find_all("ul", class_=lambda x: x and "grid" in x and "gap-4" in x)
        
        for ul in ul_tags:
            li_items = ul.find_all("li", recursive=False)
            for li in li_items:
                bike = extract_list_item(li)
                if bike:
                    bikes.append(bike)
                    
        # Strategy B: If A fails, look for specific link patterns
        if not bikes:
            print("   ⚠️ Strategy A failed (no grid found). Trying Strategy B (link patterns)...")
            # Look for links containing /bike/ and having specific classes
            links = soup.find_all("a", href=lambda x: x and "/bikes/" in x)
            seen_links = set()
            for link in links:
                href = link.get('href')
                if href in seen_links: continue
                
                # Check if it looks like a model link (usually ends with a slug, not query params)
                if href.count('/') > 2: # e.g. /bikes/brand/model
                     title = link.get_text(strip=True)
                     if title:
                         bikes.append({
                             "title": title,
                             "link": href if href.startswith("http") else f"https://www.carandbike.com{href}"
                         })
                         seen_links.add(href)

    print(f"   Found {len(bikes)} bikes.")
    return bikes

def extract_list_item(li_item):
    """Extract bike details from a list item (reused logic from cars)"""
    details = {}
    
    # Title and Link
    title_tag = li_item.find("a", class_=lambda x: x and "js-tracker" in x) or li_item.find("a")
    if title_tag:
        details["title"] = title_tag.get_text(strip=True)
        href = title_tag.get("href", "")
        details["link"] = href if href.startswith("http") else f"https://www.carandbike.com{href}"
    else:
        return None  # Skip if no link

    # Image
    img_tag = li_item.find("img")
    if img_tag:
        details["image_link"] = img_tag.get("src", "")

    # Price
    # Look for INR symbol or price text
    text = li_item.get_text()
    price_match = re.search(r'₹\s*([\d,]+\.?\d*)', text)
    if price_match:
        details["exshowroom_price"] = price_match.group(1)

    return details

def extract_detailed_bike_info(soup):
    """Extract full bike details from the detail page (Adapted from Cars)"""
    bike_details = {}
    
    # 1. JSON-LD (Primary Source)
    json_ld_script = soup.find('script', {'id': 'product-schema-script', 'type': 'application/ld+json'})
    
    if json_ld_script:
        try:
            data = json.loads(json_ld_script.string)
            # Handle @graph
            if '@graph' in data:
                data = data['@graph'][0]
            
            # Map fields
            bike_details['basic_info'] = {
                'name': data.get('name'),
                'manufacturer': data.get('manufacturer'),
                'model': data.get('model'),
                'body_type': data.get('bodyType'), # Often "Motorcycle"
                'description': data.get('description'),
                'sku': data.get('sku')
            }
            
            # Specs
            if 'vehicleEngine' in data:
                engine_data = data['vehicleEngine']
                # Sometimes it's a list
                if isinstance(engine_data, list): 
                    engine_data = engine_data[0] if engine_data else {}
                
                bike_details['engine'] = {
                    'displacement': engine_data.get('engineDisplacement'),
                    'power': engine_data.get('enginePower'),
                    'torque': engine_data.get('torque'),
                    'fuel_type': engine_data.get('fuelType')
                }
            
            # Price
            if 'offers' in data:
                offer = data['offers']
                bike_details['price'] = {
                    'value': offer.get('price'),
                    'currency': offer.get('priceCurrency'),
                    'availability': offer.get('availability')
                }

            # Brand
            if 'brand' in data:
                bike_details['brand'] = {'name': data['brand'].get('name')}

        except Exception as e:
            print(f"    ⚠ JSON-LD parse error: {e}")

    # 2. Key Specs (Fallback/Additional) from the main specs table
    # Often found in a div with "Key Specs"
    specs = {}
    spec_tables = soup.find_all("div", class_=lambda x: x and "contentCard" in x)
    for card in spec_tables:
        rows = card.find_all("div", class_="flex justify-between") # Common row pattern
        for row in rows:
            cols = row.find_all("div")
            if len(cols) == 2:
                key = cols[0].get_text(strip=True)
                val = cols[1].get_text(strip=True)
                specs[key] = val
    
    if specs:
        bike_details['scraped_specs'] = specs
        
        # Fill missing core data if JSON-LD failed
        if 'mileage' not in bike_details:
             # Try to find mileage in scraped specs
             for k, v in specs.items():
                 if 'mileage' in k.lower():
                     bike_details.setdefault('fuel', {})['efficiency'] = v

    return bike_details

def download_page(url, output_path):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(resp.text)
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Extract New Bike Data")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of bikes")
    args = parser.parse_args()

    # Setup directories
    output_dir = Path("data/new_bike_details")
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(".temp/bikes_html")
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Fetch List
    bikes_list = fetch_bike_links_playwright()
    if not bikes_list:
        print("❌ No bikes found. Exiting.")
        return

    if args.limit:
        bikes_list = bikes_list[:args.limit]
        print(f"⚠️ Limiting to {args.limit} bikes.")

    # Save list
    with open("data/new_bikes_list.json", "w") as f:
        json.dump(bikes_list, f, indent=2)

    # 2. Process Each Bike
    print(f"\n🚀 Processing {len(bikes_list)} bikes...")
    
    success_count = 0
    for bike in tqdm(bikes_list):
        title = bike['title']
        link = bike['link']
        safe_filename = title.replace(' ', '_').replace('/', '_') + ".json"
        json_path = output_dir / safe_filename
        
        if json_path.exists():
            continue # Skip existing
            
        # Download
        html_filename = temp_dir / (title.replace(' ', '_') + ".html")
        if not html_filename.exists():
            if not download_page(link, html_filename):
                continue
            time.sleep(1) # Polite delay
            
        # Extract
        with open(html_filename, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
            
        details = extract_detailed_bike_info(soup)
        
        # Merge basic list info (like price if missing)
        if 'basic_info' not in details: details['basic_info'] = {}
        details['basic_info']['name'] = title
        if 'exshowroom_price' in bike and 'price' not in details:
            details['price'] = {'value': bike['exshowroom_price'], 'currency': 'INR'}
            
        # Save
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(details, f, indent=2, ensure_ascii=False)
        
        success_count += 1

    print(f"\n✅ Done. {success_count} new bikes saved to {output_dir}")

if __name__ == "__main__":
    main()
