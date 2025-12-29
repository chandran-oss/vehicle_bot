"""
Extract bike list and detailed bike information from CarAndBike HTML files.

This script is adapted from the car extractor and works for bikes as well.
"""

import argparse
import json
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def extract_bike_list_item(li_item):
    """Extract bike details from a single li item"""
    bike_details = {}
    
    # Extract title and link
    title_tag = li_item.find("a", class_="js-tracker")
    if title_tag:
        bike_details["title"] = title_tag.get_text(strip=True)
        bike_details["link"] = title_tag.get("href", "")
    
    # Extract image link
    img_tag = li_item.find("img")
    if img_tag:
        bike_details["image_link"] = img_tag.get("src", "")
    
    # Extract variants
    variants_span = li_item.find("span", class_="text-blue-800 underline cursor-pointer")
    if variants_span:
        variants_text = variants_span.get_text(strip=True)
        variants_match = re.search(r'\+?(\d+)', variants_text)
        if variants_match:
            bike_details["variants"] = int(variants_match.group(1))
    
    # Extract Ex-Showroom price
    price_divs = li_item.find_all("div", class_="text-xs text-[#454545] font-semibold")
    for div in price_divs:
        if "Ex-Showroom" in div.get_text():
            price_div = div.find_next_sibling("div")
            if price_div:
                bike_details["exshowroom_price"] = price_div.get_text(strip=True)
            break
    
    return bike_details


def extract_bike_list(html_path):
    """Extract all bike listings from HTML file"""
    print(f"Reading HTML file: {html_path}")
    
    with open(html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")
    
    ul_tags = soup.find_all("ul", class_="grid grid-cols-1 md:grid-cols-3 gap-4")
    
    all_bikes = []
    for ul_tag in ul_tags:
        li_items = [child for child in ul_tag.children if child.name == 'li']
        for li in li_items:
            bike_details = extract_bike_list_item(li)
            if bike_details:
                all_bikes.append(bike_details)
    
    return all_bikes


def extract_detailed_bike_info(soup):
    """Extract comprehensive bike details from individual bike page HTML"""
    bike_details = {}
    
    # Extract JSON-LD structured data
    json_ld_script = soup.find('script', {'id': 'product-schema-script', 'type': 'application/ld+json'})
    
    if json_ld_script:
        try:
            structured_data = json.loads(json_ld_script.string)
            car_data = structured_data['@graph'][0] if '@graph' in structured_data else structured_data
            
            bike_details['basic_info'] = {
                'name': car_data.get('name'),
                'manufacturer': car_data.get('manufacturer'),
                'model': car_data.get('model'),
                'body_type': car_data.get('bodyType'),
                'url': car_data.get('url'),
                'image_url': car_data.get('image'),
                'description': car_data.get('description', [''])[0] if isinstance(car_data.get('description'), list) else car_data.get('description'),
            }
            
            engine_specs = {}
            if 'vehicleEngine' in car_data:
                for spec in car_data['vehicleEngine']:
                    if 'engineDisplacement' in spec: engine_specs['displacement'] = spec['engineDisplacement']
                    elif 'enginePower' in spec: engine_specs['power'] = spec['enginePower']
                    elif 'torque' in spec: engine_specs['torque'] = spec['torque']
                    elif 'fuelType' in spec: engine_specs['fuel_type'] = spec['fuelType']
            bike_details['engine'] = engine_specs
            
            if 'offers' in car_data:
                offer = car_data['offers']
                bike_details['price'] = {
                    'value': offer.get('price'),
                    'currency': offer.get('priceCurrency'),
                }
            
            if 'brand' in car_data:
                bike_details['brand'] = {'name': car_data['brand'].get('name')}
        except Exception as e:
            print(f"    ⚠ Warning: Error parsing JSON-LD: {e}")

    # ========== REVIEW VIDEOS ==========
    review_videos = []
    videos_section = soup.find('div', id='videos')
    if videos_section:
        video_links = videos_section.find_all('a', class_='js-tracker')
        for link in video_links:
            href = link.get('href', '')
            if '/videos/' in href:
                video_title = link.get('title', '') or link.get_text(strip=True)
                full_url = f"https://www.carandbike.com{href}" if not href.startswith('http') else href
                video_info = {'title': video_title, 'url': full_url}
                img_tag = link.find('img')
                if img_tag:
                    img_src = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('srcset', '')
                    video_info['thumbnail_url'] = img_src
                    yt_match = re.search(r'/vi/([^/]+)/', img_src)
                    if yt_match: video_info['youtube_id'] = yt_match.group(1)
                review_videos.append(video_info)
    
    if review_videos:
        bike_details['review_videos'] = review_videos
    
    return bike_details


def download_bike_page(bike_url, output_dir, bike_title):
    """Download individual bike detail page"""
    safe_title = bike_title.replace(' ', '_').replace('/', '_')
    output_path = output_dir / f"{safe_title}.html"
    if output_path.exists(): return output_path
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(bike_url, headers=headers, timeout=30)
        response.raise_for_status()
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        return output_path
    except Exception as e:
        print(f"    ✗ Error downloading: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Extract bike logic")
    parser.add_argument("--input", type=str, required=True)
    parser.add_argument("--output-dir", type=str, default="data/new_bike_details")
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(".temp/carandbike_new_bikes")
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    print("📋 Extracting bike list...")
    bikes_list = extract_bike_list(input_path)
    
    for bike in tqdm(bikes_list, desc="Processing bikes"):
        bike_title = bike.get('title', 'Unknown')
        safe_title = bike_title.replace(' ', '_').replace('/', '_')
        json_path = output_dir / f"{safe_title}.json"
        
        if json_path.exists(): continue
        if 'link' not in bike: continue
        
        html_path = download_bike_page(bike['link'], temp_dir, bike_title)
        if not html_path: continue
        
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
            bike_details = extract_detailed_bike_info(soup)
            bike_details['id'] = safe_title.lower()
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(bike_details, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f" Error extracting {bike_title}: {e}")


if __name__ == "__main__":
    main()
