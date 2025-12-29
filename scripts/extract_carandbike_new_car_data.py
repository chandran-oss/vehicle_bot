"""
Extract car list and detailed car information from CarAndBike HTML files.

This script:
1. Extracts car list from the main HTML file
2. Downloads individual car detail pages (resumable)
3. Extracts comprehensive car details from each page
4. Saves results to CSV and JSON files
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


def extract_car_list_item(li_item):
    """Extract car details from a single li item
    
    Args:
        li_item: BeautifulSoup li element containing car information
        
    Returns:
        dict: Car details including title, link, variants, price, etc.
    """
    car_details = {}
    
    # Extract title and link
    title_tag = li_item.find("a", class_="js-tracker")
    if title_tag:
        car_details["title"] = title_tag.get_text(strip=True)
        car_details["link"] = title_tag.get("href", "")
    
    # Extract image link
    img_tag = li_item.find("img")
    if img_tag:
        car_details["image_link"] = img_tag.get("src", "")
    
    # Extract variants
    variants_span = li_item.find("span", class_="text-blue-800 underline cursor-pointer")
    if variants_span:
        variants_text = variants_span.get_text(strip=True)
        # Extract number from text like "+25 Variants"
        variants_match = re.search(r'\+?(\d+)', variants_text)
        if variants_match:
            car_details["variants"] = int(variants_match.group(1))
    
    # Extract Ex-Showroom price
    price_divs = li_item.find_all("div", class_="text-xs text-[#454545] font-semibold")
    for div in price_divs:
        if "Ex-Showroom" in div.get_text():
            price_div = div.find_next_sibling("div")
            if price_div:
                car_details["exshowroom_price"] = price_div.get_text(strip=True)
            break
    
    # Extract EMI starts at
    for div in price_divs:
        if "EMI starts at" in div.get_text():
            emi_div = div.find_next_sibling("div")
            if emi_div:
                car_details["emi_starts_at"] = emi_div.get_text(strip=True).replace("₹", "").strip()
            break
    
    return car_details


def extract_car_list(html_path):
    """Extract all car listings from HTML file
    
    Args:
        html_path: Path to the HTML file
        
    Returns:
        list: List of car dictionaries
    """
    print(f"Reading HTML file: {html_path}")
    
    with open(html_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")
    
    # Find all <ul> tags having the specific class
    ul_tags = soup.find_all("ul", class_="grid grid-cols-1 md:grid-cols-3 gap-4")
    
    print(f"Found {len(ul_tags)} ul tags with car listings")
    
    # Extract all car details from all ul tags
    all_cars = []
    
    for ul_tag in ul_tags:
        # Get all li items (filter out text nodes)
        li_items = [child for child in ul_tag.children if child.name == 'li']
        
        for li in li_items:
            car_details = extract_car_list_item(li)
            if car_details:  # Only add if we got some data
                all_cars.append(car_details)
    
    print(f"Total cars extracted: {len(all_cars)}\n")
    return all_cars


def extract_detailed_car_info(soup):
    """Extract comprehensive car details from individual car page HTML
    
    Args:
        soup: BeautifulSoup object of car detail page
        
    Returns:
        dict: Comprehensive car details
    """
    car_details = {}
    
    # Extract JSON-LD structured data (primary source)
    json_ld_script = soup.find('script', {'id': 'product-schema-script', 'type': 'application/ld+json'})
    
    if json_ld_script:
        try:
            structured_data = json.loads(json_ld_script.string)
            
            # Navigate to the car product data
            if '@graph' in structured_data:
                car_data = structured_data['@graph'][0]
            else:
                car_data = structured_data
            
            # Basic Information
            car_details['basic_info'] = {
                'name': car_data.get('name'),
                'manufacturer': car_data.get('manufacturer'),
                'model': car_data.get('model'),
                'body_type': car_data.get('bodyType'),
                'url': car_data.get('url'),
                'image_url': car_data.get('image'),
                'description': car_data.get('description', [''])[0] if isinstance(car_data.get('description'), list) else car_data.get('description'),
                'model_date': car_data.get('vehicleModelDate'),
                'sku': car_data.get('sku'),
                'vin': car_data.get('vehicleIdentificationNumber'),
                'condition': car_data.get('itemCondition')
            }
            
            # Engine Specifications
            engine_specs = {}
            if 'vehicleEngine' in car_data:
                for spec in car_data['vehicleEngine']:
                    if 'engineDisplacement' in spec:
                        engine_specs['displacement'] = spec['engineDisplacement']
                    elif 'enginePower' in spec:
                        engine_specs['power'] = spec['enginePower']
                    elif 'torque' in spec:
                        engine_specs['torque'] = spec['torque']
                    elif 'fuelType' in spec:
                        engine_specs['fuel_type'] = spec['fuelType']
            
            car_details['engine'] = engine_specs
            
            # Transmission
            car_details['transmission'] = car_data.get('vehicleTransmission')
            
            # Fuel Information
            car_details['fuel'] = {
                'type': car_data.get('fuelType'),
                'efficiency': car_data.get('fuelEfficiency')
            }
            
            # Dimensions & Capacity
            car_details['dimensions'] = {
                'width': car_data.get('width'),
                'height': car_data.get('height'),
                'weight': car_data.get('weight'),
                'seating_capacity': car_data.get('vehicleSeatingCapacity'),
                'number_of_doors': car_data.get('numberOfDoors')
            }
            
            # Colors
            car_details['colors'] = car_data.get('color', [])
            
            # Price Information
            if 'offers' in car_data:
                offer = car_data['offers']
                car_details['price'] = {
                    'value': offer.get('price'),
                    'currency': offer.get('priceCurrency'),
                    'availability': offer.get('availability'),
                    'valid_until': offer.get('priceValidUntil'),
                    'url': offer.get('url')
                }
            
            # Brand Information
            if 'brand' in car_data:
                car_details['brand'] = {
                    'name': car_data['brand'].get('name'),
                    'image': car_data['brand'].get('image')
                }
            
            # Review/Rating
            if 'review' in car_data:
                review = car_data['review']
                if 'reviewRating' in review:
                    rating = review['reviewRating']
                    car_details['rating'] = {
                        'value': rating.get('ratingValue'),
                        'worst': rating.get('worstRating'),
                        'best': rating.get('bestRating')
                    }
                
                if 'author' in review:
                    author = review['author']
                    car_details['reviewed_by'] = {
                        'name': author.get('name'),
                        'job_title': author.get('jobTitle'),
                        'url': author.get('url')
                    }
                
                # Positive and negative notes
                if 'positiveNotes' in review and 'itemListElement' in review['positiveNotes']:
                    car_details['pros'] = [item.get('name') for item in review['positiveNotes']['itemListElement']]
                
                if 'negativeNotes' in review and 'itemListElement' in review['negativeNotes']:
                    car_details['cons'] = [item.get('name') for item in review['negativeNotes']['itemListElement']]
        
        except Exception as e:
            print(f"    ⚠ Warning: Error parsing JSON-LD data: {e}")
    
    # ========== MILEAGE TABLE ==========
    mileage_data = []
    tables = soup.find_all('table')
    for table in tables:
        thead = table.find('thead')
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all('th')]
            if 'Fuel Type' in headers or 'City Mileage' in headers:
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) >= 5:
                            mileage_entry = {
                                'fuel_type': cols[0].get_text(strip=True),
                                'transmission': cols[1].get_text(strip=True),
                                'mileage': cols[2].get_text(strip=True),
                                'city_mileage': cols[3].get_text(strip=True),
                                'highway_mileage': cols[4].get_text(strip=True)
                            }
                            mileage_data.append(mileage_entry)
                break
    
    if mileage_data:
        car_details['mileage_details'] = mileage_data
    
    # ========== WHAT'S NEW SECTION ==========
    whats_new_section = soup.find('h2', string=lambda x: x and "What's New" in str(x))
    
    if whats_new_section:
        content_div = whats_new_section.find_next('div', class_=lambda x: x and 'contentCard' in str(x))
        if content_div:
            sections = {}
            for h3 in content_div.find_all('h3'):
                heading = h3.get_text(strip=True)
                content = []
                for sibling in h3.find_next_siblings():
                    if sibling.name == 'h3':
                        break
                    if sibling.name == 'ul':
                        content.extend([li.get_text(strip=True) for li in sibling.find_all('li')])
                    elif sibling.name == 'p' and not sibling.find('img'):
                        text = sibling.get_text(strip=True)
                        if text:
                            content.append(text)
                
                if content:
                    sections[heading] = content
            
            if sections:
                car_details['whats_new'] = sections
    
    # ========== EXPERT REVIEW SECTIONS ==========
    expert_review = {}
    review_sections = [
        'Design and Styling',
        'Interior and Features',
        'Safety and ADAS',
        'Ride and Handling',
        'Performance and Engine',
        'Comfort and Space'
    ]
    
    for section_name in review_sections:
        section_button = soup.find('button', string=lambda x: x and section_name in str(x))
        if not section_button:
            section_div = soup.find('div', string=lambda x: x and section_name in str(x))
            if section_div:
                section_button = section_div.find_parent('button')
        
        if section_button:
            content_div = section_button.find_next('div', class_=lambda x: x and ('contentCard' in str(x) or 'contentWrap' in str(x)))
            if content_div:
                paragraphs = content_div.find_all('p')
                content_text = ' '.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                
                if content_text:
                    key = section_name.lower().replace(' and ', '_').replace(' ', '_').rstrip(':')
                    expert_review[key] = content_text
    
    if expert_review:
        car_details['expert_review'] = expert_review
    
    # ========== VERDICT ==========
    verdict_heading = soup.find('h3', string=lambda x: x and 'Verdict' in str(x))
    if verdict_heading:
        verdict_div = verdict_heading.find_next('div')
        if verdict_div:
            verdict_p = verdict_div.find('p')
            if verdict_p:
                car_details['verdict'] = verdict_p.get_text(strip=True)
    
    # ========== COMPARISON WITH COMPETITORS ==========
    comparison_heading = soup.find('h2', string=lambda x: x and ('Quick Compare' in str(x) or 'Competitors' in str(x)))
    
    if comparison_heading:
        comparison_table = comparison_heading.find_next('table')
        if comparison_table:
            thead = comparison_table.find('thead')
            if thead:
                header_row = thead.find('tr')
                car_headers = []
                
                for th in header_row.find_all('th'):
                    car_name_div = th.find('div', class_=lambda x: x and 'font-semibold' in str(x))
                    price_div = th.find('div', class_=lambda x: x and 'font-bold' in str(x))
                    
                    if car_name_div:
                        car_info = {
                            'name': car_name_div.get_text(strip=True),
                            'price': price_div.get_text(strip=True) if price_div else None
                        }
                        
                        link_tag = th.find('a')
                        if link_tag:
                            car_info['url'] = link_tag.get('href', '')
                        
                        car_headers.append(car_info)
                
                tbody = comparison_table.find('tbody')
                if tbody and car_headers:
                    comparison_rows = []
                    for row in tbody.find_all('tr'):
                        cells = row.find_all('td')
                        if cells:
                            first_cell = cells[0]
                            feature_div = first_cell.find('div', class_=lambda x: x and 'font-bold' in str(x))
                            feature_name = feature_div.get_text(strip=True) if feature_div else ''
                            
                            value_div = first_cell.find('div', class_=lambda x: x and 'font-medium' in str(x))
                            primary_value = value_div.get_text(strip=True) if value_div else ''
                            
                            if not feature_name and not primary_value:
                                feature_name = first_cell.get_text(strip=True)
                                primary_value = ''
                            
                            competitor_values = [cell.get_text(strip=True) for cell in cells[1:]]
                            all_values = [primary_value] + competitor_values if primary_value else competitor_values
                            
                            comparison_rows.append({
                                'feature': feature_name,
                                'values': all_values
                            })
                    
                    car_details['competitor_comparison'] = {
                        'cars': car_headers,
                        'features': comparison_rows
                    }
    
    # ========== REVIEW VIDEOS ==========
    review_videos = []
    videos_section = soup.find('div', id='videos')
    if videos_section:
        video_links = videos_section.find_all('a', class_='js-tracker')
        for link in video_links:
            href = link.get('href', '')
            if '/videos/' in href:
                video_title = link.get('title', '') or link.get_text(strip=True)
                
                # Full URL
                full_url = href
                if not href.startswith('http'):
                    full_url = f"https://www.carandbike.com{href}"
                
                video_info = {
                    'title': video_title,
                    'url': full_url
                }
                
                # Try to extract YouTube ID from thumbnail
                img_tag = link.find('img')
                if img_tag:
                    img_src = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('srcset', '')
                    video_info['thumbnail_url'] = img_src
                    
                    # Pattern match for YouTube ID: .../vi/[VIDEO_ID]/...
                    yt_match = re.search(r'/vi/([^/]+)/', img_src)
                    if yt_match:
                        video_info['youtube_id'] = yt_match.group(1)
                
                review_videos.append(video_info)
    
    if review_videos:
        car_details['review_videos'] = review_videos
    
    return car_details


def download_car_page(car_url, output_dir, car_title):
    """Download individual car detail page
    
    Args:
        car_url: URL of the car detail page
        output_dir: Directory to save HTML files
        car_title: Title of the car (for filename)
        
    Returns:
        Path: Path to saved HTML file, or None if download failed
    """
    # Create safe filename from title
    safe_title = car_title.replace(' ', '_').replace('/', '_')
    output_path = output_dir / f"{safe_title}.html"
    
    # Skip if already exists
    if output_path.exists():
        return output_path
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        response = requests.get(car_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        return output_path
    
    except Exception as e:
        print(f"    ✗ Error downloading: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Extract car list and detailed information from CarAndBike HTML files"
    )
    parser.add_argument(
        "--input",
        type=str,
        default="experiment-notebooks/html_files/carandbike.com_new-cars_models_6.html",
        help="Path to input HTML file with car listings"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="Directory to save output files (default: data)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay between downloads in seconds (default: 0)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of cars to process (for testing)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading HTML files and only extract from existing files"
    )
    
    args = parser.parse_args()
    
    # Convert paths to Path objects
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    temp_dir = Path(".temp/carandbike_new_cars")
    details_output_dir = output_dir / "new_car_details"
    
    # Check if input file exists
    if not input_path.exists():
        print(f"❌ Error: Input file not found: {input_path}")
        return
    
    # Create output directories
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    details_output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("🚗 CarAndBike Data Extractor")
    print("=" * 80)
    print()
    
    # STEP 1: Extract car list
    print("📋 STEP 1: Extracting car list...")
    print("-" * 80)
    cars_list = extract_car_list(input_path)
    
    if not cars_list:
        print("❌ Warning: No cars extracted!")
        return
    
    # Apply limit if specified
    if args.limit:
        print(f"⚠️  Limiting to first {args.limit} cars for testing\n")
        cars_list = cars_list[:args.limit]
    
    # Save car list
    df = pd.DataFrame(cars_list)
    cars_list_csv = output_dir / "new_cars_list.csv"
    cars_list_json = output_dir / "new_cars_list.json"
    
    df.to_csv(cars_list_csv, index=False)
    print(f"✅ Saved car list to: {cars_list_csv}")
    
    with open(cars_list_json, 'w', encoding='utf-8') as f:
        json.dump(cars_list, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved car list to: {cars_list_json}")
    
    print(f"\n📊 Summary: {len(df)} cars, {df['variants'].sum() if 'variants' in df.columns else 0} total variants")
    
    # STEP 2: Download and extract car details
    print("\n" + "=" * 80)
    print("🔍 STEP 2: Downloading and extracting car details...")
    print("-" * 80)
    
    # Check which cars already have details extracted
    existing_details = set()
    for json_file in details_output_dir.glob("*.json"):
        existing_details.add(json_file.stem)
    
    if existing_details:
        print(f"📁 Found {len(existing_details)} existing car details (will skip these)")
    
    # Process each car
    successful = 0
    skipped = 0
    failed = 0
    
    print(f"\n🚀 Processing {len(cars_list)} cars...\n")
    
    for car in tqdm(cars_list, desc="Processing cars", unit="car"):
        car_title = car.get('title', 'Unknown')
        safe_title = car_title.replace(' ', '_').replace('/', '_')
        
        # Check if details already exist
        json_path = details_output_dir / f"{safe_title}.json"
        if json_path.exists():
            skipped += 1
            continue
        
        # Check if car has a link
        if 'link' not in car or not car['link']:
            tqdm.write(f"  ⚠️  {car_title}: No link available")
            failed += 1
            continue
        
        # Download HTML if not skipping
        html_path = None
        if not args.skip_download:
            html_path = download_car_page(car['link'], temp_dir, car_title)
            
            if not html_path:
                failed += 1
                continue
            
            # Add delay if specified
            if args.delay > 0:
                time.sleep(args.delay)
        else:
            # Use existing HTML file
            safe_title = car_title.replace(' ', '_').replace('/', '_')
            html_path = temp_dir / f"{safe_title}.html"
            if not html_path.exists():
                tqdm.write(f"  ⚠️  {car_title}: HTML file not found")
                failed += 1
                continue
        
        # Extract car details from HTML
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
            
            car_details = extract_detailed_car_info(soup)
            
            # Save to JSON
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(car_details, f, indent=2, ensure_ascii=False)
            
            successful += 1
        
        except Exception as e:
            tqdm.write(f"  ✗ {car_title}: Error extracting details: {e}")
            failed += 1
    
    # Final summary
    print("\n" + "=" * 80)
    print("✅ Extraction Complete!")
    print("=" * 80)
    print(f"""
📊 Results:
  • Total cars processed: {len(cars_list)}
  • Successfully extracted: {successful}
  • Skipped (already exist): {skipped}
  • Failed: {failed}

📁 Output files:
  • Car list CSV: {cars_list_csv}
  • Car list JSON: {cars_list_json}
  • Car details: {details_output_dir}/ ({successful + skipped} JSON files)
  • Temp HTML files: {temp_dir}/ ({len(list(temp_dir.glob('*.html')))} files)
""")
    
    if successful + skipped > 0:
        print("🎉 Success! Car data has been extracted and saved.")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
