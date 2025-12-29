"""Data preprocessing functions for cleaning and normalizing car data from JSON files."""

import re
from typing import Any


def parse_price(value: Any) -> int | None:
    """
    Convert price value to integer.
    
    Args:
        value: Price as string or int
        
    Returns:
        Price as integer in INR, or None if invalid
        
    Examples:
        >>> parse_price("43797297")
        43797297
        >>> parse_price(43797297)
        43797297
        >>> parse_price(None)
        None
    """
    if value is None:
        return None
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, str):
        # Remove any non-digit characters
        cleaned = re.sub(r'[^\d]', '', value)
        if cleaned:
            return int(cleaned)
    
    return None


def parse_engine_displacement(value: str | None) -> list[dict[str, Any]]:
    """
    Parse engine displacement keeping ALL values with units preserved.
    
    Args:
        value: Displacement string like "3982 cc" or "1197,1497 cc"
        
    Returns:
        List of dicts with value and unit
        
    Examples:
        >>> parse_engine_displacement("3982 cc")
        [{'value': 3982, 'unit': 'cc'}]
        >>> parse_engine_displacement("1197,1497 cc")
        [{'value': 1197, 'unit': 'cc'}, {'value': 1497, 'unit': 'cc'}]
        >>> parse_engine_displacement("N/A")
        []
    """
    if not value or value == "N/A":
        return []
    
    # Extract unit (cc, CC, etc.) - typically at the end
    unit_match = re.search(r'\s*([a-zA-Z]+)\s*$', value)
    unit = unit_match.group(1) if unit_match else "cc"
    
    # Extract all numeric values
    numbers = re.findall(r'\d+', value)
    
    if not numbers:
        return []
    
    return [{"value": int(num), "unit": unit} for num in numbers]


def parse_mileage(value: str | None) -> dict[str, Any]:
    """
    Parse mileage/efficiency values including ranges and EV formats.
    
    Args:
        value: Mileage string like "12 KM/L", "19 - 25 KM/L", "265 Km/Full Charge"
        
    Returns:
        Dict with value/min/max, unit, and type
        
    Examples:
        >>> parse_mileage("12 KM/L")
        {'value': 12.0, 'unit': 'km/l', 'type': 'fuel'}
        >>> parse_mileage("19 - 25 KM/L")
        {'min': 19.0, 'max': 25.0, 'unit': 'km/l', 'type': 'fuel'}
        >>> parse_mileage("265 Km/Full Charge")
        {'value': 265.0, 'unit': 'km', 'type': 'electric'}
    """
    if not value:
        return {}
    
    value_lower = value.lower()
    
    # Check if it's electric vehicle range
    is_electric = "full charge" in value_lower or "km/full" in value_lower
    
    # Extract numbers
    numbers = re.findall(r'\d+\.?\d*', value)
    
    if not numbers:
        return {}
    
    # Convert to floats
    nums = [float(n) for n in numbers]
    
    if is_electric:
        # EV format
        if len(nums) == 1:
            return {"value": nums[0], "unit": "km", "type": "electric"}
        elif len(nums) >= 2:
            return {"min": nums[0], "max": nums[1], "unit": "km", "type": "electric"}
    else:
        # Fuel format - check for range
        if "-" in value and len(nums) >= 2:
            return {"min": nums[0], "max": nums[1], "unit": "km/l", "type": "fuel"}
        elif "," in value and len(nums) >= 2:
            # Handle comma-separated values like "265,365"
            return {"min": nums[0], "max": nums[1], "unit": "km/l", "type": "fuel"}
        else:
            return {"value": nums[0], "unit": "km/l", "type": "fuel"}
    
    return {}


def parse_seating_capacity(value: Any) -> int | None:
    """
    Extract integer from seating capacity value.
    
    Args:
        value: String like "5" or "5 Seater"
        
    Returns:
        Seating capacity as integer
        
    Examples:
        >>> parse_seating_capacity("5")
        5
        >>> parse_seating_capacity("5 Seater")
        5
    """
    if value is None:
        return None
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, str):
        match = re.search(r'\d+', value)
        if match:
            return int(match.group())
    
    return None


def parse_rating(value: Any) -> float | None:
    """
    Convert rating to float.
    
    Args:
        value: Rating as string, int, or float
        
    Returns:
        Rating as float or None
        
    Examples:
        >>> parse_rating("7.5")
        7.5
        >>> parse_rating(7)
        7.0
        >>> parse_rating("-")
        None
    """
    if value is None or value == "-":
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    
    return None


def parse_dimension(value: str | None) -> dict[str, Any] | None:
    """
    Parse dimension value preserving the unit.
    
    Args:
        value: Dimension string like "1700 mm " or "67.2 inches"
        
    Returns:
        Dict with value and unit, or None
        
    Examples:
        >>> parse_dimension("1700 mm ")
        {'value': 1700.0, 'unit': 'mm'}
        >>> parse_dimension("67.2 inches")
        {'value': 67.2, 'unit': 'inches'}
    """
    if not value:
        return None
    
    # Clean up whitespace
    value = value.strip()
    
    # Extract number and unit
    match = re.match(r'([\d.]+)\s*([a-zA-Z]+)?', value)
    
    if match:
        num = float(match.group(1))
        unit = match.group(2) if match.group(2) else "mm"  # Default to mm
        return {"value": num, "unit": unit}
    
    return None


def parse_weight(value: str | None) -> dict[str, int]:
    """
    Parse weight value from formats like "1788/1788".
    
    Args:
        value: Weight string
        
    Returns:
        Dict with kerb_weight and gross_weight
        
    Examples:
        >>> parse_weight("1788/1788")
        {'kerb_weight': 1788, 'gross_weight': 1788}
        >>> parse_weight("1500")
        {'kerb_weight': 1500, 'gross_weight': 1500}
    """
    if not value:
        return {}
    
    # Split by slash if present
    parts = value.split('/')
    numbers = []
    
    for part in parts:
        match = re.search(r'\d+', part)
        if match:
            numbers.append(int(match.group()))
    
    if len(numbers) >= 2:
        return {"kerb_weight": numbers[0], "gross_weight": numbers[1]}
    elif len(numbers) == 1:
        return {"kerb_weight": numbers[0], "gross_weight": numbers[0]}
    
    return {}


def parse_power_torque(value: str | None) -> list[dict]:
    """
    Parse complex power/torque strings with multiple values.
    
    Args:
        value: Power/torque string like "680 bhp" or "109bhp @5000 rpm,110bhp @5000 rpm"
        
    Returns:
        List of dicts with value, unit, and optionally rpm
        
    Examples:
        >>> parse_power_torque("680 bhp")
        [{'value': 680, 'unit': 'bhp'}]
        >>> parse_power_torque("109bhp @5000 rpm,110bhp @5000 rpm")
        [{'value': 109, 'unit': 'bhp', 'rpm': '5000 rpm'}, {'value': 110, 'unit': 'bhp', 'rpm': '5000 rpm'}]
    """
    if not value:
        return []
    
    results = []
    
    # Split by comma for multiple values
    parts = value.split(',')
    
    for part in parts:
        part = part.strip()
        
        # Extract number, unit, and rpm info
        # Pattern: number + unit + optional rpm info
        match = re.search(r'(\d+\.?\d*)\s*([a-zA-Z/]+)(?:\s+@?\s*([\d\-\s]+(?:rpm)?))?', part, re.IGNORECASE)
        
        if match:
            num = float(match.group(1))
            unit = match.group(2).lower()
            rpm = match.group(3).strip() if match.group(3) else None
            
            entry = {"value": num, "unit": unit}
            if rpm:
                entry["rpm"] = rpm
            
            results.append(entry)
    
    return results


def parse_multi_value_field(value: str | None, delimiter: str = ",") -> list[str]:
    """
    Split multi-value fields into list.
    
    Args:
        value: String with multiple values like "Petrol, Diesel"
        delimiter: Delimiter to split on
        
    Returns:
        List of trimmed strings
        
    Examples:
        >>> parse_multi_value_field("Petrol, Diesel")
        ['Petrol', 'Diesel']
        >>> parse_multi_value_field("Manual")
        ['Manual']
    """
    if not value:
        return []
    
    if delimiter in value:
        return [item.strip() for item in value.split(delimiter) if item.strip()]
    
    return [value.strip()] if value.strip() else []


def normalize_fuel_type(fuel_types: list[str]) -> list[str]:
    """
    Normalize fuel type values.
    
    Args:
        fuel_types: List of fuel type strings
        
    Returns:
        Normalized list of fuel types
        
    Examples:
        >>> normalize_fuel_type(["Petrol+CNG"])
        ['Petrol', 'CNG']
        >>> normalize_fuel_type(["Petrol", "Diesel"])
        ['Petrol', 'Diesel']
    """
    normalized = []
    
    for fuel_type in fuel_types:
        # Split by + or / for combined fuel types
        if '+' in fuel_type or '/' in fuel_type:
            parts = re.split(r'[+/]', fuel_type)
            normalized.extend([p.strip() for p in parts if p.strip()])
        else:
            normalized.append(fuel_type.strip())
    
    # Remove duplicates while preserving order
    seen = set()
    result = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result


def parse_number_of_doors(value: Any) -> int | None:
    """
    Extract number of doors.
    
    Args:
        value: String or int representing number of doors
        
    Returns:
        Number of doors as integer
        
    Examples:
        >>> parse_number_of_doors("5")
        5
        >>> parse_number_of_doors(3)
        3
    """
    if value is None:
        return None
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, str):
        match = re.search(r'\d+', value)
        if match:
            return int(match.group())
    
    return None


def generate_image_url_id(car_id: str, suffix: str) -> str:
    """
    Generate unique image URL identifier.
    
    Args:
        car_id: Car identifier (e.g., "aston_martin_dbx")
        suffix: Image type (e.g., "main", "brand_logo")
        
    Returns:
        Formatted url_id
        
    Examples:
        >>> generate_image_url_id("aston_martin_dbx", "main")
        'aston_martin_dbx_main'
        >>> generate_image_url_id("aston_martin_dbx", "brand_logo")
        'aston_martin_dbx_brand_logo'
    """
    return f"{car_id}_{suffix}"


def process_image_urls(car_id: str, raw_data: dict) -> dict[str, dict]:
    """
    Process all image URLs in the car data and assign url_ids.
    
    Args:
        car_id: Car identifier
        raw_data: Raw car data dictionary
        
    Returns:
        Dict mapping field paths to image reference dicts
        
    Example:
        {
            "basic_info.image_url": {
                "url": "https://...",
                "url_id": "aston_martin_dbx_main",
                "alt_text": "Aston Martin DBX"
            },
            "brand.image": {
                "url": "https://...",
                "url_id": "aston_martin_dbx_brand_logo",
                "alt_text": "Aston Martin Logo"
            }
        }
    """
    image_refs = {}
    
    # Process main car image
    if raw_data.get("basic_info", {}).get("image_url"):
        car_name = raw_data.get("basic_info", {}).get("name", "Car")
        image_refs["basic_info.image_url"] = {
            "url": raw_data["basic_info"]["image_url"],
            "url_id": generate_image_url_id(car_id, "main"),
            "alt_text": car_name
        }
    
    # Process brand logo
    if raw_data.get("brand", {}).get("image"):
        brand_name = raw_data.get("brand", {}).get("name", "Brand")
        image_refs["brand.image"] = {
            "url": raw_data["brand"]["image"],
            "url_id": generate_image_url_id(car_id, "brand_logo"),
            "alt_text": f"{brand_name} Logo"
        }
    
    return image_refs


def preprocess_car_data(raw_data: dict, car_id: str) -> dict:
    """
    Apply all preprocessing transformations to car data.
    
    Args:
        raw_data: Raw car data from JSON
        car_id: Car identifier
        
    Returns:
        Preprocessed data dictionary ready for Pydantic validation
    """
    processed = {}
    
    # Add car_id
    processed["id"] = car_id
    
    # Process image URLs first
    image_refs = process_image_urls(car_id, raw_data)
    
    # Process basic_info
    if "basic_info" in raw_data:
        basic_info = raw_data["basic_info"].copy()
        
        # Add image reference if exists
        if "basic_info.image_url" in image_refs:
            basic_info["image_url"] = image_refs["basic_info.image_url"]
        
        processed["basic_info"] = basic_info
    
    # Process engine
    if "engine" in raw_data:
        engine = {}
        
        if "displacement" in raw_data["engine"]:
            engine["displacement"] = parse_engine_displacement(raw_data["engine"]["displacement"])
        
        if "power" in raw_data["engine"]:
            engine["power"] = parse_power_torque(raw_data["engine"]["power"])
        
        if "torque" in raw_data["engine"]:
            engine["torque"] = parse_power_torque(raw_data["engine"]["torque"])
        
        if "fuel_type" in raw_data["engine"]:
            fuel_types = parse_multi_value_field(raw_data["engine"]["fuel_type"])
            engine["fuel_type"] = normalize_fuel_type(fuel_types)
        
        processed["engine"] = engine
    
    # Process transmission
    if "transmission" in raw_data:
        processed["transmission"] = parse_multi_value_field(raw_data["transmission"])
    
    # Process fuel
    if "fuel" in raw_data:
        fuel = {}
        
        if "type" in raw_data["fuel"]:
            fuel_types = parse_multi_value_field(raw_data["fuel"]["type"])
            fuel["type"] = normalize_fuel_type(fuel_types)
        
        if "efficiency" in raw_data["fuel"]:
            fuel["efficiency"] = parse_mileage(raw_data["fuel"]["efficiency"])
        
        processed["fuel"] = fuel
    
    # Process dimensions
    if "dimensions" in raw_data:
        dims = {}
        
        if "width" in raw_data["dimensions"]:
            dims["width"] = parse_dimension(raw_data["dimensions"]["width"])
        
        if "height" in raw_data["dimensions"]:
            dims["height"] = parse_dimension(raw_data["dimensions"]["height"])
        
        if "weight" in raw_data["dimensions"]:
            dims["weight"] = parse_weight(raw_data["dimensions"]["weight"])
        
        if "seating_capacity" in raw_data["dimensions"]:
            dims["seating_capacity"] = parse_seating_capacity(raw_data["dimensions"]["seating_capacity"])
        
        if "number_of_doors" in raw_data["dimensions"]:
            dims["number_of_doors"] = parse_number_of_doors(raw_data["dimensions"]["number_of_doors"])
        
        processed["dimensions"] = dims
    
    # Process price
    if "price" in raw_data:
        price = raw_data["price"].copy()
        if "value" in price:
            price["value"] = parse_price(price["value"])
        processed["price"] = price
    
    # Process brand
    if "brand" in raw_data:
        brand = raw_data["brand"].copy()
        
        # Add image reference if exists
        if "brand.image" in image_refs:
            brand["image"] = image_refs["brand.image"]
        
        processed["brand"] = brand
    
    # Process rating
    if "rating" in raw_data:
        rating = raw_data["rating"].copy()
        if "value" in rating:
            rating["value"] = parse_rating(rating["value"])
        processed["rating"] = rating
    
    # Copy other fields as-is
    for key in ["colors", "reviewed_by", "pros", "cons", "verdict", 
                "competitor_comparison", "mileage_details", "whats_new", "review_videos"]:
        if key in raw_data:
            processed[key] = raw_data[key]
    
    return processed
