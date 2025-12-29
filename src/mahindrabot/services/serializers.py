"""Serialization functions for compact, human/LLM-readable output."""

from pydantic import BaseModel

from mahindrabot.models.car import CarComparison, CarDetail
from mahindrabot.models.bike import BikeComparison, BikeDetail
from mahindrabot.models.ev_location import EVLocationResult


class QNAResult(BaseModel):
    """Result model for FAQ search queries."""
    
    id: str
    question: str
    answer: str
    score: float
    category: str
    subcategory: str


def _format_price(price_value: int) -> str:
    """
    Format price in compact notation (Lakh/Crore).
    
    Args:
        price_value: Price in INR
        
    Returns:
        Formatted price string
        
    Examples:
        >>> _format_price(1000000)
        '₹10L'
        >>> _format_price(43797297)
        '₹4.38Cr'
    """
    if price_value >= 10000000:  # 1 Crore or more
        crore = price_value / 10000000
        return f"₹{crore:.2f}Cr"
    elif price_value >= 100000:  # 1 Lakh or more
        lakh = price_value / 100000
        return f"₹{lakh:.2f}L"
    else:
        return f"₹{price_value}"


def _format_displacement(vehicle) -> str:
    """
    Format engine displacement.
    
    Args:
        car: CarDetail object
        
    Returns:
        Formatted displacement string
    """
    if not vehicle.engine or not vehicle.engine.displacement:
        return "N/A"
    
    if len(vehicle.engine.displacement) == 1:
        disp = vehicle.engine.displacement[0]
        return f"{disp.value}{disp.unit}"
    else:
        # Multiple displacements
        disps = [f"{d.value}{d.unit}" for d in vehicle.engine.displacement]
        return "/".join(disps)


def _format_fuel_types(vehicle) -> str | None:
    """
    Format fuel types.
    
    Args:
        car: CarDetail object
        
    Returns:
        Formatted fuel types string or None if not available
    """
    fuel_types = []
    
    if vehicle.fuel and vehicle.fuel.type:
        fuel_types.extend(vehicle.fuel.type)
    elif vehicle.engine and vehicle.engine.fuel_type:
        fuel_types.extend(vehicle.engine.fuel_type)
    
    if not fuel_types:
        return None
    
    return ", ".join(fuel_types)


def _format_transmission(vehicle) -> str | None:
    """
    Format transmission types.
    
    Args:
        car: CarDetail object
        
    Returns:
        Formatted transmission string or None if not available
    """
    if not vehicle.transmission:
        return None
    
    return ", ".join(vehicle.transmission)


def _format_mileage(vehicle) -> str | None:
    """
    Format mileage/efficiency.
    
    Args:
        car: CarDetail object
        
    Returns:
        Formatted mileage string with units or None if not available
    """
    if not vehicle.fuel or not vehicle.fuel.efficiency:
        return None
    
    eff = vehicle.fuel.efficiency
    eff_type = eff.get("type", "fuel")
    
    if "value" in eff:
        value = eff['value']
        if eff_type == "electric":
            return f"{value:.1f} km range per charge"
        else:
            return f"{value:.1f} km/l"
    elif "min" in eff and "max" in eff:
        min_val = eff['min']
        max_val = eff['max']
        if eff_type == "electric":
            return f"{min_val:.1f}-{max_val:.1f} km range per charge"
        else:
            return f"{min_val:.1f}-{max_val:.1f} km/l"
    
    return None


def _format_rating(vehicle) -> str | None:
    """
    Format rating.
    
    Args:
        car: CarDetail object
        
    Returns:
        Formatted rating string or None if not available
    """
    if vehicle.rating and vehicle.rating.value is not None:
        return f"{vehicle.rating.value}/10"
    return None


def _format_image_reference(image_ref, label: str) -> str:
    """
    Format image reference as markdown.
    
    Args:
        image_ref: ImageReference object
        label: Label for the image
        
    Returns:
        Markdown formatted string
    """
    if image_ref:
        return f"[{label}]({image_ref.url})"
    return ""


def serialize_car_detail(car_detail: CarDetail) -> str:
    """
    Serialize CarDetail to detailed, human-readable string.
    
    Shows all available information in a clear, organized format.
    
    Args:
        car_detail: CarDetail object to serialize
        
    Returns:
        Human-readable string representation with all details
    """
    lines = []
    
    # Header with car name
    lines.append("=" * 80)
    lines.append(f"🚗 {car_detail.basic_info.name}")
    lines.append(f"ID: {car_detail.id}")
    lines.append("=" * 80)
    lines.append("")
    
    # Basic Information
    lines.append("📋 BASIC INFORMATION")
    lines.append("-" * 80)
    if car_detail.basic_info.manufacturer:
        lines.append(f"Manufacturer: {car_detail.basic_info.manufacturer}")
    lines.append(f"Model: {car_detail.basic_info.model}")
    if car_detail.basic_info.body_type:
        lines.append(f"Body Type: {car_detail.basic_info.body_type}")
    if car_detail.basic_info.image_url:
        img_ref = _format_image_reference(
            car_detail.basic_info.image_url,
            car_detail.basic_info.name
        )
        lines.append(f"Image: {img_ref}")
    if car_detail.basic_info.description:
        lines.append(f"Description: {car_detail.basic_info.description}")
    lines.append("")
    
    # Price
    lines.append("💰 PRICE")
    lines.append("-" * 80)
    lines.append(f"Price: {_format_price(car_detail.price.value)}")
    lines.append(f"Brand: {car_detail.brand.name}")
    if car_detail.brand.image:
        brand_logo = _format_image_reference(
            car_detail.brand.image,
            f"{car_detail.brand.name} Logo"
        )
        lines.append(f"Brand Logo: {brand_logo}")
    lines.append("")
    
    # Engine & Performance (only if available)
    has_engine_info = (car_detail.engine or car_detail.transmission or 
                       car_detail.fuel or car_detail.dimensions)
    
    if has_engine_info:
        lines.append("⚙️  ENGINE & PERFORMANCE")
        lines.append("-" * 80)
        
        # Engine displacement
        if car_detail.engine and car_detail.engine.displacement:
            disps = [f"{d.value} {d.unit}" for d in car_detail.engine.displacement]
            lines.append(f"Engine Displacement: {', '.join(disps)}")
        
        # Fuel type
        fuel_types = _format_fuel_types(car_detail)
        if fuel_types:
            lines.append(f"Fuel Type: {fuel_types}")
        
        # Transmission
        transmission = _format_transmission(car_detail)
        if transmission:
            lines.append(f"Transmission: {transmission}")
        
        # Mileage/Efficiency
        mileage = _format_mileage(car_detail)
        if mileage:
            lines.append(f"Mileage/Efficiency: {mileage}")
        
        # Power (if available)
        if car_detail.engine and car_detail.engine.power:
            power_specs = [f"{p.value} {p.unit}" for p in car_detail.engine.power]
            lines.append(f"Power: {', '.join(power_specs)}")
        
        # Torque (if available)
        if car_detail.engine and car_detail.engine.torque:
            torque_specs = [f"{t.value} {t.unit}" for t in car_detail.engine.torque]
            lines.append(f"Torque: {', '.join(torque_specs)}")
        
        lines.append("")
    
    # Dimensions (only if available)
    if car_detail.dimensions:
        lines.append("📏 DIMENSIONS")
        lines.append("-" * 80)
        lines.append(f"Seating Capacity: {car_detail.dimensions.seating_capacity} seats")
        
        if car_detail.dimensions.number_of_doors:
            lines.append(f"Number of Doors: {car_detail.dimensions.number_of_doors}")
        
        if car_detail.dimensions.width:
            lines.append(f"Width: {car_detail.dimensions.width.value} {car_detail.dimensions.width.unit}")
        
        if car_detail.dimensions.height:
            lines.append(f"Height: {car_detail.dimensions.height.value} {car_detail.dimensions.height.unit}")
        
        if car_detail.dimensions.weight:
            if "kerb_weight" in car_detail.dimensions.weight:
                lines.append(f"Kerb Weight: {car_detail.dimensions.weight['kerb_weight']} kg")
        
        lines.append("")
    
    # Colors (only if available)
    if car_detail.colors:
        lines.append(f"🎨 AVAILABLE COLORS ({len(car_detail.colors)})")
        lines.append("-" * 80)
        for i, color in enumerate(car_detail.colors, 1):
            lines.append(f"{i}. {color}")
        lines.append("")
    
    # Rating & Review (only if available)
    if car_detail.rating or car_detail.reviewed_by or car_detail.pros or car_detail.cons:
        lines.append("⭐ RATING & REVIEW")
        lines.append("-" * 80)
        
        rating = _format_rating(car_detail)
        if rating:
            lines.append(f"Expert Rating: {rating}")
        
        if car_detail.reviewed_by:
            lines.append(f"Reviewed By: {car_detail.reviewed_by.name}")
            if car_detail.reviewed_by.job_title:
                lines.append(f"Position: {car_detail.reviewed_by.job_title}")
        
        if car_detail.pros:
            lines.append(f"\n✅ Pros:")
            for pro in car_detail.pros:
                lines.append(f"  • {pro}")
        
        if car_detail.cons:
            lines.append(f"\n❌ Cons:")
            for con in car_detail.cons:
                lines.append(f"  • {con}")
        
        lines.append("")
    
    # Verdict (only if available)
    if car_detail.verdict:
        lines.append("📝 EXPERT VERDICT")
        lines.append("-" * 80)
        lines.append(car_detail.verdict)
        lines.append("")
    
    # What's New (only if available)
    if car_detail.whats_new:
        lines.append(f"🆕 WHAT'S NEW")
        lines.append("-" * 80)
        for section_name, points in car_detail.whats_new.items():
            lines.append(f"\n{section_name}:")
            for point in points:
                lines.append(f"  • {point}")
        lines.append("")
    
    # Competitor Comparison (only if available)
    if car_detail.competitor_comparison:
        competitors = [c.name for c in car_detail.competitor_comparison.cars if c.name != car_detail.basic_info.name]
        if competitors:
            lines.append(f"🔄 COMPETITOR COMPARISON")
            lines.append("-" * 80)
            lines.append(f"Compared with: {', '.join(competitors)}")
            lines.append("")
    
    # Review Videos (only if available)
    if car_detail.review_videos:
        lines.append("🎬 REVIEW VIDEOS")
        lines.append("-" * 80)
        for i, video in enumerate(car_detail.review_videos, 1):
            video_line = f"{i}. [{video.title}]({video.url})"
            if video.youtube_id:
                video_line += f" (YouTube: {video.youtube_id})"
            lines.append(video_line)
        lines.append("")
    
    return "\n".join(lines)


def serialize_car_comparison(car_comparison: CarComparison) -> str:
    """
    Serialize CarComparison to compact table format.
    
    Args:
        car_comparison: CarComparison object to serialize
        
    Returns:
        Compact table string
        
    Example output:
        COMPARISON: Aston Martin DBX vs Lamborghini Urus vs Mercedes-AMG G 63
        ──────────────────────────────────────────────────────────────────
        Feature              | Car 1           | Car 2           | Car 3
        ──────────────────────────────────────────────────────────────────
        Price (INR)          | ₹4.38Cr         | ₹4.75Cr         | ₹3.85Cr
        Brand                | Aston Martin    | Lamborghini     | Mercedes-AMG
        Body Type            | SUV             | SUV             | SUV
        Engine Displacement  | 3982cc          | 3996cc          | 3998cc
        Fuel Type            | Diesel          | Petrol          | Petrol
        Transmission         | Automatic       | Automatic       | Automatic
        Mileage              | 12.0 km/l       | 7.8 km/l        | 8.47 km/l
        Seating Capacity     | 5               | 5               | 5
        Rating               | 7.7/10          | 8.6/10          | 7.3/10
    """
    lines = []
    
    # Header with car names
    car_names = [car.basic_info.name for car in car_comparison.cars]
    header = f"COMPARISON: {' vs '.join(car_names)}"
    lines.append(header)
    lines.append("─" * len(header))
    
    # Table header
    table_header = ["Feature"]
    for i in range(len(car_comparison.cars)):
        table_header.append(f"Car {i+1}")
    
    # Determine column widths
    col_widths = [20]  # Feature column
    for _ in car_comparison.cars:
        col_widths.append(15)  # Car columns
    
    # Format header row
    header_row = " | ".join([
        name.ljust(width) 
        for name, width in zip(table_header, col_widths)
    ])
    lines.append(header_row)
    lines.append("─" * len(header_row))
    
    # Format data rows
    for feature, values in car_comparison.comparison_matrix.items():
        row_parts = [feature.ljust(col_widths[0])]
        
        for i, value in enumerate(values):
            # Format value based on type
            if isinstance(value, int):
                if feature == "Price (INR)":
                    formatted = _format_price(value)
                else:
                    formatted = str(value)
            else:
                formatted = str(value)
            
            row_parts.append(formatted.ljust(col_widths[i + 1]))
        
        lines.append(" | ".join(row_parts))
    
    # Add image references at the bottom
    lines.append("")
    lines.append("Images:")
    for i, car in enumerate(car_comparison.cars):
        if car.basic_info.image_url:
            img_ref = _format_image_reference(
                car.basic_info.image_url,
                car.basic_info.name
            )
            lines.append(f"  Car {i+1}: {img_ref}")
    
    return "\n".join(lines)


def serialize_ev_location(ev_location: EVLocationResult) -> str:
    """
    Serialize EVLocationResult to detailed, human-readable string.
    
    Shows all available information about an EV charging station in a clear,
    organized format suitable for LLM consumption and user display.
    
    Args:
        ev_location: EVLocationResult object to serialize
        
    Returns:
        Human-readable string representation with all details
    """
    lines = []
    
    # Header with charging station name/id
    lines.append("=" * 80)
    if ev_location.name:
        lines.append(f"🔌 {ev_location.name}")
    else:
        lines.append(f"🔌 EV Charging Station")
    lines.append(f"ID: {ev_location.id}")
    if ev_location.distance_km is not None:
        lines.append(f"📍 Distance: {ev_location.distance_km:.2f} km away")
    lines.append("=" * 80)
    lines.append("")
    
    # Location Information
    lines.append("📍 LOCATION")
    lines.append("-" * 80)
    lines.append(f"Address: {ev_location.address}")
    lines.append(f"City: {ev_location.city}")
    lines.append(f"Postal Code: {ev_location.postal_code}")
    lines.append(f"Country: {ev_location.country}")
    lines.append(f"Coordinates: {ev_location.latitude}, {ev_location.longitude}")
    
    # Google Maps link
    if ev_location.google_maps_link:
        lines.append(f"\n🗺️  Google Maps: {ev_location.google_maps_link}")
    lines.append("")
    
    # Charging Specifications
    lines.append("⚡ CHARGING SPECIFICATIONS")
    lines.append("-" * 80)
    lines.append(f"Capacity: {ev_location.capacity}")
    lines.append(f"Charger Type: {ev_location.charger_type}")
    lines.append(f"Charging Type: {ev_location.charging_type}")
    lines.append(f"Total Chargers: {ev_location.no_of_chargers}")
    lines.append(f"Currently Available: {ev_location.available}")
    lines.append("")
    
    # Operating Hours
    lines.append("🕐 OPERATING HOURS")
    lines.append("-" * 80)
    lines.append(f"Timing: {ev_location.timing}")
    lines.append(f"Open: {ev_location.open}")
    lines.append(f"Close: {ev_location.close}")
    lines.append(f"Staff: {ev_location.staff}")
    lines.append("")
    
    # Payment Information
    lines.append("💳 PAYMENT INFORMATION")
    lines.append("-" * 80)
    lines.append(f"Cost per Unit: ₹{ev_location.cost_per_unit}")
    lines.append(f"Payment Modes: {ev_location.payment_modes}")
    lines.append("")
    
    # Additional Information
    lines.append("ℹ️  ADDITIONAL INFORMATION")
    lines.append("-" * 80)
    lines.append(f"Vendor/Operator: {ev_location.vendor}")
    if ev_location.contact_number:
        lines.append(f"Contact Number: {ev_location.contact_number}")
    lines.append("")
    
    return "\n".join(lines)


def serialize_multiple_ev_locations(ev_locations: list[EVLocationResult]) -> str:
    """
    Serialize multiple EVLocationResult objects to human-readable string.
    
    Shows a summary list followed by detailed information for each location.
    
    Args:
        ev_locations: List of EVLocationResult objects to serialize
        
    Returns:
        Human-readable string representation with all locations
    """
    if not ev_locations:
        return "No EV charging stations found."
    
    lines = []
    
    # Header with count
    lines.append("=" * 80)
    lines.append(f"🔌 FOUND {len(ev_locations)} EV CHARGING STATION{'S' if len(ev_locations) > 1 else ''}")
    lines.append("=" * 80)
    lines.append("")
    
    # Summary list
    lines.append("📋 SUMMARY (Sorted by Distance)")
    lines.append("-" * 80)
    for i, loc in enumerate(ev_locations, 1):
        name = loc.name if loc.name else f"Station {loc.id}"
        lines.append(f"{i}. {name}")
        lines.append(f"   📍 {loc.address}, {loc.city}")
        lines.append(f"   🚗 {loc.distance_km:.2f} km away")
        lines.append(f"   ⚡ {loc.available}/{loc.no_of_chargers} chargers available • {loc.capacity}")
        lines.append("")
    
    lines.append("=" * 80)
    lines.append("")
    
    # Detailed information for each location
    for i, loc in enumerate(ev_locations, 1):
        lines.append("=" * 80)
        lines.append(f"LOCATION #{i}")
        lines.append("=" * 80)
        lines.append("")
        
        if loc.name:
            lines.append(f"🔌 {loc.name}")
        else:
            lines.append("🔌 EV Charging Station")
        lines.append(f"ID: {loc.id}")
        lines.append(f"📍 Distance: {loc.distance_km:.2f} km away")
        lines.append("")
        
        # Location
        lines.append("📍 LOCATION")
        lines.append("-" * 80)
        lines.append(f"Address: {loc.address}")
        lines.append(f"City: {loc.city}")
        lines.append(f"Postal Code: {loc.postal_code}")
        if loc.google_maps_link:
            lines.append(f"\n🗺️  Google Maps: {loc.google_maps_link}")
        lines.append("")
        
        # Charging Specs (compact)
        lines.append("⚡ CHARGING")
        lines.append("-" * 80)
        lines.append(f"{loc.available}/{loc.no_of_chargers} chargers available • {loc.capacity} • {loc.charger_type}")
        lines.append(f"Cost: ₹{loc.cost_per_unit}/unit • {loc.payment_modes}")
        lines.append("")
        
        # Operating Hours (compact)
        lines.append("🕐 HOURS")
        lines.append("-" * 80)
        lines.append(f"{loc.timing} • {loc.staff}")
        lines.append("")
        
        # Vendor (compact)
        lines.append(f"Operator: {loc.vendor}")
        if loc.contact_number:
            lines.append(f"Contact: {loc.contact_number}")
        lines.append("")
        
        # Separator between locations (except last one)
        if i < len(ev_locations):
            lines.append("")
    
    return "\n".join(lines)


def serialize_bike_detail(bike_detail: BikeDetail) -> str:
    """
    Serialize BikeDetail to detailed, human-readable string.
    
    Shows all available information in a clear, organized format.
    
    Args:
        bike_detail: BikeDetail object to serialize
        
    Returns:
        Human-readable string representation with all details
    """
    lines = []
    
    # Header with bike name
    lines.append("=" * 80)
    lines.append(f"🏍️ {bike_detail.basic_info.name}")
    lines.append(f"ID: {bike_detail.id}")
    lines.append("=" * 80)
    lines.append("")
    
    # Basic Information
    lines.append("📋 BASIC INFORMATION")
    lines.append("-" * 80)
    if bike_detail.basic_info.manufacturer:
        lines.append(f"Manufacturer: {bike_detail.basic_info.manufacturer}")
    lines.append(f"Model: {bike_detail.basic_info.model}")
    if bike_detail.basic_info.body_type:
        lines.append(f"Body Type: {bike_detail.basic_info.body_type}")
    if bike_detail.basic_info.image_url:
        img_ref = _format_image_reference(
            bike_detail.basic_info.image_url,
            bike_detail.basic_info.name
        )
        lines.append(f"Image: {img_ref}")
    if bike_detail.basic_info.description:
        lines.append(f"Description: {bike_detail.basic_info.description}")
    lines.append("")
    
    # Price
    lines.append("💰 PRICE")
    lines.append("-" * 80)
    lines.append(f"Price: {_format_price(bike_detail.price.value)}")
    lines.append(f"Brand: {bike_detail.brand.name}")
    if bike_detail.brand.image:
        brand_logo = _format_image_reference(
            bike_detail.brand.image,
            f"{bike_detail.brand.name} Logo"
        )
        lines.append(f"Brand Logo: {brand_logo}")
    lines.append("")
    
    # Engine & Performance (only if available)
    has_engine_info = (bike_detail.engine or bike_detail.transmission or 
                       bike_detail.fuel or bike_detail.dimensions)
    
    if has_engine_info:
        lines.append("⚙️  ENGINE & PERFORMANCE")
        lines.append("-" * 80)
        
        # Engine displacement
        if bike_detail.engine and bike_detail.engine.displacement:
            disps = [f"{d.value} {d.unit}" for d in bike_detail.engine.displacement]
            lines.append(f"Engine Displacement: {', '.join(disps)}")
        
        # Fuel type
        fuel_types = _format_fuel_types(bike_detail)
        if fuel_types:
            lines.append(f"Fuel Type: {fuel_types}")
        
        # Transmission
        transmission = _format_transmission(bike_detail)
        if transmission:
            lines.append(f"Transmission: {transmission}")
        
        # Mileage/Efficiency
        mileage = _format_mileage(bike_detail)
        if mileage:
            lines.append(f"Mileage/Efficiency: {mileage}")
        
        # Power (if available)
        if bike_detail.engine and bike_detail.engine.power:
            power_specs = [f"{p.value} {p.unit}" for p in bike_detail.engine.power]
            lines.append(f"Power: {', '.join(power_specs)}")
        
        # Torque (if available)
        if bike_detail.engine and bike_detail.engine.torque:
            torque_specs = [f"{t.value} {t.unit}" for t in bike_detail.engine.torque]
            lines.append(f"Torque: {', '.join(torque_specs)}")
        
        lines.append("")
    
    # Dimensions (only if available)
    if bike_detail.dimensions:
        lines.append("📏 DIMENSIONS")
        lines.append("-" * 80)
        
        if bike_detail.dimensions.seat_height:
            lines.append(f"Seat Height: {bike_detail.dimensions.seat_height.value} {bike_detail.dimensions.seat_height.unit}")
            
        if bike_detail.dimensions.ground_clearance:
            lines.append(f"Ground Clearance: {bike_detail.dimensions.ground_clearance.value} {bike_detail.dimensions.ground_clearance.unit}")
        
        if bike_detail.dimensions.width:
            lines.append(f"Width: {bike_detail.dimensions.width.value} {bike_detail.dimensions.width.unit}")
        
        if bike_detail.dimensions.height:
            lines.append(f"Height: {bike_detail.dimensions.height.value} {bike_detail.dimensions.height.unit}")
        
        if bike_detail.dimensions.weight:
            if "kerb_weight" in bike_detail.dimensions.weight:
                lines.append(f"Kerb Weight: {bike_detail.dimensions.weight['kerb_weight']} kg")
        
        lines.append("")
    
    # Features (only if available)
    if bike_detail.features:
        lines.append(f"🛠️  FEATURES")
        lines.append("-" * 80)
        for i, feature in enumerate(bike_detail.features, 1):
            lines.append(f"{i}. {feature}")
        lines.append("")

    # Colors (only if available)
    if bike_detail.colors:
        lines.append(f"🎨 AVAILABLE COLORS ({len(bike_detail.colors)})")
        lines.append("-" * 80)
        for i, color in enumerate(bike_detail.colors, 1):
            lines.append(f"{i}. {color}")
        lines.append("")
    
    # Rating & Review (only if available)
    if bike_detail.rating or bike_detail.reviewed_by or bike_detail.pros or bike_detail.cons:
        lines.append("⭐ RATING & REVIEW")
        lines.append("-" * 80)
        
        rating = _format_rating(bike_detail)
        if rating:
            lines.append(f"Expert Rating: {rating}")
        
        if bike_detail.reviewed_by:
            lines.append(f"Reviewed By: {bike_detail.reviewed_by.name}")
            if bike_detail.reviewed_by.job_title:
                lines.append(f"Position: {bike_detail.reviewed_by.job_title}")
        
        if bike_detail.pros:
            lines.append(f"\n✅ Pros:")
            for pro in bike_detail.pros:
                lines.append(f"  • {pro}")
        
        if bike_detail.cons:
            lines.append(f"\n❌ Cons:")
            for con in bike_detail.cons:
                lines.append(f"  • {con}")
        
        lines.append("")
    
    # Verdict (only if available)
    if bike_detail.verdict:
        lines.append("📝 EXPERT VERDICT")
        lines.append("-" * 80)
        lines.append(bike_detail.verdict)
        lines.append("")
    
    # What's New (only if available)
    if bike_detail.whats_new:
        lines.append(f"🆕 WHAT'S NEW")
        lines.append("-" * 80)
        for section_name, points in bike_detail.whats_new.items():
            lines.append(f"\n{section_name}:")
            for point in points:
                lines.append(f"  • {point}")
        lines.append("")
    
    # Competitor Comparison (only if available)
    if bike_detail.competitor_comparison:
        competitors = [b.name for b in bike_detail.competitor_comparison.bikes if b.name != bike_detail.basic_info.name]
        if competitors:
            lines.append(f"🔄 COMPETITOR COMPARISON")
            lines.append("-" * 80)
            lines.append(f"Compared with: {', '.join(competitors)}")
            lines.append("")
    
    return "\n".join(lines)


def serialize_bike_comparison(bike_comparison: BikeComparison) -> str:
    """
    Serialize BikeComparison to compact table format.
    
    Args:
        bike_comparison: BikeComparison object to serialize
        
    Returns:
        Compact table string
    """
    lines = []
    
    # Header with bike names
    bike_names = [bike.basic_info.name for bike in bike_comparison.bikes]
    header = f"COMPARISON: {' vs '.join(bike_names)}"
    lines.append(header)
    lines.append("─" * len(header))
    
    # Table header
    table_header = ["Feature"]
    for i in range(len(bike_comparison.bikes)):
        table_header.append(f"Bike {i+1}")
    
    # Determine column widths
    col_widths = [20]  # Feature column
    for _ in bike_comparison.bikes:
        col_widths.append(15)  # Bike columns
    
    # Format header row
    header_row = " | ".join([
        name.ljust(width) 
        for name, width in zip(table_header, col_widths)
    ])
    lines.append(header_row)
    lines.append("─" * len(header_row))
    
    # Format data rows
    for feature, values in bike_comparison.comparison_matrix.items():
        row_parts = [feature.ljust(col_widths[0])]
        
        for i, value in enumerate(values):
            # Format value based on type
            if isinstance(value, int):
                if feature == "Price (INR)":
                    formatted = _format_price(value)
                else:
                    formatted = str(value)
            else:
                formatted = str(value)
            
            row_parts.append(formatted.ljust(col_widths[i + 1]))
        
        lines.append(" | ".join(row_parts))
    
    # Add image references at the bottom
    lines.append("")
    lines.append("Images:")
    for i, bike in enumerate(bike_comparison.bikes):
        if bike.basic_info.image_url:
            img_ref = _format_image_reference(
                bike.basic_info.image_url,
                bike.basic_info.name
            )
            lines.append(f"  Bike {i+1}: {img_ref}")
    
    return "\n".join(lines)
