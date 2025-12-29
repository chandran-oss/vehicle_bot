"""CarService for managing and querying car data."""

import json
import os
from pathlib import Path
from typing import Optional

from thefuzz import fuzz, process

from mahindrabot.models.car import CarComparison, CarDetail

from .data_preprocessor import preprocess_car_data


class CarNotFoundError(Exception):
    """Raised when a car is not found."""
    pass


class InvalidFilterError(Exception):
    """Raised when an invalid filter value is provided."""
    
    def __init__(self, filter_name: str, invalid_value: str, suggestions: list[str]):
        self.filter_name = filter_name
        self.invalid_value = invalid_value
        self.suggestions = suggestions
        
        message = f"Invalid {filter_name}: '{invalid_value}'\nDid you mean one of these?\n"
        for i, suggestion in enumerate(suggestions[:5], 1):
            message += f"  {i}. {suggestion}\n"
        
        super().__init__(message)


class CarService:
    """Service for loading, filtering, searching, and comparing car data."""
    
    def __init__(self, json_folder: str, fuzzy_threshold: int = 70):
        """
        Initialize CarService by loading and preprocessing JSON files.
        
        Args:
            json_folder: Path to folder containing car JSON files
            fuzzy_threshold: Minimum similarity score for fuzzy search (0-100)
        """
        self.fuzzy_threshold = fuzzy_threshold
        self.cars: dict[str, CarDetail] = {}
        
        # Track unique values for filter validation
        self.available_brands: set[str] = set()
        self.available_body_types: set[str] = set()
        self.available_fuel_types: set[str] = set()
        self.available_transmissions: set[str] = set()
        
        self._load_cars(json_folder)
    
    def _load_cars(self, json_folder: str) -> None:
        """
        Load all car/bike JSON files from folder recursively.
        
        Args:
            json_folder: Path to folder containing JSON files
        """
        folder_path = Path(json_folder)
        
        if not folder_path.exists():
            raise ValueError(f"Data folder not found: {json_folder}")
        
        # Load JSON files recursively to support cars/ bikes/ etc.
        json_files = list(folder_path.rglob("*.json"))
        
        if not json_files:
            print(f"Warning: No JSON files found in: {json_folder}")
            return
        
        for json_file in json_files:
            # Generate car_id from filename (lowercase with underscores)
            car_id = json_file.stem.lower()
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                
                # Only load if it looks like vehicle data (has basic_info and price)
                if "basic_info" not in raw_data or "price" not in raw_data:
                    continue
                
                # Preprocess the data
                preprocessed = preprocess_car_data(raw_data, car_id)
                
                # Validate and create CarDetail model
                car = CarDetail(**preprocessed)
                self.cars[car_id] = car
                
                # Track unique filter values
                if car.brand and car.brand.name:
                    self.available_brands.add(car.brand.name)
                
                if car.basic_info.body_type:
                    self.available_body_types.add(car.basic_info.body_type)
                
                # Collect fuel types
                if car.fuel and car.fuel.type:
                    self.available_fuel_types.update(car.fuel.type)
                if car.engine and car.engine.fuel_type:
                    self.available_fuel_types.update(car.engine.fuel_type)
                
                # Collect transmission types
                if car.transmission:
                    self.available_transmissions.update(car.transmission)
                
            except Exception as e:
                print(f"Warning: Failed to load {json_file.name}: {e}")
                continue
        
        print(f"Loaded {len(self.cars)} cars successfully")
        print(f"Available brands: {len(self.available_brands)}")
        print(f"Available body types: {len(self.available_body_types)}")
        print(f"Available fuel types: {len(self.available_fuel_types)}")
        print(f"Available transmissions: {len(self.available_transmissions)}")
    
    def get_car_details(self, car_id: str) -> CarDetail:
        """
        Get basic car details (without extended information).
        
        Args:
            car_id: Car identifier
            
        Returns:
            CarDetail with basic fields only
            
        Raises:
            CarNotFoundError: If car_id is not found
        """
        car = self.cars.get(car_id.lower())
        if car:
            return car.get_basic_only()
        
        # Car not found - suggest similar car IDs
        available_ids = list(self.cars.keys())
        suggestions = process.extract(car_id.lower(), available_ids, limit=5)
        suggestion_list = [s[0] for s in suggestions]
        
        raise CarNotFoundError(
            f"Car '{car_id}' not found.\n"
            f"Did you mean one of these?\n" + 
            "\n".join(f"  {i}. {s}" for i, s in enumerate(suggestion_list, 1))
        )
    
    def get_extended_car_details(self, car_id: str) -> CarDetail:
        """
        Get full car details including all extended information.
        
        Args:
            car_id: Car identifier
            
        Returns:
            Complete CarDetail
            
        Raises:
            CarNotFoundError: If car_id is not found
        """
        car = self.cars.get(car_id.lower())
        if car:
            return car
        
        # Car not found - suggest similar car IDs
        available_ids = list(self.cars.keys())
        suggestions = process.extract(car_id.lower(), available_ids, limit=5)
        suggestion_list = [s[0] for s in suggestions]
        
        raise CarNotFoundError(
            f"Car '{car_id}' not found.\n"
            f"Did you mean one of these?\n" + 
            "\n".join(f"  {i}. {s}" for i, s in enumerate(suggestion_list, 1))
        )
    
    def get_car_comparison(self, car_ids: list[str]) -> CarComparison:
        """
        Compare multiple cars.
        
        Args:
            car_ids: List of car identifiers to compare
            
        Returns:
            CarComparison with structured comparison matrix
            
        Raises:
            CarNotFoundError: If any car_id is not found
        """
        # Get extended details for all cars (will raise CarNotFoundError if not found)
        cars = []
        for car_id in car_ids:
            car = self.get_extended_car_details(car_id)
            cars.append(car)
        
        if not cars:
            raise ValueError("No cars provided for comparison")
        
        # Build comparison matrix
        comparison_matrix = {}
        
        # Price comparison
        comparison_matrix["Price (INR)"] = [car.price.value for car in cars]
        
        # Brand
        comparison_matrix["Brand"] = [car.brand.name for car in cars]
        
        # Body Type
        comparison_matrix["Body Type"] = [
            car.basic_info.body_type if car.basic_info.body_type else "N/A" 
            for car in cars
        ]
        
        # Engine Displacement
        comparison_matrix["Engine Displacement"] = []
        for car in cars:
            if car.engine and car.engine.displacement:
                disps = [f"{d.value}{d.unit}" for d in car.engine.displacement]
                comparison_matrix["Engine Displacement"].append(", ".join(disps))
            else:
                comparison_matrix["Engine Displacement"].append("N/A")
        
        # Fuel Type
        comparison_matrix["Fuel Type"] = []
        for car in cars:
            if car.fuel and car.fuel.type:
                comparison_matrix["Fuel Type"].append(", ".join(car.fuel.type))
            elif car.engine and car.engine.fuel_type:
                comparison_matrix["Fuel Type"].append(", ".join(car.engine.fuel_type))
            else:
                comparison_matrix["Fuel Type"].append("N/A")
        
        # Transmission
        comparison_matrix["Transmission"] = []
        for car in cars:
            if car.transmission:
                comparison_matrix["Transmission"].append(", ".join(car.transmission))
            else:
                comparison_matrix["Transmission"].append("N/A")
        
        # Mileage
        comparison_matrix["Mileage"] = []
        for car in cars:
            if car.fuel and car.fuel.efficiency:
                eff = car.fuel.efficiency
                if "value" in eff:
                    comparison_matrix["Mileage"].append(f"{eff['value']} {eff.get('unit', '')}")
                elif "min" in eff and "max" in eff:
                    comparison_matrix["Mileage"].append(f"{eff['min']}-{eff['max']} {eff.get('unit', '')}")
                else:
                    comparison_matrix["Mileage"].append("N/A")
            else:
                comparison_matrix["Mileage"].append("N/A")
        
        # Seating Capacity
        comparison_matrix["Seating Capacity"] = []
        for car in cars:
            if car.dimensions:
                comparison_matrix["Seating Capacity"].append(car.dimensions.seating_capacity)
            else:
                comparison_matrix["Seating Capacity"].append("N/A")
        
        # Rating
        comparison_matrix["Rating"] = []
        for car in cars:
            if car.rating and car.rating.value is not None:
                comparison_matrix["Rating"].append(f"{car.rating.value}/10")
            else:
                comparison_matrix["Rating"].append("N/A")
        
        return CarComparison(cars=cars, comparison_matrix=comparison_matrix)
    
    def _validate_filter_value(self, filter_name: str, value: str, available_values: set[str]) -> None:
        """
        Validate filter value and raise error with suggestions if invalid.
        
        Args:
            filter_name: Name of the filter
            value: Value to validate
            available_values: Set of valid values
            
        Raises:
            InvalidFilterError: If value is not in available_values
        """
        # Case-insensitive check
        available_values_lower = {v.lower(): v for v in available_values}
        
        if value.lower() not in available_values_lower:
            # Find closest matches using fuzzy matching
            suggestions = process.extract(value, list(available_values), limit=5)
            suggestion_list = [s[0] for s in suggestions if s[1] > 60]  # Only show reasonable matches
            
            if not suggestion_list:
                suggestion_list = sorted(list(available_values))[:5]  # Show first 5 if no good matches
            
            raise InvalidFilterError(filter_name, value, suggestion_list)
    
    def _matches_filters(
        self,
        car: CarDetail,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        brand: Optional[str] = None,
        body_type: Optional[str] = None,
        fuel_type: Optional[str] = None,
        mileage_more_than: Optional[float] = None,
        mileage_less_than: Optional[float] = None,
        seating_capacity: Optional[int] = None,
        transmission: Optional[str] = None,
        engine_displacement_more_than: Optional[int] = None,
        engine_displacement_less_than: Optional[int] = None,
    ) -> bool:
        """
        Check if car matches all provided filters (AND logic).
        
        Returns:
            True if car matches all filters
        """
        # Price filter
        if min_price is not None and car.price.value < min_price:
            return False
        if max_price is not None and car.price.value > max_price:
            return False
        
        # Brand filter (case-insensitive)
        if brand is not None:
            if not car.brand.name or car.brand.name.lower() != brand.lower():
                return False
        
        # Body type filter (case-insensitive)
        if body_type is not None:
            if not car.basic_info.body_type or car.basic_info.body_type.lower() != body_type.lower():
                return False
        
        # Fuel type filter (case-insensitive, check if exists in list)
        if fuel_type is not None:
            fuel_types = []
            if car.fuel and car.fuel.type:
                fuel_types.extend([ft.lower() for ft in car.fuel.type])
            if car.engine and car.engine.fuel_type:
                fuel_types.extend([ft.lower() for ft in car.engine.fuel_type])
            
            if fuel_type.lower() not in fuel_types:
                return False
        
        # Transmission filter (case-insensitive, check if exists in list)
        if transmission is not None:
            if not car.transmission:
                return False
            trans_list = [t.lower() for t in car.transmission]
            if transmission.lower() not in trans_list:
                return False
        
        # Seating capacity filter (exact match)
        if seating_capacity is not None:
            if not car.dimensions or car.dimensions.seating_capacity != seating_capacity:
                return False
        
        # Engine displacement filters (check against all displacement values)
        if engine_displacement_more_than is not None or engine_displacement_less_than is not None:
            if not car.engine or not car.engine.displacement:
                return False
            
            displacement_values = [d.value for d in car.engine.displacement]
            max_displacement = max(displacement_values)
            min_displacement = min(displacement_values)
            
            if engine_displacement_more_than is not None:
                if max_displacement <= engine_displacement_more_than:
                    return False
            
            if engine_displacement_less_than is not None:
                if min_displacement >= engine_displacement_less_than:
                    return False
        
        # Mileage filters (check against mileage values)
        if mileage_more_than is not None or mileage_less_than is not None:
            if not car.fuel or not car.fuel.efficiency:
                return False
            
            eff = car.fuel.efficiency
            car_mileage = None
            
            # Get mileage value (use max for range, single value otherwise)
            if "value" in eff:
                car_mileage = eff["value"]
            elif "max" in eff:
                car_mileage = eff["max"]
            elif "min" in eff:
                car_mileage = eff["min"]
            
            if car_mileage is None:
                return False
            
            if mileage_more_than is not None and car_mileage <= mileage_more_than:
                return False
            
            if mileage_less_than is not None and car_mileage >= mileage_less_than:
                return False
        
        return True
    
    def _get_sort_value(self, car: CarDetail, sort_by: str) -> tuple:
        """
        Extract sort value from car for the specified field.
        
        Returns a tuple for proper sorting (handles None values).
        
        Args:
            car: CarDetail to extract value from
            sort_by: Field to sort by
            
        Returns:
            Tuple of (has_value, value) for proper sorting
        """
        if sort_by == "price":
            return (True, car.price.value)
        
        elif sort_by == "mileage":
            if not car.fuel or not car.fuel.efficiency:
                return (False, 0)
            
            eff = car.fuel.efficiency
            # Get mileage value (use max for range, single value otherwise)
            if "value" in eff:
                return (True, eff["value"])
            elif "max" in eff:
                return (True, eff["max"])
            elif "min" in eff:
                return (True, eff["min"])
            return (False, 0)
        
        elif sort_by == "seating_capacity":
            if not car.dimensions or car.dimensions.seating_capacity is None:
                return (False, 0)
            return (True, car.dimensions.seating_capacity)
        
        elif sort_by == "engine_displacement":
            if not car.engine or not car.engine.displacement:
                return (False, 0)
            # Use the maximum displacement value
            displacement_values = [d.value for d in car.engine.displacement]
            return (True, max(displacement_values))
        
        return (True, 0)
    
    def list_cars(
        self,
        limit: int,
        offset: int = 0,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        brand: Optional[str] = None,
        body_type: Optional[str] = None,
        fuel_type: Optional[str] = None,
        mileage_more_than: Optional[float] = None,
        mileage_less_than: Optional[float] = None,
        seating_capacity: Optional[int] = None,
        transmission: Optional[str] = None,
        engine_displacement_more_than: Optional[int] = None,
        engine_displacement_less_than: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> list[CarDetail]:
        """
        List cars with optional filters and pagination.
        
        All filters are applied with AND logic.
        
        Args:
            limit: Maximum number of results
            offset: Number of results to skip
            min_price: Minimum price filter
            max_price: Maximum price filter
            brand: Brand name filter (case-insensitive, validated)
            body_type: Body type filter (case-insensitive, validated)
            fuel_type: Fuel type filter (case-insensitive, validated)
            mileage_more_than: Minimum mileage filter (exclusive)
            mileage_less_than: Maximum mileage filter (exclusive)
            seating_capacity: Seating capacity filter
            transmission: Transmission type filter (case-insensitive, validated)
            engine_displacement_more_than: Minimum engine displacement (exclusive)
            engine_displacement_less_than: Maximum engine displacement (exclusive)
            sort_by: Field to sort by (price, mileage, seating_capacity, engine_displacement)
            sort_order: Sort order (asc or desc), default is asc
            
        Returns:
            List of CarDetail with basic fields only
            
        Raises:
            InvalidFilterError: If filter value is invalid
            ValueError: If sort_by or sort_order is invalid
        """
        # Validate filter values
        if brand is not None:
            self._validate_filter_value("brand", brand, self.available_brands)
        
        if body_type is not None:
            self._validate_filter_value("body_type", body_type, self.available_body_types)
        
        if fuel_type is not None:
            self._validate_filter_value("fuel_type", fuel_type, self.available_fuel_types)
        
        if transmission is not None:
            self._validate_filter_value("transmission", transmission, self.available_transmissions)
        
        # Validate sort parameters
        valid_sort_fields = ["price", "mileage", "seating_capacity", "engine_displacement"]
        if sort_by is not None and sort_by not in valid_sort_fields:
            raise ValueError(f"Invalid sort_by: '{sort_by}'. Must be one of: {', '.join(valid_sort_fields)}")
        
        if sort_order not in ["asc", "desc"]:
            raise ValueError(f"Invalid sort_order: '{sort_order}'. Must be 'asc' or 'desc'")
        
        # Filter cars
        filtered_cars = []
        for car in self.cars.values():
            if self._matches_filters(
                car,
                min_price=min_price,
                max_price=max_price,
                brand=brand,
                body_type=body_type,
                fuel_type=fuel_type,
                mileage_more_than=mileage_more_than,
                mileage_less_than=mileage_less_than,
                seating_capacity=seating_capacity,
                transmission=transmission,
                engine_displacement_more_than=engine_displacement_more_than,
                engine_displacement_less_than=engine_displacement_less_than,
            ):
                filtered_cars.append(car)
        
        # Sort cars
        if sort_by:
            # Sort with None values last
            filtered_cars.sort(
                key=lambda c: self._get_sort_value(c, sort_by),
                reverse=(sort_order == "desc")
            )
        else:
            # Default sort by price (ascending)
            filtered_cars.sort(key=lambda c: c.price.value)
        
        # Apply pagination
        paginated = filtered_cars[offset:offset + limit]
        
        # Return basic details only
        return [car.get_basic_only() for car in paginated]
    
    def search(
        self,
        query: str,
        limit: int,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        brand: Optional[str] = None,
        body_type: Optional[str] = None,
        fuel_type: Optional[str] = None,
        mileage_more_than: Optional[float] = None,
        mileage_less_than: Optional[float] = None,
        seating_capacity: Optional[int] = None,
        transmission: Optional[str] = None,
        engine_displacement_more_than: Optional[int] = None,
        engine_displacement_less_than: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> list[CarDetail]:
        """
        Search cars by query string with optional filters.
        
        First attempts direct string search, falls back to fuzzy search if no results.
        All filters are applied with AND logic.
        
        Args:
            query: Search query string
            limit: Maximum number of results
            (filter args same as list_cars)
            sort_by: Field to sort by (price, mileage, seating_capacity, engine_displacement)
            sort_order: Sort order (asc or desc), default is asc
            
        Returns:
            List of matching CarDetail with basic fields only
            
        Raises:
            InvalidFilterError: If filter value is invalid
            ValueError: If sort_by or sort_order is invalid
        """
        # Validate filter values (same as list_cars)
        if brand is not None:
            self._validate_filter_value("brand", brand, self.available_brands)
        
        if body_type is not None:
            self._validate_filter_value("body_type", body_type, self.available_body_types)
        
        if fuel_type is not None:
            self._validate_filter_value("fuel_type", fuel_type, self.available_fuel_types)
        
        if transmission is not None:
            self._validate_filter_value("transmission", transmission, self.available_transmissions)
        
        # Validate sort parameters
        valid_sort_fields = ["price", "mileage", "seating_capacity", "engine_displacement"]
        if sort_by is not None and sort_by not in valid_sort_fields:
            raise ValueError(f"Invalid sort_by: '{sort_by}'. Must be one of: {', '.join(valid_sort_fields)}")
        
        if sort_order not in ["asc", "desc"]:
            raise ValueError(f"Invalid sort_order: '{sort_order}'. Must be 'asc' or 'desc'")
        
        query_lower = query.lower()
        matching_cars = []
        
        # First try: Direct string search (case-insensitive)
        for car in self.cars.values():
            # Check if query appears in name, manufacturer, or model
            manufacturer_match = (car.basic_info.manufacturer and 
                                query_lower in car.basic_info.manufacturer.lower())
            
            if (query_lower in car.basic_info.name.lower() or
                manufacturer_match or
                query_lower in car.basic_info.model.lower()):
                
                # Also check filters
                if self._matches_filters(
                    car,
                    min_price=min_price,
                    max_price=max_price,
                    brand=brand,
                    body_type=body_type,
                    fuel_type=fuel_type,
                    mileage_more_than=mileage_more_than,
                    mileage_less_than=mileage_less_than,
                    seating_capacity=seating_capacity,
                    transmission=transmission,
                    engine_displacement_more_than=engine_displacement_more_than,
                    engine_displacement_less_than=engine_displacement_less_than,
                ):
                    matching_cars.append((car, 100))  # Perfect match score
        
        # If no direct matches, use fuzzy search
        if not matching_cars:
            for car in self.cars.values():
                # Calculate fuzzy match score on name
                score = fuzz.partial_ratio(query_lower, car.basic_info.name.lower())
                
                if score >= self.fuzzy_threshold:
                    # Check filters
                    if self._matches_filters(
                        car,
                        min_price=min_price,
                        max_price=max_price,
                        brand=brand,
                        body_type=body_type,
                        fuel_type=fuel_type,
                        mileage_more_than=mileage_more_than,
                        mileage_less_than=mileage_less_than,
                        seating_capacity=seating_capacity,
                        transmission=transmission,
                        engine_displacement_more_than=engine_displacement_more_than,
                        engine_displacement_less_than=engine_displacement_less_than,
                    ):
                        matching_cars.append((car, score))
        
        # Sort results
        if sort_by:
            # Sort by specified field, then by search score
            matching_cars.sort(
                key=lambda x: (
                    self._get_sort_value(x[0], sort_by)[0],  # has_value (False sorts first)
                    self._get_sort_value(x[0], sort_by)[1] if sort_order == "asc" else -self._get_sort_value(x[0], sort_by)[1],
                    -x[1]  # Then by search score (descending)
                )
            )
        else:
            # Default: Sort by score (descending) then by price (ascending)
            matching_cars.sort(key=lambda x: (-x[1], x[0].price.value))
        
        # Take top results
        results = [car for car, score in matching_cars[:limit]]
        
        # Return basic details only
        return [car.get_basic_only() for car in results]
