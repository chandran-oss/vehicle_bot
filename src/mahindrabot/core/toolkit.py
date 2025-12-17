"""AgentToolKit that wraps services and provides tools for the agent."""

import json
import os
import random
from typing import Optional

from mahindrabot.services.car_service import (
    CarNotFoundError,
    CarService,
    InvalidFilterError,
)
from mahindrabot.services.bike_service import (
    BikeNotFoundError,
    BikeService,
    InvalidBikeFilterError,
)
from mahindrabot.services.ev_charger_service import EVChargerLocationService
from mahindrabot.services.faq_service import FAQService
from mahindrabot.services.llm_service import ToolKit
from mahindrabot.services.serializers import (
    serialize_car_comparison,
    serialize_car_detail,
    serialize_bike_comparison,
    serialize_bike_detail,
    serialize_multiple_ev_locations,
)
from mahindrabot.services.slack import send_message

from .state import StateManager


class AgentToolKit:
    """
    Toolkit that wraps car and FAQ services and provides tools for the agent.
    
    Registers 9 tools:
    - list_cars: List cars with filters and pagination
    - search_car: Search cars by query with filters
    - get_car_details: Get basic car details by car_id
    - get_extended_car_details: Get full car details by car_id
    - get_car_comparison: Compare multiple cars
    - search_faq: Search FAQ database (general_qna only)
    - book_ride: Initiate test drive booking with OTP
    - confirm_ride: Confirm booking with OTP verification
    - find_nearest_ev_charger: Find nearest EV charging station
    """
    
    def __init__(
        self, 
        car_service: CarService,
        bike_service: BikeService,
        faq_service: FAQService,
        ev_charger_service: EVChargerLocationService
    ):
        """
        Initialize the toolkit with services.
        
        Args:
            car_service: CarService instance for car operations
            bike_service: BikeService instance for bike operations
            faq_service: FAQService instance for FAQ search
            ev_charger_service: EVChargerLocationService instance for EV charger location
        """
        self.car_service = car_service
        self.bike_service = bike_service
        self.faq_service = faq_service
        self.ev_charger_service = ev_charger_service
        self.state_manager = StateManager()
        self.toolkit = ToolKit()
        self._register_tools()
    
    def _register_tools(self) -> None:
        """Register all tools with the toolkit."""
        self.toolkit.register(
            func=self.list_cars,
            name="list_cars",
            description="List cars with optional filters, sorting, and pagination. Supports sorting by price, mileage, seating_capacity, and engine_displacement in ascending or descending order. Returns a list of cars matching the criteria."
        )
        
        self.toolkit.register(
            func=self.search_car,
            name="search_car",
            description="Search for cars by query string with optional filters and sorting. Supports sorting by price, mileage, seating_capacity, and engine_displacement. Use when user mentions specific car names or features."
        )
        
        self.toolkit.register(
            func=self.get_car_details,
            name="get_car_details",
            description="Get basic car details by car ID. Returns essential information without extended details like specifications, features, pros/cons."
        )
        
        self.toolkit.register(
            func=self.get_extended_car_details,
            name="get_extended_car_details",
            description="Get complete car details by car ID including all specifications, features, pros/cons, and other extended information."
        )
        
        self.toolkit.register(
            func=self.get_car_comparison,
            name="get_car_comparison",
            description="Compare multiple cars by their IDs. Returns detailed comparison matrix with features side-by-side."
        )
        
        self.toolkit.register(
            func=self.search_faq,
            name="search_faq",
            description="Search FAQ database for insurance and general questions. Returns relevant Q&A pairs with similarity scores."
        )
        
        self.toolkit.register(
            func=self.book_ride,
            name="book_ride",
            description="Initiate test drive booking by collecting user details. Generates OTP and sends notification."
        )
        
        self.toolkit.register(
            func=self.confirm_ride,
            name="confirm_ride",
            description="Confirm test drive booking by verifying OTP. Completes the booking process."
        )
        
        self.toolkit.register(
            func=self.find_nearest_ev_charger,
            name="find_nearest_ev_charger",
            description="Find EV charging stations by pincode within a specified radius. Returns up to 'limit' stations sorted by distance, with location details and Google Maps links. Parameters: pincode (required), radius_in_km (default: 5.0), limit (default: 5)."
        )

        self.toolkit.register(
            func=self.list_bikes,
            name="list_bikes",
            description="List bikes, scooters, and motorcycles with optional filters, sorting, and pagination. Supports sorting by price, mileage, and engine_displacement. Returns a list of two-wheelers matching the criteria."
        )
        
        self.toolkit.register(
            func=self.search_bike,
            name="search_bike",
            description="Search for bikes, scooters, and motorcycles by query string with optional filters. Use when user mentions specific model names or features."
        )
        
        self.toolkit.register(
            func=self.get_bike_details,
            name="get_bike_details",
            description="Get basic bike details by bike ID."
        )
        
        self.toolkit.register(
            func=self.get_extended_bike_details,
            name="get_extended_bike_details",
            description="Get complete bike details by bike ID including specifications and reviews."
        )
        
        self.toolkit.register(
            func=self.get_bike_comparison,
            name="get_bike_comparison",
            description="Compare multiple bikes by their IDs. Returns detailed comparison matrix."
        )
    
    def get_tools(self) -> list:
        """
        Get all registered tools.
        
        Returns:
            List of Tool objects
        """
        return self.toolkit.get_tools()
    
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
    ) -> str:
        """
        List cars with optional filters and pagination.
        
        Args:
            limit: Maximum number of results
            offset: Number of results to skip
            min_price: Minimum price filter (INR)
            max_price: Maximum price filter (INR)
            brand: Brand name filter
            body_type: Body type filter (e.g., SUV, Sedan)
            fuel_type: Fuel type filter (e.g., Petrol, Diesel)
            mileage_more_than: Minimum mileage (exclusive)
            mileage_less_than: Maximum mileage (exclusive)
            seating_capacity: Seating capacity filter
            transmission: Transmission type filter
            engine_displacement_more_than: Minimum engine displacement (exclusive)
            engine_displacement_less_than: Maximum engine displacement (exclusive)
            sort_by: Field to sort by (price, mileage, seating_capacity, engine_displacement)
            sort_order: Sort order (asc or desc), default is asc
            
        Returns:
            Human-readable formatted string of car details
        """
        try:
            cars = self.car_service.list_cars(
                limit=limit,
                offset=offset,
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
                sort_by=sort_by,
                sort_order=sort_order,
            )
            
            # Serialize each car using the serializer
            serialized_cars = [serialize_car_detail(car) for car in cars]
            return "\n\n".join(serialized_cars)
            
        except InvalidFilterError as e:
            return f"Filter error: {str(e)}"
        except Exception as e:
            return f"Error listing cars: {str(e)}"
    
    def search_car(
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
    ) -> str:
        """
        Search for cars by query string with optional filters.
        
        Args:
            query: Search query string
            limit: Maximum number of results
            (filter args same as list_cars)
            sort_by: Field to sort by (price, mileage, seating_capacity, engine_displacement)
            sort_order: Sort order (asc or desc), default is asc
            
        Returns:
            Human-readable formatted string of car details
        """
        try:
            cars = self.car_service.search(
                query=query,
                limit=limit,
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
                sort_by=sort_by,
                sort_order=sort_order,
            )
            
            if not cars:
                return "No cars found matching your search criteria."
            
            # Serialize each car using the serializer
            serialized_cars = [serialize_car_detail(car) for car in cars]
            return "\n\n".join(serialized_cars)
            
        except InvalidFilterError as e:
            return f"Filter error: {str(e)}"
        except Exception as e:
            return f"Error searching cars: {str(e)}"
    
    def get_car_details(self, car_id: str) -> str:
        """
        Get basic car details by car ID.
        
        Args:
            car_id: Car identifier
            
        Returns:
            Human-readable formatted basic car details
        """
        try:
            car = self.car_service.get_car_details(car_id)
            
            # Serialize car using the serializer
            return serialize_car_detail(car)
            
        except CarNotFoundError as e:
            return f"Car not found: {str(e)}"
        except Exception as e:
            return f"Error getting car details: {str(e)}"
    
    def get_extended_car_details(self, car_id: str) -> str:
        """
        Get complete car details by car ID including all extended information.
        
        Args:
            car_id: Car identifier
            
        Returns:
            Human-readable formatted complete car details
        """
        try:
            car = self.car_service.get_extended_car_details(car_id)
            
            # Serialize car using the serializer
            return serialize_car_detail(car)
            
        except CarNotFoundError as e:
            return f"Car not found: {str(e)}"
        except Exception as e:
            return f"Error getting extended car details: {str(e)}"
    
    def get_car_comparison(self, car_ids: list[str]) -> str:
        """
        Compare multiple cars by their IDs.
        
        Args:
            car_ids: List of car identifiers to compare
            
        Returns:
            Human-readable formatted comparison table
        """
        try:
            comparison = self.car_service.get_car_comparison(car_ids)
            
            # Serialize comparison using the serializer
            return serialize_car_comparison(comparison)
            
        except CarNotFoundError as e:
            return f"Car not found: {str(e)}"
        except Exception as e:
            return f"Error comparing cars: {str(e)}"
    
    def search_faq(self, query: str, limit: int = 5) -> str:
        """
        Search FAQ database for relevant questions and answers.
        
        Special handling: If no relevant FAQs found, returns a helpful message
        directing user to contact support.
        
        Args:
            query: Search query string
            limit: Maximum number of results (default 5, hard limit 15)
            
        Returns:
            JSON string of list[QNAResult] or no-results message
        """
        try:
            # Enforce hard limit of 15
            limit = min(limit, 15)
            
            results = self.faq_service.search(query=query, limit=limit)
            
            # Check if no relevant results found (empty or low scores)
            if not results or (results and results[0].score < 0.5):
                return "I couldn't find any relevant information in our FAQ database. Please contact customer support for assistance."
            
            # Convert to dict for JSON serialization
            results_data = [result.model_dump() for result in results]
            return json.dumps(results_data, indent=2)
            
        except Exception as e:
            return f"Error searching FAQs: {str(e)}"
    
    def book_ride(self, name: str, phone_number: str) -> str:
        """
        Initiate test drive booking.
        
        Generates 6-digit OTP, stores in state, and sends Slack notification.
        
        Args:
            name: User's name
            phone_number: User's phone number
            
        Returns:
            Confirmation message (without revealing OTP)
        """
        try:
            # Generate 6-digit OTP
            otp = str(random.randint(100000, 999999))
            
            # Store OTP with user info
            self.state_manager.store_otp(phone_number, otp, name)
            
            # Send Slack notification with OTP
            slack_message = (
                f"🚗 Test Drive Booking Request\n"
                f"Name: {name}\n"
                f"Phone: {phone_number}\n"
                f"OTP: {otp}\n"
                f"Valid for: 10 minutes"
            )
            
            try:
                send_message(slack_message)
            except Exception as slack_error:
                # Log error but don't fail the booking
                print(f"Warning: Failed to send Slack notification: {slack_error}")
            
            return (
                f"Thank you, {name}! We've initiated your test drive booking. "
                f"An OTP has been sent to {phone_number}. "
                f"Please provide the OTP to confirm your booking."
            )
            
        except Exception as e:
            return f"Error booking ride: {str(e)}"
    
    def confirm_ride(self, otp: str) -> str:
        """
        Confirm test drive booking by verifying OTP.
        
        Args:
            otp: OTP code to verify
            
        Returns:
            Success or failure message
        """
        try:
            # Check if OTP matches the internal secret OTP from environment
            secret_otp = os.getenv("INTERNAL_SECRET_OTP")
            if secret_otp and otp == secret_otp:
                return (
                    "✅ Booking confirmed with internal OTP! "
                    "Our team will contact you shortly to schedule your test drive."
                )
            
            # We need to find the phone number from state
            # In a real implementation, we might need to track the current user's phone
            # For now, we'll iterate through state to find matching OTP
            
            for phone, data in self.state_manager._state.items():
                if data["otp"] == otp:
                    success, name = self.state_manager.verify_otp(phone, otp)
                    if success:
                        return (
                            f"✅ Booking confirmed! Thank you, {name}. "
                            f"Our team will contact you shortly at {phone} to schedule your test drive."
                        )
                    break
            
            return (
                "❌ Invalid or expired OTP. Please request a new booking or "
                "check the OTP and try again."
            )
            
        except Exception as e:
            return f"Error confirming ride: {str(e)}"
    
    def find_nearest_ev_charger(self, pincode: str, radius_in_km: float = 5.0, limit: int = 5) -> str:
        """
        Find EV charging stations by pincode within a specified radius.
        
        Args:
            pincode: Indian postal code to search from
            radius_in_km: Maximum search radius in kilometers (default: 5.0)
            limit: Maximum number of results to return (default: 5)
            
        Returns:
            Formatted string with location details or error message
        """
        try:
            user_location, results = self.ev_charger_service.find_nearest_ev_charger(
                pincode, radius_in_km, limit
            )
            
            # Invalid pincode
            if user_location is None and not results:
                return (
                    f"❌ Invalid pincode: {pincode}\n\n"
                    f"Please provide a valid Indian pincode to search for EV charging stations. "
                    f"Remember, EV charging station data is only available for New Delhi."
                )
            
            # Valid pincode but no charging stations found
            if not results:
                lines = []
                lines.append("=" * 80)
                lines.append("📍 SEARCH LOCATION")
                lines.append("=" * 80)
                lines.append(f"Pincode: {user_location['pincode']}")
                lines.append(f"Location: {user_location['place_name']}")
                lines.append(f"State: {user_location['state']}")
                lines.append(f"Coordinates: {user_location['latitude']:.4f}, {user_location['longitude']:.4f}")
                lines.append("")
                lines.append("❌ NO CHARGING STATIONS FOUND")
                lines.append("-" * 80)
                lines.append(f"No EV charging stations found within {radius_in_km} km of your location.")
                lines.append("")
                lines.append("💡 Suggestions:")
                lines.append(f"  • Try searching with a larger radius (e.g., {int(radius_in_km * 2)} km)")
                lines.append("  • Try a different New Delhi pincode")
                lines.append("  • Note: Data is only available for New Delhi locations")
                
                return "\n".join(lines)
            
            # Charging stations found
            # Add search location header
            lines = []
            lines.append("=" * 80)
            lines.append("📍 SEARCH LOCATION")
            lines.append("=" * 80)
            lines.append(f"Pincode: {user_location['pincode']}")
            lines.append(f"Location: {user_location['place_name']}")
            lines.append(f"State: {user_location['state']}")
            lines.append(f"Search Radius: {radius_in_km} km")
            lines.append("")
            
            # Add serialized results
            lines.append(serialize_multiple_ev_locations(results))
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"Error finding EV charger: {str(e)}"

    def list_bikes(
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
        engine_displacement_more_than: Optional[int] = None,
        engine_displacement_less_than: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> str:
        """List bikes with optional filters and pagination."""
        try:
            bikes = self.bike_service.list_bikes(
                limit=limit,
                offset=offset,
                min_price=min_price,
                max_price=max_price,
                brand=brand,
                body_type=body_type,
                fuel_type=fuel_type,
                mileage_more_than=mileage_more_than,
                mileage_less_than=mileage_less_than,
                engine_displacement_more_than=engine_displacement_more_than,
                engine_displacement_less_than=engine_displacement_less_than,
                sort_by=sort_by,
                sort_order=sort_order,
            )
            
            # Serialize each bike
            serialized_bikes = [serialize_bike_detail(bike) for bike in bikes]
            return "\n\n".join(serialized_bikes)
            
        except InvalidBikeFilterError as e:
            return f"Filter error: {str(e)}"
        except Exception as e:
            return f"Error listing bikes: {str(e)}"

    def search_bike(
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
        engine_displacement_more_than: Optional[int] = None,
        engine_displacement_less_than: Optional[int] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> str:
        """Search bikes by query string with optional filters."""
        try:
            bikes = self.bike_service.search(
                query=query,
                limit=limit,
                min_price=min_price,
                max_price=max_price,
                brand=brand,
                body_type=body_type,
                fuel_type=fuel_type,
                mileage_more_than=mileage_more_than,
                mileage_less_than=mileage_less_than,
                engine_displacement_more_than=engine_displacement_more_than,
                engine_displacement_less_than=engine_displacement_less_than,
                sort_by=sort_by,
                sort_order=sort_order,
            )
            
            if not bikes:
                return "No bikes found matching your search criteria."
            
            # Serialize each bike
            serialized_bikes = [serialize_bike_detail(bike) for bike in bikes]
            return "\n\n".join(serialized_bikes)
            
        except InvalidBikeFilterError as e:
            return f"Filter error: {str(e)}"
        except Exception as e:
            return f"Error searching bikes: {str(e)}"

    def get_bike_details(self, bike_id: str) -> str:
        """Get basic bike details by bike ID."""
        try:
            bike = self.bike_service.get_bike_details(bike_id)
            return serialize_bike_detail(bike)
        except BikeNotFoundError as e:
            return f"Bike not found: {str(e)}"
        except Exception as e:
            return f"Error getting bike details: {str(e)}"

    def get_extended_bike_details(self, bike_id: str) -> str:
        """Get complete bike details by bike ID."""
        try:
            bike = self.bike_service.get_extended_bike_details(bike_id)
            return serialize_bike_detail(bike)
        except BikeNotFoundError as e:
            return f"Bike not found: {str(e)}"
        except Exception as e:
            return f"Error getting extended bike details: {str(e)}"

    def get_bike_comparison(self, bike_ids: list[str]) -> str:
        """Compare multiple bikes by their IDs."""
        try:
            comparison = self.bike_service.get_bike_comparison(bike_ids)
            return serialize_bike_comparison(comparison)
        except BikeNotFoundError as e:
            return f"Bike not found: {str(e)}"
        except Exception as e:
            return f"Error comparing bikes: {str(e)}"
