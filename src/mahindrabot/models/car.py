"""Pydantic models for car data structures."""

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class ReviewVideo(BaseModel):
    """Review video information."""
    title: str
    url: str
    youtube_id: Optional[str] = None
    thumbnail_url: Optional[str] = None


class ImageReference(BaseModel):
    """Reference to an image with unique identifier."""
    url: str
    url_id: str
    alt_text: str


class DimensionValue(BaseModel):
    """Dimension with value and unit."""
    value: float
    unit: str


class DisplacementValue(BaseModel):
    """Engine displacement with value and unit."""
    value: int
    unit: str


class PowerTorqueValue(BaseModel):
    """Power or torque specification."""
    value: float
    unit: str
    rpm: Optional[str] = None


class BasicInfo(BaseModel):
    """Basic car information."""
    name: str
    manufacturer: Optional[str] = None
    model: str
    body_type: Optional[str] = None
    url: Optional[str] = None
    image_url: Optional[ImageReference] = None
    description: Optional[str] = None
    model_date: Optional[str] = None
    sku: Optional[str] = None
    vin: Optional[str] = None
    condition: Optional[str] = None


class Engine(BaseModel):
    """Engine specifications."""
    displacement: Optional[list[DisplacementValue]] = None
    power: Optional[list[PowerTorqueValue]] = None
    torque: Optional[list[PowerTorqueValue]] = None
    fuel_type: Optional[list[str]] = None


class Fuel(BaseModel):
    """Fuel type and efficiency."""
    type: list[str]
    efficiency: Optional[dict[str, Any]] = None


class Dimensions(BaseModel):
    """Physical dimensions of the car."""
    width: Optional[DimensionValue] = None
    height: Optional[DimensionValue] = None
    weight: Optional[dict[str, int]] = None
    seating_capacity: int
    number_of_doors: Optional[int] = None


class Price(BaseModel):
    """Price information."""
    value: int
    currency: str
    availability: Optional[str] = None
    valid_until: Optional[str] = None
    url: Optional[str] = None


class Brand(BaseModel):
    """Brand information."""
    name: str
    image: Optional[ImageReference] = None


class Rating(BaseModel):
    """Expert rating."""
    value: Optional[float] = None
    worst: Optional[int] = None
    best: Optional[int] = None


class ReviewedBy(BaseModel):
    """Reviewer information."""
    name: str
    job_title: Optional[str] = None
    url: Optional[str] = None


class MileageDetail(BaseModel):
    """Detailed mileage information for specific configuration."""
    fuel_type: str
    transmission: str
    mileage: str
    city_mileage: Optional[str] = None
    highway_mileage: Optional[str] = None


class CompetitorCar(BaseModel):
    """Competitor car information."""
    name: str
    price: str
    url: str


class ComparisonFeature(BaseModel):
    """Feature comparison across cars."""
    feature: str
    values: list[str]


class CompetitorComparison(BaseModel):
    """Comparison with competitor cars."""
    cars: list[CompetitorCar]
    features: list[ComparisonFeature]


class CarDetail(BaseModel):
    """Complete car detail model with all information."""
    
    # Required fields
    id: str = Field(..., description="Unique car identifier (lowercase with underscores)")
    basic_info: BasicInfo
    price: Price
    brand: Brand
    
    # Optional fields (extended details)
    engine: Optional[Engine] = None
    transmission: Optional[list[str]] = None
    fuel: Optional[Fuel] = None
    dimensions: Optional[Dimensions] = None
    colors: Optional[list[str]] = None
    rating: Optional[Rating] = None
    reviewed_by: Optional[ReviewedBy] = None
    pros: Optional[list[str]] = None
    cons: Optional[list[str]] = None
    verdict: Optional[str] = None
    competitor_comparison: Optional[CompetitorComparison] = None
    mileage_details: Optional[list[MileageDetail]] = None
    whats_new: Optional[dict[str, list[str]]] = None
    review_videos: Optional[list[ReviewVideo]] = None
    
    model_config = ConfigDict(validate_assignment=True, extra="ignore")
    
    def get_basic_only(self) -> "CarDetail":
        """
        Return a copy with only basic fields populated.
        
        Returns:
            CarDetail with extended fields set to None
        """
        return CarDetail(
            id=self.id,
            basic_info=self.basic_info,
            price=self.price,
            brand=self.brand,
            # All optional fields are None by default
        )


class CarComparison(BaseModel):
    """Comparison matrix between different cars."""
    
    cars: list[CarDetail] = Field(..., description="List of cars being compared")
    comparison_matrix: dict[str, list[Any]] = Field(
        ..., 
        description="Matrix of comparison features and values"
    )
    
    model_config = ConfigDict(validate_assignment=True)
