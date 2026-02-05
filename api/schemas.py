from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class TripSummary(BaseModel):
    total_trips: int
    min_timestamp: datetime
    max_timestamp: datetime
    min_fare: float
    max_fare: float
    min_trip_distance: float
    max_trip_distance: float


class Trip(BaseModel):
    # We use exact column names so FastAPI maps them automatically
    VendorID: Optional[int] = None
    tpep_pickup_datetime: Optional[datetime] = None
    tpep_dropoff_datetime: Optional[datetime] = None
    
    passenger_count: Optional[float] = None
    trip_distance: Optional[float] = None
    
    # DB says 'double precision', so we must use float (even if logic says ID is int)
    RatecodeID: Optional[float] = None 
    
    store_and_fwd_flag: Optional[str] = None
    PULocationID: Optional[int] = None
    DOLocationID: Optional[int] = None
    payment_type: Optional[int] = None
    
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None
    
    is_valid: Optional[bool] = None
    trip_duration_min: Optional[float] = None
    hour_of_day: Optional[int] = None
    day_of_week: Optional[int] = None
    trip_speed_mph: Optional[float] = None

    class Config:
        # This allows Pydantic to read data from SQLAlchemy/Polars objects
        from_attributes = True

