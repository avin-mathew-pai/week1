from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text, func, select
from typing import List
from database import get_db
from models import CleanTrip
from schemas import TripSummary

api = FastAPI(title="taxi data api")

@api.get('/health')
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"ststus": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error" : str(e)}
    
@api.get('/trips/summary')
def get_summary(db: Session = Depends(get_db)):
    
    stmt = select(
        func.count(CleanTrip.VendorID).label("total_trip_count"),
        func.min(CleanTrip.tpep_pickup_datetime).label("min_pickup_timestamp"),
        func.max(CleanTrip.tpep_pickup_datetime).label("max_pickup_timestamp"),
        func.min(CleanTrip.total_amount).label("min_fare_amount"),
        func.max(CleanTrip.total_amount).label("max_fare_amount"),
        func.min(CleanTrip.trip_distance).label("min_trip_distance"),
        func.max(CleanTrip.trip_distance).label("max_trip_distance")
    )

    result = db.execute(stmt).one()
    return {
        "total_trip_count" : {result.total_trip_count},
        "min_pickup_timestamp" : {result.min_pickup_timestamp},
        "max_pickup_timestamp" : {result.max_pickup_timestamp},
        "min_fare_amout" : {result.min_fare_amount},
        "max_fare_amount" : {result.max_fare_amount},
        "min_trip_distance" : {result.min_trip_distance},
        "max_trip_distance" : {result.max_trip_distance}
    }
