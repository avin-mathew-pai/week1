from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, func, select
from typing import Optional
from database import get_db
from models import CleanTrip
# from schemas import TripSummary

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

@api.get('/trips')
def get_trips_list(limit: int, offset: int, db: Session = Depends(get_db)):
    stmt = select(CleanTrip).limit(limit).offset(offset)
    result = db.execute(stmt).all()
    return result

@api.get('/trips/filter')
def get_filtered_trips(
    day_of_week: Optional[int] = None, 
    hour_of_day: Optional[int] = None, 
    payment_type: Optional[int] = None, 
    min_fare: Optional[float] = None, 
    max_faredb: Optional[float] = None, 
    db: Session = Depends(get_db)
    ):
    stmt = select(CleanTrip).limit(10)

    if day_of_week is not None:
        stmt = stmt.where(CleanTrip.day_of_week == day_of_week)

    if hour_of_day is not None:
        stmt = stmt.where(CleanTrip.hour_of_day == hour_of_day)
    
    if payment_type is not None:
        stmt = stmt.where(CleanTrip.payment_type == payment_type)
    
    if min_fare is not None:
        stmt = stmt.where(CleanTrip.total_amount >= min_fare)
    
    if max_faredb is not None:
        stmt = stmt.where(CleanTrip.total_amount <= max_faredb)

    result = db.execute(stmt).all()
    return result

@api.get('/trips/aggregates')
def get_aggregates(db: Session = Depends(get_db)):

    p_hour = db.execute(text(
        """SELECT hour_of_day, COUNT("VendorID") as trips_per_hour FROM clean_trips GROUP BY hour_of_day ORDER BY hour_of_day ASC;"""
    ))

    p_day = db.execute(text(
        """SELECT day_of_week, COUNT("VendorID") as trips_per_day_of_week FROM clean_trips GROUP BY  day_of_week;"""
    ))

    avg_fare = db.execute(text(
        """SELECT passenger_count, AVG(total_amount) AS average_fare FROM clean_trips GROUP BY passenger_count;"""
    ))

    return {
        "p_hour": [
            {"hour_of_day": row.hour_of_day, "trips_per_hour": row.trips_per_hour}
            for row in p_hour
        ],
        "p_day": [
            {"hour_of_day": row.day_of_week, "trips_per_hour": row.trips_per_day_of_week}
            for row in p_day
        ],
        "avg_fare": [
            {"hour_of_day": row.passenger_count, "trips_per_hour": row.average_fare}
            for row in avg_fare
        ]
    }

    # stmt = (
    #         select
    #         (
    #                 CleanTrip.hour_of_day,
    #                 func.count(CleanTrip.VendorID).label("trips_per_hour")
    #         ).group_by(CleanTrip.hour_of_day)
    #     )




