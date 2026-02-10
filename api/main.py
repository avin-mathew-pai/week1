from fastapi import FastAPI, Depends, status, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
from sqlalchemy.orm import Session
from sqlalchemy import text, func, select
from typing import Optional, Annotated
from database import get_db
from redis_config import redis_client
from time import perf_counter
from models import CleanTrip
import json
# from schemas import TripSummary

CACHE_TTL_SECONDS = 300

security = HTTPBasic()

def get_current_username(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = b"test1"
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = b"test2"
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

api = FastAPI(title="taxi data api", dependencies=[Depends(get_current_username)])

@api.get('/health')
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"ststus": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error" : str(e)}
    
@api.get('/trips/summary')
def get_summary(db: Session = Depends(get_db)):

    start = perf_counter()

    cache_key = "trips:summary"
    cached = redis_client.get(cache_key)

    if cached:
        print("Data availiable in redis!!")

        duration = perf_counter() - start
        print(f"\n\n\nTime taken for this request using redis : {duration}\n\n\n")

        return cached
    
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
    final_result = {
        "total_trip_count" : result.total_trip_count,
        "min_pickup_timestamp" : str(result.min_pickup_timestamp),
        "max_pickup_timestamp" : str(result.max_pickup_timestamp),
        "min_fare_amout" : result.min_fare_amount,
        "max_fare_amount" : result.max_fare_amount,
        "min_trip_distance" : result.min_trip_distance,
        "max_trip_distance" : result.max_trip_distance
    }

    redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(final_result))

    duration = perf_counter() - start
    print(f"\n\n\nTime taken for this request using postgres : {duration}\n\n\n")

    return final_result

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

    start = perf_counter()

    cache_key = "trips:aggregates"
    cached = redis_client.get(cache_key)

    if cached:
        print("\n\nData availiable in redis!!\n\n")

        duration = perf_counter() - start

        print(f"Time taken for redis = {duration}")
        return cached

    p_hour = db.execute(text(
        """SELECT hour_of_day, COUNT("VendorID") as trips_per_hour FROM clean_trips GROUP BY hour_of_day ORDER BY hour_of_day ASC;"""
    ))

    p_day = db.execute(text(
        """SELECT day_of_week, COUNT("VendorID") as trips_per_day_of_week FROM clean_trips GROUP BY  day_of_week;"""
    ))

    avg_fare = db.execute(text(
        """SELECT passenger_count, AVG(total_amount) AS average_fare FROM clean_trips GROUP BY passenger_count;"""
    ))

    final_result = {
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

    redis_client.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(final_result))

    duration = perf_counter() - start
    print(f"Time taken for postgres = {duration}")

    return final_result

#xtra del
@api.delete("/redis-key-del")
def del_key(key_names: str):
    resp = ""
    # all_keys = redis_client.keys()
    sent_keys = key_names.split(",")
    for cache_key in sent_keys:
        vall = redis_client.getdel(name=cache_key)
        if vall:
            print("Found!")
            resp +=  f"key : {cache_key} deleted successfully!!\n value in key >>>>>>>>>>>  {vall} "
        else:
            print("Not found")
            resp += f">>>>>>>>>>  key : {cache_key} does not exist!!  "
    resp += f">>>>>>>>>>>>>> remaining keys {redis_client.keys()}"
    return resp
            

    # return keys   

        
        # vall = redis_client.getdel(name=cache_key)
        # return f"Key deleted successfully !! \n >> {vall}"
    

    # if cached:
    #     print("Daat avail in cache!")
    #     return cached
    # stmt = select(func.max(CleanTrip.total_amount).label("max_fare_amount"))
    # result = db.execute(stmt).scalar()
    # result_formatted = {
    #     "max_fare_amount" : result
    # }
    # # print(f"\n\n\n\n\n\n\n\n{result_formatted}\n\n\n\n\n\n\n\n")
    # redis_client.setex(name=cache_key, time= 5, value=json.dumps(result_formatted))
    # return result_formatted


    # stmt = (
    #         select
    #         (
    #                 CleanTrip.hour_of_day,
    #                 func.count(CleanTrip.VendorID).label("trips_per_hour")
    #         ).group_by(CleanTrip.hour_of_day)
    #     )




