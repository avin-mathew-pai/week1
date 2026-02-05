# from sqlalchemy.ext.automap import automap_base
# from database import engine

# Base = automap_base()

# Base.prepare(autoload_with=engine)

# CleanTrip = Base.classes.clean_trips


from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class CleanTrip(Base):
    __tablename__ = "clean_trips"

    VendorID = Column(Integer, primary_key=True)

    tpep_pickup_datetime = Column(DateTime)
    tpep_dropoff_datetime = Column(DateTime)

    passenger_count = Column(Float)
    trip_distance = Column(Float)

    RatecodeID = Column(Float)  

    store_and_fwd_flag = Column(String)
    PULocationID = Column(Integer)
    DOLocationID = Column(Integer)
    payment_type = Column(Integer)

    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    congestion_surcharge = Column(Float)
    airport_fee = Column(Float)

    is_valid = Column(Boolean)
    trip_duration_min = Column(Float)
    hour_of_day = Column(Integer)
    day_of_week = Column(Integer)
    trip_speed_mph = Column(Float)

    # # --- Helper Method ---
    # def to_dict(self):
    #     """Converts this ORM object into a dictionary for easy debugging."""
    #     return {c.name: getattr(self, c.name) for c in self.__table__.columns}

