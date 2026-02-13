from pyspark.sql import functions as F

class Cleaner():

    def clean_data(self, df):
        # CLEANING steps

        # 1. Adding an is_valid column (to polars df not to postgres table)

        df = df.withColumn("is_valid",
            F.when(

                (F.col('trip_distance') > 0) &
                (F.col('fare_amount') >= 0) &
                (F.col('passenger_count') >= 0),
                True
            )
            # to take care of null , converted into false
            .otherwise(False)
        )


        # 2. creating trip_duration_min column if is_valid is true
        duration_seconds = (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long") - F.col("tpep_pickup_datetime").cast("timestamp").cast("long"))
        duration_minutes = duration_seconds/60
        df = (df
            .withColumn("trip_duration_min", F.when(F.col('is_valid') == True, duration_minutes).otherwise(None))

            #maybe do this even if it is not valid because both fields are correct even if error happened (optional) or wrap it in is_valid == True
            .withColumn('hour_of_day', F.hour(F.col('tpep_pickup_datetime')))

            .withColumn('day_of_week', F.dayofweek(F.col('tpep_pickup_datetime')))
        )

        #3. dropping all columns with -ve trip duration
        df = df.filter(F.col('trip_duration_min') >= 0)

        #4. add speed field
        speed = (F.col('trip_distance') / (F.col('trip_duration_min') / 60))
        df = df.withColumn("trip_speed_mph",
            F.when(F.col('is_valid') == True, speed)
            .otherwise(None)
        )

        # print(df)

        #5. if dropping all non valid rows use this
        df = df.filter(F.col('is_valid'))

        return df