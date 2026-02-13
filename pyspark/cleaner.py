import polars as pl

class Cleaner():
    def clean_data(self, df):
        # CLEANING steps

        # 1. Adding an is_valid column (to polars df not to postgres table)

        df = df.with_columns([
            (
                (pl.col('trip_distance') > 0)&
                (pl.col('fare_amount') >= 0)&
                (pl.col('passenger_count') >= 0)
            )
            # to take care of null , converted into false
            .fill_null(False)  
            .alias('is_valid')
        ])


        # 2. creating trip_duration_min column if is_valid is true
        df = df.with_columns([
            pl.when(pl.col('is_valid') == True)
            .then(
                (pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime'))
                .dt.total_seconds() / 60
            )
            .alias('trip_duration_min'),

            #maybe do this even if it is not valid because both fields are correct even if error happened (optional) or wrap it in is_valid == True
            pl.col('tpep_pickup_datetime').dt.hour().alias('hour_of_day'),

            pl.col('tpep_pickup_datetime').dt.weekday().alias('day_of_week'),

        ])

        #3. dropping all columns with -ve trip duration
        df = df.filter(pl.col('trip_duration_min') >= 0)

        #4. add speed field

        df = df.with_columns([
            pl.when(pl.col('is_valid') == True)
            .then(
                (pl.col('trip_distance') / (pl.col('trip_duration_min') / 60))
            )
            .alias('trip_speed_mph')
        ])

        # print(df)

        #5. if dropping all non valid rows use this
        df = df.filter(pl.col('is_valid'))

        return df