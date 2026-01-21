import pandas as pd, pyarrow, psycopg2, matplotlib, os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import polars as pl

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

file_path = "/mnt/c/Datasetw1/yellow_tripdata_2023-01.parquet"

# df = pd.read_parquet(file_path, engine='pyarrow')
# print(len(df))

#reading using polars
df = pl.scan_parquet(file_path)

# # for testing with sample data
# df = pl.scan_parquet(file_path).collect().sample(n=150)

URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --------------------------------------------------------------------------------

# parquet ---> postgres (just writing raw data as it is)
try:
    df.collect().write_database(
            table_name = "raw_trips",
            connection = URI,
            #change according to need ('replace' to show working)
            if_table_exists = "fail",
            engine = "adbc"
        )
except Exception as e:
    print(f"ERROR \n {repr(e)}")

# ---------------------------------------------------------------------

# # printing from postgres(using polars)(just checking output)
# df_head = pl.read_database_uri(
#     query = "SELECT * FROM raw_trips LIMIT 10;",
#     uri = URI,
#     engine='adbc'
# )
# print(f"\n\nData from POSTGRES \n\n {df_head.schema}")

# #checking total number of rows
# df_len = pl.read_database_uri(
#     query = "SELECT COUNT(*) FROM raw_trips;",
#     uri = URI,
#     engine='adbc'
# )
# print(f"Total rows = {df_len}")

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
# df = df.with_columns([
#     pl.when(pl.col('is_valid') == True)
#     .then(
#         (pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime'))
#         .dt.total_seconds() / 60
#     )
#     .alias('trip_duration_min'),

#     #maybe do this even if it is not valid because both fields are correct even if error happened (optional) or wrap it in is_valid == True
#     pl.col('tpep_pickup_datetime').dt.hour().alias('hour_of_day'),

#     pl.col('tpep_pickup_datetime').dt.weekday().alias('day_of_week'),

# ])

#3. add speed field

# df = df.with_columns([
#     pl.when(pl.col('is_valid') == True)
#     .then(
#         (pl.col('trip_distance') / (pl.col('trip_duration_min') / 60))
#     )
#     .alias('trip_speed_mph')
# ])

# print(df)

#4. if dropping all non valid rows use this
# df = df.filter(pl.col('is_valid'))

# cleaned df ---> postgres
try:
    df.collect().write_database(
            table_name = "clean_trips",
            connection = URI,
            #change according to need ('replace' to show working)
            if_table_exists = "fail",
            engine = "adbc"
        )
except Exception as e:
    print(f"ERROR \n {repr(e)}")



# # printing from postgres(using polars)(just checking output)
# df_head = pl.read_database_uri(
#     query = "SELECT * FROM clean_trips ORDER BY RANDOM() LIMIT 10;",
#     uri = URI,
#     engine='adbc'
# )
# print(f"\n\nClean Data from POSTGRES \n\n{df_head}\n\n\n\n\n {df_head.schema}")


# queries for checking
df_query = pl.read_database_uri(
    query = "SELECT COUNT(*) FROM clean_trips;",
    uri = URI,
    engine='adbc'
)
print(df_query)

df_query = pl.read_database_uri(
    query = "SELECT COUNT(*) FROM clean_trips WHERE fare_amount < 0;",
    uri = URI,
    engine='adbc'
)
print(df_query)

df_query = pl.read_database_uri(
    query = "SELECT COUNT(*) FROM clean_trips WHERE trip_distance <= 0;",
    uri = URI,
    engine='adbc'
)
print(df_query)

df_query = pl.read_database_uri(
    query = "SELECT MIN(trip_duration_min), MAX(trip_duration_min), AVG(trip_duration_min) FROM clean_trips;",
    uri = URI,
    engine='adbc'
)
print(df_query)

df_query = pl.read_database_uri(
    query = "SELECT day_of_week, COUNT(*) FROM clean_trips GROUP BY day_of_week ORDER BY day_of_week;",
    uri = URI,
    engine='adbc'
)
print(df_query)







































# # PANDAS + SQL alchemy approach (MEMORY ISSUES FOR 3 MILLION ROWS)
# df = pd.read_parquet(file_path, engine='pyarrow')
# print(f"Data from parquet file \n\n {df.head()}")

# # column_names = df.columns
# # print(column_names)
# # schema = pyarrow.Schema.from_pandas(df)

# engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


# # parquet ---> postgres
# with engine.connect() as conn:
#     df.to_sql(
#         name = 'raw_trips',
#         con = conn,
#         if_exists = 'replace',
#         index = False
#     )

#     df2 = pd.read_sql_table('raw_trips', conn)
#     print(f"\n\nData from POSTGRES \n\n {df2.head()}")





# # MANUAL approach(outdated)

# schema = pyarrow.Schema.from_pandas(df)

# dtype_mappings = {
#     "int64": "BIGINT",
#     "int32": "INTEGER",
#     "int16": "SMALLINT",
#     "int8": "SMALLINT",

#     "double": "DOUBLE PRECISION",
#     "float": "REAL",

#     "boolean": "BOOLEAN",

#     "string": "TEXT",
#     "utf8": "TEXT",

#     "binary": "BYTEA",

#     "timestamp[us]": "TIMESTAMP",
#     "timestamp[ms]": "TIMESTAMP",
#     "timestamp[s]": "TIMESTAMP",

#     "date32": "DATE",
#     "date64": "DATE",

#     "decimal": "NUMERIC",      

#     "list": "JSONB",   
#     "struct": "JSONB"         
# }


# create_query = "CREATE TABLE IF NOT EXISTS raw_trips ("

# for i in range(len(schema)):
#     create_query += f"\n\t{schema[i].name} {dtype_mappings[schema[i].type]},"

# create_query = create_query[:-1]

# create_query += "\n);"

# check_table_query = """
#                         SELECT table_name
#                         FROM information_schema.tables
#                         WHERE table_type = 'BASE TABLE'
#                         AND table_schema NOT IN ('pg_catalog', 'information_schema');
#                     """


# db_conn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# try:
#     with psycopg.connect(db_conn, connect_timeout=5) as conn:
#         with conn.cursor() as cur:
#             cur.execute(create_query)

#             cur.execute(check_table_query)
#             b=cur.fetchall()

#             print(f"{b}")

# except Exception as e:
#     print(f"Connection failed !!\n\n{repr(e)}")


