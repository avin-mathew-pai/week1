from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
import polars as pl
from loader import Loader
from cleaner import Cleaner

ORIGINAL_FILE_PATH = "/app_week1/data/yellow_tripdata_2023-01.parquet"
TEMP_FILE = "/app_week1/data/temp_clean.parquet"

@dag(
        schedule='@daily',
        start_date=datetime(2026,1,1)
)
def nyc_taxi_ingestion_pipeline():

    @task()
    def create_insert_raw_table():
        print("Task 1")

        table_loader = Loader()

        # #reading using polars
        df = pl.scan_parquet(ORIGINAL_FILE_PATH)

        raw_table_name = "raw_trips"
        conn_id = BaseHook.get_connection("taxi_db")

        db_uri = conn_id.get_uri()

        if db_uri.startswith("postgres://"):
            db_uri = db_uri.replace("postgres://", "postgresql://", 1)

        # for docker
        table_loader.load_table(df=df, table_name=raw_table_name, URI=db_uri)


    @task()
    def process_raw_data():
        print("Task 2")

        table_cleaner = Cleaner()
        
        df = pl.scan_parquet(ORIGINAL_FILE_PATH)

        print("\nCleaning data now..........\n")

        cleaned_df = table_cleaner.clean_data(df)

        print("\nInputting clean table to postgres !!!")
        print(f"\nWriting to temp file {TEMP_FILE}......")

        cleaned_df.collect().write_parquet(TEMP_FILE)

        # return TEMP_FILE

    @task()
    def create_insert_clean_table():
        print("Task 3")

        table_loader = Loader()

        # #reading using polars
        df = pl.scan_parquet(TEMP_FILE)

        clean_table_name = "clean_trips"
        conn_id = BaseHook.get_connection("taxi_db")

        db_uri = conn_id.get_uri()

        if db_uri.startswith("postgres://"):
            db_uri = db_uri.replace("postgres://", "postgresql://", 1)

        # for docker
        table_loader.load_table(df=df, table_name=clean_table_name, URI=db_uri)
    
    create_insert_raw_table() >> process_raw_data() >> create_insert_clean_table()

nyc_taxi_ingestion_pipeline()
















# load_dotenv()

# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")
# DB_HOST = os.getenv("DB_HOST")
# DB_HOST_DOCKER = os.getenv("DB_HOST_DOCKER")
# DB_PORT = os.getenv("DB_PORT")
# DB_NAME = os.getenv("DB_NAME")

# # for docker
# file_path_docker = "/app_week1/data/yellow_tripdata_2023-01.parquet"

# #reading using polars
# df = pl.scan_parquet(file_path_docker)

# # URI_LOCAL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# #use uri for docker and db host for docker during containerization
# URI_DOCKER = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST_DOCKER}:{DB_PORT}/{DB_NAME}"

# table_loader = Loader()
# table_cleaner = Cleaner()

# # raw_table_name = input("Enter table name for raw data : ")
# raw_table_name = "raw_trips"

# print("Inputting raw table to postgres!!!")

# # table_loader.load_table(df=df, table_name=raw_table_name, URI=URI)

# # for docker
# table_loader.load_table(df=df, table_name=raw_table_name, URI=URI_DOCKER)


# print("\nCleaning data now..........\n")

# cleaned_df = table_cleaner.clean_data(df)

# print("\nInputting clean table to postgres !!!")

# # raw_table_name = input("Enter table name for clean data : ")
# clean_table_name = "clean_trips"

# # table_loader.load_table(df=cleaned_df, table_name=clean_table_name, URI=URI)

# #for docker
# table_loader.load_table(df=cleaned_df, table_name=clean_table_name, URI=URI_DOCKER)



# print("testtesttesttese/n/n   DB_NAME =", DB_NAME)

# print("\n\n\n\n\n\n\nPipeline finished successfully!!!!!!")