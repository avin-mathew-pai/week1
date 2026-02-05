from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import os
import polars as pl
from taxi_utils.loader import Loader
from taxi_utils.cleaner import Cleaner

ORIGINAL_FILE_PATH = "/app_week1/data/yellow_tripdata_2023-01.parquet"
TEMP_FILE = "/app_week1/data/temp_clean.parquet"

logger = logging.getLogger("airflow.task")

@dag(
        schedule='@daily',
        start_date=datetime(2026,2,1),
        catchup=False,
        default_args={
            "retries":2,
            "retry_delay": timedelta(seconds=5)
        }
)
def nyc_taxi_ingestion_pipeline():

    @task()
    def create_insert_raw_table(**context):
        print("Task 1")

        # simulated failure
        if context['ti'].try_number == 1:
            print("Failing the first try.")
            err_msg = "SIMULATED FAILURE !! Retry will be successful !!"
            logger.error(err_msg)
            raise ValueError(err_msg)
        
        else:

            #LOG
            logger.info(f"\nExecution date is : {context['ds']}\ncreate_insert_raw_table , START time = {datetime.now()}")

            try:
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

                no_of_rows = df.select(pl.len()).collect().item()
                # LOG
                logger.info(f"create_insert_raw_table , END time = {datetime.now()}\n Number of rows processed : {no_of_rows}\nSUCCESS")
            except Exception as e:
                logger.error(f"Task 1 FAILED\n{str(e)}")
                raise e



    @task()
    def process_raw_data():
        
        print("Task 2")

        #LOG
        logger.info(f"\nprocess_raw_data , START time = {datetime.now()}")

        try:
            table_cleaner = Cleaner()
            
            df = pl.scan_parquet(ORIGINAL_FILE_PATH)

            print("\nCleaning data now..........\n")

            cleaned_df = table_cleaner.clean_data(df)

            print("\nInputting clean table to postgres !!!")
            print(f"\nWriting to temp file {TEMP_FILE}......")

            cleaned_df.collect().write_parquet(TEMP_FILE)

            no_of_rows = cleaned_df.select(pl.len()).collect().item()
            # LOG
            logger.info(f"process_raw_data , END time = {datetime.now()}\n Number of rows processed : {no_of_rows}\nSUCCESS")
        except Exception as e:
            logger.error(f"Task 2 FAILED\n{str(e)}")
            raise e

        # return TEMP_FILE

    @task()
    def create_insert_clean_table():
        print("Task 3")

        #LOG
        logger.info(f"\ncreate_insert_clean_table , START time = {datetime.now()}")

        try:
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

            no_of_rows = df.select(pl.len()).collect().item()
            # LOG
            logger.info(f"create_insert_raw_table , END time = {datetime.now()}\n Number of rows processed : {no_of_rows}\nSUCCESS")
        except Exception as e:
            logger.error(f"Task 3 FAILED\n{str(e)}")
            raise e


    @task()
    def cleanup_temp_files():
        print("Cleaning up temp files!!")
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)
            print("File successfully deleted!!")
        else:
            print("File not found!!")

    create_insert_raw_table() >> process_raw_data() >> create_insert_clean_table() >> cleanup_temp_files()

nyc_taxi_ingestion_pipeline()
















