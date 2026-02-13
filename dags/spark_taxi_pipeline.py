from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from datetime import datetime, timedelta
import logging
import os
from taxi_utils.spark_loader import Loader
from taxi_utils.spark_cleaner import Cleaner

TEMP_FILE = "/app_week1/data/temp_clean.parquet"

logger = logging.getLogger("airflow.task")

@dag(
        schedule='@daily',
        start_date=datetime(2026,2,1),
        catchup=False,
        default_args={
            "retries":2,
            "retry_delay": timedelta(seconds=5)
        },
        params={
            "filename": Param("yellow_tripdata_2023-01.parquet", type="string")
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

                # #reading using spark
                ORIGINAL_FILE_PATH=f"/app_week1/data/{context['params']['filename']}"

                # LOG
                logger.info(f"create_insert_raw_table , END time = {datetime.now()}\n Number of rows processed : {no_of_rows}\nSUCCESS")
            except Exception as e:
                logger.error(f"Task 1 FAILED\n{str(e)}")
                raise e
        return ORIGINAL_FILE_PATH



    @task()
    def process_raw_data(ORIGINAL_FILE_PATH):
        
        print("Task 2")

        #LOG
        logger.info(f"\nprocess_raw_data , START time = {datetime.now()}")

        try:
            table_cleaner = Cleaner()
            
            # ORIGINAL_FILE_PATH=f"/app_week1/data/{context['params']['filename']}"

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

    process_raw_data(create_insert_raw_table()) >> create_insert_clean_table() >> cleanup_temp_files()

nyc_taxi_ingestion_pipeline()
















