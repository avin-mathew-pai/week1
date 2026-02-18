from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

FILE_PATH = "/opt/spark/data/yellow_tripdata_2023-01.parquet"
TEMP_FILE = "/opt/spark/data/<filename.parquet>"

@dag(
        schedule='@daily',
        start_date=datetime(2026,2,1),
        catchup=False,
        default_args={
            "retries":2,
            "retry_delay": timedelta(seconds=5)
        },
        # params={
        #     "filename": Param("yellow_tripdata_2023-01.parquet", type="string")
        # }
)
def spark_taxi_pipeline():

# Task : 1
    taxi_spark_job = BashOperator(
        task_id="run_in_taxi-pyspark",
        bash_command="""
            docker exec -i taxi-pyspark \
            spark-submit \
            --master local[*] \
            --driver-memory 4g \
            --jars /opt/spark/work-dir/postgresql-42.7.10.jar \
            /opt/spark/work-dir/spark_main.py
        """
    )
    taxi_spark_job

spark_taxi_pipeline()
















