import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkConf
from spark_loader import Loader
from spark_cleaner import Cleaner

file_path = "/opt/spark/data/yellow_tripdata_2023-01.parquet"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger("airflow.task")

conf = SparkConf() \
    .setAppName("TaxiSparkPipeline") \
    # .set("spark.jars", "/opt/spark/work-dir/postgresql-42.7.10.jar")

spark: SparkSession = (
    SparkSession.builder \
    # .config(conf=conf) \
    .getOrCreate()
)

# spark.sparkContext.setLogLevel("WARN")

loader = Loader(spark)
cleaner = Cleaner()

raw_df = spark.read.parquet(file_path)

loader.load_table(raw_df, "spark_raw_trips_kubeeeee", logger, datetime)

clean_df = cleaner.clean_data(raw_df, logger, datetime)

loader.load_table(clean_df, "spark_clean_trips", logger, datetime)

spark.stop()


# spark-submit \
#     --driver-memory 4g \
#     --jars /opt/spark/work-dir/postgresql-42.7.10.jar \
#     spark_test.py


































# # jdbc_url = "jdbc:postgresql://taxi_db:5432/your_database_name"
# jdbc_url = "jdbc:postgresql://taxi_db:5432/mydatabase"
# connection_properties = {
#     "user": "avin",
#     "password": "avin",
#     "driver": "org.postgresql.Driver",
#     "batchsize": "10000"
# }

# try:
#     df.repartition(4).write.jdbc(
#         url=jdbc_url,
#         table="spark_raw_trips",
#         mode="overwrite",
#         properties=connection_properties
#     )
# except Exception as e:
#     print(f"Operation failed!\n{str(e)}")


# try:
#     print("Attempting to connect to Postgres...")
#     df = spark.read.jdbc(
#         url=jdbc_url, 
#         table="raw_trips", 
#         properties=connection_properties
#     )
#     df.show(10)
#     print("Connection Successful!")
# except Exception as e:
#     print(f"Connection Failed: {e}")

# names = df.columns
# print(f"\n\n\n\nCOUNT ->>>>>>>>>{df.count()}\n\n\n\n")
# print(f"\n\n\n\nColumn names ->>>>>>>>>{names}\n\n\n\n")

