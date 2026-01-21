from dotenv import load_dotenv
import os, polars as pl
from loader import Loader
from cleaner import Cleaner
import sys

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_HOST_DOCKER = os.getenv("DB_HOST_DOCKER")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# commented for docker
# file_path = input("Enter file path : ")
# file_path.strip()

# # copy this filepath
# file_path = "/mnt/c/Datasetw1/yellow_tripdata_2023-01.parquet"  

# for docker
file_path_docker = "/app_week1/data/yellow_tripdata_2023-01.parquet"


# file_path_docker = input("Enter file path : ")
# file_path_docker.strip() 

# print("Enter file path: ")
# # .strip() is important because readline() keeps the '\n' at the end
# file_path_docker = sys.stdin.readline().strip()

# if not file_path_docker:
#     print("Error: No path provided via stdin.")
#     sys.exit(1)

# print(f"Path received: {file_path_docker}")



#reading using polars
df = pl.scan_parquet(file_path_docker)

# URI_LOCAL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#use uri for docker and db host for docker during containerization
URI_DOCKER = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST_DOCKER}:{DB_PORT}/{DB_NAME}"

table_loader = Loader()
table_cleaner = Cleaner()

# raw_table_name = input("Enter table name for raw data : ")
raw_table_name = "raw_trips"

print("Inputting raw table to postgres!!!")

# table_loader.load_table(df=df, table_name=raw_table_name, URI=URI)

# for docker
table_loader.load_table(df=df, table_name=raw_table_name, URI=URI_DOCKER)


print("\nCleaning data now..........\n")

cleaned_df = table_cleaner.clean_data(df)

print("\nInputting clean table to postgres !!!")

# raw_table_name = input("Enter table name for clean data : ")
clean_table_name = "clean_trips"

# table_loader.load_table(df=cleaned_df, table_name=clean_table_name, URI=URI)

#for docker
table_loader.load_table(df=cleaned_df, table_name=clean_table_name, URI=URI_DOCKER)




print("\n\n\n\n\n\n\nPipeline finished successfully!!!!!!")