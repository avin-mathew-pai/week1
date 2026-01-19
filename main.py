import pandas as pd, pyarrow, sqlalchemy, psycopg, matplotlib, os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

file_path = "/mnt/c/Datasetw1/yellow_tripdata_2023-01.parquet"

df = pd.read_parquet(file_path, engine='pyarrow')
column_names = df.columns
# print(column_names)

schema = pyarrow.Schema.from_pandas(df)
print(schema)

dtype_mappings = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "SMALLINT",
    "int8": "SMALLINT",

    "double": "DOUBLE PRECISION",
    "float": "REAL",

    "boolean": "BOOLEAN",

    "string": "TEXT",
    "utf8": "TEXT",

    "binary": "BYTEA",

    "timestamp[us]": "TIMESTAMP",
    "timestamp[ms]": "TIMESTAMP",
    "timestamp[s]": "TIMESTAMP",

    "date32": "DATE",
    "date64": "DATE",

    "decimal": "NUMERIC",      

    "list": "JSONB",   
    "struct": "JSONB"         
}

# for field in schema:
#     print(f"\n##{field.name}    ##{field.type}")

# db_conn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# try:
#     with psycopg.connect(db_conn, connect_timeout=5) as conn:
#         with conn.cursor() as cur:
#             cur.execute("SELECT version();")
#             version=cur.fetchone()[0]
#             print(f"Current version is  : {version}")

#             cur.execute("SELECT 1 + 1;")
#             print(f"Quick test results for (1+1) = {cur.fetchone()[0]}")

# except Exception as e:
#     print(f"Connection failed !!\n\n{repr(e)}")


