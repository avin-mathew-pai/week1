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
# print(schema)

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

# print(dtype_mappings["struct"])




create_query = "CREATE TABLE IF NOT EXISTS raw_trips ("

for i in range(len(schema)):
    create_query += f"\n\t{schema[i].name} {dtype_mappings[schema[i].type]},"

create_query = create_query[:-1]

create_query += "\n);"

# print(create_query)


# for field in schema:
#     print(f"\n##{field.name}    ##{field.type}")

check_table_query = """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                        AND table_schema NOT IN ('pg_catalog', 'information_schema');
                    """


db_conn = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    with psycopg.connect(db_conn, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(create_query)

            cur.execute(check_table_query)
            b=cur.fetchall()

            print(f"{b}")

except Exception as e:
    print(f"Connection failed !!\n\n{repr(e)}")


