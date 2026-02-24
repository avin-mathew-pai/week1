import polars as pl

class Loader():
    def load_table(self, df, table_name, URI):
        try:
            df.collect().write_database(
                    table_name = table_name,
                    connection = URI,
                    #change according to need ('replace' to show working)
                    if_table_exists = "replace",
                    engine = "adbc"
                )
        except Exception as e:
            print(f"ERROR table : {table_name} already exists !!! \n {repr(e)}")
        else: 
            print(f"Table : {table_name} SUCCESSFULLY input to postgres !!!")
        









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