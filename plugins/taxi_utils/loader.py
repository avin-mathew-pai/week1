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
            return f"ERROR table : {table_name} already exists !!! \n {repr(e)}"
        else: 
            print(f"Table : {table_name} SUCCESSFULLY input to postgres !!!")
        

