class Loader():
    def __init__(self, spark_session):

        self.spark = spark_session

    def load_table(self, df, table_name):

        # jdbc_url = "jdbc:postgresql://taxi_db:5432/your_database_name"
        jdbc_url = "jdbc:postgresql://taxi_db:5432/mydatabase"
        connection_properties = {
            "user": "avin",
            "password": "avin",
            "driver": "org.postgresql.Driver",
            "batchsize": "10000"
        }
        try:

            df.repartition(4).write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=connection_properties
            )
        except Exception as e:
            print(f"ERROR table : {table_name} already exists !!! \n {repr(e)}")
        else: 
            print(f"Table : {table_name} SUCCESSFULLY input to postgres !!!")
        

