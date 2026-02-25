class Loader():
    def __init__(self, spark_session):

        self.spark = spark_session

    def load_table(self, df, table_name, logger, datetime):

        logger.info(f"\n\nInserting table : {table_name} , START time = {datetime.now()}\n\n")
        
        # jdbc_url = "jdbc:postgresql://taxi_db:5432/your_database_name"
        # jdbc_url = "jdbc:postgresql://taxi_db:5432/mydatabase"
        jdbc_url_kube = "jdbc:postgresql://postgresdb-service:5432/mydatabase"

        connection_properties = {
            "user": "avin",
            "password": "avin",
            "driver": "org.postgresql.Driver",
            "batchsize": "10000"
        }
        try:

            df.repartition(4).write.jdbc(
                url=jdbc_url_kube,
                table=table_name,
                mode="overwrite",
                properties=connection_properties
            )
        except Exception as e:
            logger.error(f"ERROR table : {table_name} already exists !!! \n {repr(e)}")
        else: 
            no_of_rows = df.count()
            # LOG
            logger.info(f"\n\nInserting table : {table_name} , END time = {datetime.now()}\n Number of rows processed : {no_of_rows}\nSUCCESS\n\n")
            print(f"Table : {table_name} SUCCESSFULLY input to postgres !!!")
        

