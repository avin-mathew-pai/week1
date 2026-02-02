# from airflow.sdk import dag, task
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.sdk.bases.sensor import PokeReturnValue
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# @dag
# def user_processing():

#     # task 1 creating a table
#     create_table = SQLExecuteQueryOperator(
#         task_id = "create_table",
#         conn_id="postgres",
#         sql="""
#                 CREATE TABLE IF NOT EXISTS users(
#                     id INT PRIMARY KEY,
#                     firstname VARCHAR(255),
#                     lastname VARCHAR(255),
#                     email VARCHAR(255),
#                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#                 );
#             """
#     )

#     # task 2 checking if an api is availiable
#     @task.sensor()
#     def is_api_available(poke_interval=15, timeout=100) -> PokeReturnValue:
#         import requests
#         response = requests.get(url="https://raw.githubusercontent.com/marclamberti/datasets/fcad9e032e8d0c54f8dd40830efd7858d73e92d2/fakeuser.json")
#         print(response.status_code)

#         if response.status_code == 200:
#             condition = True
#             fake_user = response.json()
#         else:
#             condition = False
#             fake_user = None
#         return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
#     # task 3 extracting the data pushed to xcom by task 2 into dictionary ie. user info
#     @task
#     def extract_user(fake_user):

#         data = {
#             "id" : fake_user["id"],
#             "firstname" : fake_user["personalInfo"]["firstName"],
#             "lastname" : fake_user["personalInfo"]["lastName"],
#             "email" : fake_user["personalInfo"]["email"],
#         }
#         return data
    
#     #task 4 process user
#     @task
#     def process_user(user_info):

#         import csv
#         from datetime import datetime

#         # user_info = {
#         #     "id" : 3456,
#         #     "firstname" : "Jody",
#         #     "lastname" : "Cooper",
#         #     "email" : "jody34@gmail.com",
#         # }
#         user_info['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#         with open('/tmp/user_info.csv', 'w') as fp:

#             fieldnames = user_info.keys()
#             writer = csv.DictWriter(fp, fieldnames= fieldnames)
#             writer.writeheader()
#             writer.writerow(user_info)
#         print("File created successfully !!!")

#     # task 5 store user to postgres        
#     @task
#     def store_user():
#         hook = PostgresHook(postgres_conn_id = "postgres")
#         hook.copy_expert(
#             sql = "COPY users FROM STDIN WITH CSV HEADER",
#             filename="/tmp/user_info.csv"
#         )
        
#     # working correctly connected dag
#     api_sensor = is_api_available()

#     user_data = extract_user(api_sensor)
#     processed_data = process_user(user_data)

#     create_table >> api_sensor

#     processed_data >> store_user()

# user_processing()






#     # [create_table, process_user(extract_user(is_api_available()))] >> store_user()


#     # fake_user = is_api_available()
#     # user_info = extract_user(fake_user)
#     # process_user(user_info)
#     # store_user()

# # from airflow.providers.standard.operators.python import PythonOperator

#     # # old way
#     # def _extract_user(ti):
#     #     # fake_user = ti.xcom_pull(task_ids="is_api_availiable")

#     #     # print(fake_user)

#     #     # testing
#     #     import requests
#     #     response = requests.get(url="https://raw.githubusercontent.com/marclamberti/datasets/fcad9e032e8d0c54f8dd40830efd7858d73e92d2/fakeuser.json")
#     #     fake_user = response.json()

#     #     data = {
#     #         "id" : fake_user["id"],
#     #         "firstname" : fake_user["personalInfo"]["firstName"],
#     #         "lastname" : fake_user["personalInfo"]["lastName"],
#     #         "email" : fake_user["personalInfo"]["email"],
#     #     }
#     #     return data



#     #     extract_user = PythonOperator(
#     #     task_id="extract_user",
#     #     python_callable=_extract_user
#     # )





