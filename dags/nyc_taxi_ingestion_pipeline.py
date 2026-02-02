from airflow.decorators import dag, task

@dag()
def check_fn():

    @task()
    def task1():
        print("Task 1")
    
    task1()

check_fn()