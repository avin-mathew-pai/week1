from airflow.decorators import dag, task
# from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

FILE_PATH = "/opt/spark/data/yellow_tripdata_2023-01.parquet"
# TEMP_FILE = "/opt/spark/data/<filename.parquet>"

@dag(
        schedule='@daily',
        start_date=datetime(2026,2,1),
        catchup=False,
        default_args={
            "retries":2,
            "retry_delay": timedelta(seconds=5)
        },
        # params={
        #     "filename": Param("yellow_tripdata_2023-01.parquet", type="string")
        # }
)
def spark_taxi_pipeline():

# Task : 1 for non-kubernetes approach
    # taxi_spark_job = BashOperator(
    #     task_id="run_in_taxi-pyspark",
    #     bash_command="""
    #         docker exec -i taxi-pyspark \
    #         spark-submit \
    #         --master local[*] \
    #         --driver-memory 4g \
    #         --jars /opt/spark/work-dir/postgresql-42.7.10.jar \
    #         /opt/spark/work-dir/spark_main.py
    #     """
    # )
    # taxi_spark_job

# Task : 1 for Kubernetes approach

    data_volume = k8s.V1Volume(
        name='taxi-data-volume',
        host_path=k8s.V1HostPathVolumeSource(path='/mnt/c/Datasetw1')
    )

    data_volume_mount = k8s.V1VolumeMount(
        name='taxi-data-volume',
        mount_path='/opt/spark/data',
        read_only=True
    )

    taxi_spark_job = KubernetesPodOperator(
        task_id="run_in_kubernetes",
        name="spark-taxi-processor",
        namespace="airflow",
        image="taxi-spark-kube-job-ui:v6",
        image_pull_policy="Never",
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=[
            "--master", "local[*]",
            "--driver-memory", "4g",
            "--jars", "/opt/spark/work-dir/postgresql-42.7.10.jar",
            "/opt/spark/work-dir/spark_main.py"
        ],
        volumes=[data_volume],
        volume_mounts=[data_volume_mount],
        get_logs=True,
        is_delete_operator_pod=True
    )
    taxi_spark_job

spark_taxi_pipeline()



# kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow, minikube mount /mnt/c/Datasetw1:/mnt/c/Datasetw1, minikube mount $(pwd):/mnt/week1
# kubectl port-forward fastapi-pod 8000:8000
# for spark ui
# kubectl port-forward pod/<pod in airflow namespace> 4040:4040 -n airflow
# will maybe have to delete following : 
# kubectl delete pods -l component=dag-processor -n airflow
# kubectl delete pods -l component=scheduler -n airflow
# kubectl delete pods -l component=worker -n airflow

# because we mount storage after the pods start










